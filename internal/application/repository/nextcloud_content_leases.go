package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// NextcloudContentLeaseMaxTTL bounds the lifetime of a durable content lease.
const NextcloudContentLeaseMaxTTL = 5 * time.Minute

const nextcloudContentLeasePruneMaxBatch = 1000

var (
	// ErrNextcloudContentLeaseInvalid reports an invalid durable content-lease scope.
	ErrNextcloudContentLeaseInvalid = errors.New("invalid Nextcloud content lease request")
	// ErrNextcloudContentLeaseDenied reports a closed content-read or build fence.
	ErrNextcloudContentLeaseDenied = apperrors.NewProtocolError(errors.New(
		"nextcloud content fence is closed",
	), "Nextcloud content fence is closed")
	// ErrNextcloudContentLeaseExpired reports an expired or released content lease.
	ErrNextcloudContentLeaseExpired = apperrors.NewProtocolError(errors.New(
		"nextcloud content lease expired or released",
	), "Nextcloud content lease expired or released")
	// ErrNextcloudContentGCBusy reports a collection blocked by an active lease or claim.
	ErrNextcloudContentGCBusy = apperrors.NewProtocolError(
		errors.New(
			("nextcloud content GC is blocked by an active lease or c" +
				"laim")), "Nextcloud content GC is blocked by an active lease or claim")
	// ErrNextcloudContentGCUncovered reports unproven durable lease-protocol coverage.
	ErrNextcloudContentGCUncovered = apperrors.NewProtocolError(errors.New(
		"nextcloud content lease protocol coverage is unproven",
	), "Nextcloud content lease protocol coverage is unproven")
)

// NextcloudContentLeaseKind identifies the purpose of a durable content lease.
type NextcloudContentLeaseKind string

const (
	// NextcloudContentReadLease identifies a lease protecting source-content reads.
	NextcloudContentReadLease NextcloudContentLeaseKind = "read"
	// NextcloudContentBuildLease identifies a lease protecting source-content writes.
	NextcloudContentBuildLease NextcloudContentLeaseKind = "build"
)

// NextcloudContentScope identifies one immutable imported knowledge generation.
// A caller must obtain these values from the persisted knowledge/source tuple,
// never from an untrusted URL. A lease is a GC barrier, not authorization.
type NextcloudContentScope struct {
	TenantID        uint64
	KnowledgeBaseID string
	KnowledgeID     string
	DataSourceID    string
	ExternalID      string
}

func (s NextcloudContentScope) valid() bool {
	return s.TenantID > 0 && s.KnowledgeBaseID != "" && s.KnowledgeID != "" &&
		s.DataSourceID != "" && s.ExternalID != ""
}

// NextcloudContentLease records a durable source-content lease.
type NextcloudContentLease struct {
	ID              string
	TenantID        uint64
	KnowledgeBaseID string
	KnowledgeID     string
	Kind            NextcloudContentLeaseKind
	Epoch           int64
	OwnerID         string
	ExpiresAt       time.Time
}

// NextcloudContentGCClaim records a durable claim for source-content collection.
type NextcloudContentGCClaim struct {
	Token     string
	Scope     NextcloudContentScope
	Epoch     int64
	ExpiresAt time.Time
}

type nextcloudContentFence struct {
	TenantID        uint64 `gorm:"column:tenant_id"`
	KnowledgeBaseID string `gorm:"column:knowledge_base_id"`
	KnowledgeID     string `gorm:"column:knowledge_id"`
	DataSourceID    string `gorm:"column:datasource_id"`
	ExternalID      string `gorm:"column:external_id"`
	Epoch           int64  `gorm:"column:epoch"`
	State           string `gorm:"column:state"`
	RetiredAtMS     *int64 `gorm:"column:retired_at_ms"`
	ClaimToken      string `gorm:"column:claim_token"`
	ClaimUntilMS    *int64 `gorm:"column:claim_until_ms"`
}

func (nextcloudContentFence) TableName() string { return "nextcloud_content_fences" }

type nextcloudContentLeaseRow struct {
	LeaseID         string `gorm:"column:lease_id"`
	TenantID        uint64 `gorm:"column:tenant_id"`
	KnowledgeBaseID string `gorm:"column:knowledge_base_id"`
	KnowledgeID     string `gorm:"column:knowledge_id"`
	Kind            string `gorm:"column:kind"`
	Epoch           int64  `gorm:"column:epoch"`
	OwnerID         string `gorm:"column:owner_id"`
	ExpiresAtMS     int64  `gorm:"column:expires_at_ms"`
	ReleasedAtMS    *int64 `gorm:"column:released_at_ms"`
}

func (nextcloudContentLeaseRow) TableName() string { return "nextcloud_content_leases" }

// NextcloudContentLeaseStore persists source-content leases and collection fences.
type NextcloudContentLeaseStore struct{ db *gorm.DB }

// NewNextcloudContentLeaseStore returns the durable source-content lease store.
func NewNextcloudContentLeaseStore(db *gorm.DB) *NextcloudContentLeaseStore {
	return &NextcloudContentLeaseStore{db: db}
}

func nextcloudLeaseTTL(ttl time.Duration) (int64, error) {
	if ttl <= 0 || ttl > NextcloudContentLeaseMaxTTL || ttl.Milliseconds() == 0 {
		return 0, ErrNextcloudContentLeaseInvalid
	}
	return ttl.Milliseconds(), nil
}

// All expiry decisions use the database clock, not an app node's clock.
func nextcloudLeaseNowMS(tx *gorm.DB) (int64, error) {
	var ms int64
	var query string
	switch tx.Name() {
	case "postgres":
		query = "SELECT floor(extract(epoch from clock_timestamp()) * 1000)::bigint"
	case "sqlite":
		query = "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
	default:
		return 0, fmt.Errorf("unsupported Nextcloud content lease database %q", tx.Name())
	}
	if err := tx.Raw(query).Scan(&ms).Error; err != nil {
		return 0, err
	}
	return ms, nil
}

// Lock order for every lease/fence mutation: KB sentinel, then exact fence.
// An UPDATE, including a no-op, takes the PostgreSQL row lock and starts a
// SQLite write transaction before any state-dependent read.
func nextcloudLockKBFence(tx *gorm.DB, tenantID uint64, kbID string, nowMS int64) (nextcloudContentFence, error) {
	var fence nextcloudContentFence
	if tenantID == 0 || kbID == "" {
		return fence, ErrNextcloudContentLeaseInvalid
	}
	if err := tx.Exec(`INSERT INTO nextcloud_content_fences
		(tenant_id, knowledge_base_id, knowledge_id, datasource_id, external_id,
		 epoch, state, claim_token, created_at_ms, updated_at_ms)
		VALUES (?, ?, '', '', '', 1, 'open', '', ?, ?)
		ON CONFLICT (tenant_id, knowledge_base_id, knowledge_id) DO NOTHING`,
		tenantID, kbID, nowMS, nowMS).Error; err != nil {
		return fence, err
	}
	if err := tx.Exec(`UPDATE nextcloud_content_fences SET epoch = epoch
		WHERE tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ''`,
		tenantID, kbID).Error; err != nil {
		return fence, err
	}
	err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ''", tenantID, kbID).
		Take(&fence).Error
	return fence, err
}

func nextcloudLockExactFence(tx *gorm.DB, scope NextcloudContentScope, nowMS int64, create bool) (
	nextcloudContentFence,
	error,
) {
	var fence nextcloudContentFence
	if !scope.valid() {
		return fence, ErrNextcloudContentLeaseInvalid
	}
	if create {
		if err := tx.Exec(`INSERT INTO nextcloud_content_fences
			(tenant_id, knowledge_base_id, knowledge_id, datasource_id, external_id,
			 epoch, state, claim_token, created_at_ms, updated_at_ms)
			VALUES (?, ?, ?, ?, ?, 1, 'open', '', ?, ?)
			ON CONFLICT (tenant_id, knowledge_base_id, knowledge_id) DO NOTHING`,
			scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID,
			scope.DataSourceID, scope.ExternalID, nowMS, nowMS).Error; err != nil {
			return fence, err
		}
	}
	updated := tx.Exec(`UPDATE nextcloud_content_fences SET epoch = epoch
		WHERE tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?`,
		scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID)
	if updated.Error != nil {
		return fence, updated.Error
	}
	if updated.RowsAffected != 1 {
		return fence, ErrNextcloudContentLeaseDenied
	}
	if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
		scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID).Take(&fence).Error; err != nil {
		return fence, err
	}
	if fence.DataSourceID != scope.DataSourceID || fence.ExternalID != scope.ExternalID {
		return fence, ErrNextcloudContentLeaseDenied
	}
	return fence, nil
}

func nextcloudLeasePublic(row nextcloudContentLeaseRow) NextcloudContentLease {
	return NextcloudContentLease{
		ID: row.LeaseID, TenantID: row.TenantID,
		KnowledgeBaseID: row.KnowledgeBaseID, KnowledgeID: row.KnowledgeID,
		Kind: NextcloudContentLeaseKind(row.Kind), Epoch: row.Epoch,
		OwnerID: row.OwnerID, ExpiresAt: time.UnixMilli(row.ExpiresAtMS).UTC(),
	}
}

// Call only while holding the KB sentinel lock. A claim that has expired is
// no longer allowed to delete and must not strand future KB searches. A
// subsequent claimant races with this check under the same sentinel lock.
func nextcloudActiveGCClaimInKB(tx *gorm.DB, tenantID uint64, kbID string, nowMS int64) (bool, error) {
	var active int64
	err := tx.Table("nextcloud_content_fences").Where(`tenant_id = ? AND
		knowledge_base_id = ? AND knowledge_id <> '' AND state = 'deleting' AND
		claim_until_ms > ?`, tenantID, kbID, nowMS).Count(&active).Error
	return active != 0, err
}

func nextcloudInsertLease(tx *gorm.DB, tenantID uint64, kbID, knowledgeID string,
	kind NextcloudContentLeaseKind, epoch int64, owner string, nowMS, ttlMS int64,
) (NextcloudContentLease, error) {
	row := nextcloudContentLeaseRow{
		LeaseID: uuid.NewString(), TenantID: tenantID,
		KnowledgeBaseID: kbID, KnowledgeID: knowledgeID, Kind: string(kind),
		Epoch: epoch, OwnerID: owner, ExpiresAtMS: nowMS + ttlMS,
	}
	err := tx.Exec(`INSERT INTO nextcloud_content_leases
		(lease_id, tenant_id, knowledge_base_id, knowledge_id, kind, epoch,
		 owner_id, expires_at_ms, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, row.LeaseID, row.TenantID,
		row.KnowledgeBaseID, row.KnowledgeID, row.Kind, row.Epoch, row.OwnerID,
		row.ExpiresAtMS, nowMS, nowMS).Error
	return nextcloudLeasePublic(row), err
}

// AcquireKBRead protects a search while the matching document IDs are still
// unknown. It must be held through result hydration or upgraded to exact
// leases before release. Publication authorization is still required.
func (s *NextcloudContentLeaseStore) AcquireKBRead(ctx context.Context,
	tenantID uint64, kbID, owner string, ttl time.Duration,
) (NextcloudContentLease, error) {
	var lease NextcloudContentLease
	ttlMS, err := nextcloudLeaseTTL(ttl)
	if err != nil || tenantID == 0 || kbID == "" || owner == "" {
		return lease, ErrNextcloudContentLeaseInvalid
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		nowMS, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		kb, err := nextcloudLockKBFence(tx, tenantID, kbID, nowMS)
		if err != nil {
			return err
		}
		if kb.State != "open" {
			return ErrNextcloudContentLeaseDenied
		}
		activeGC, err := nextcloudActiveGCClaimInKB(tx, tenantID, kbID, nowMS)
		if err != nil {
			return err
		}
		if activeGC {
			return ErrNextcloudContentLeaseDenied
		}
		lease, err = nextcloudInsertLease(tx, tenantID, kbID, "", NextcloudContentReadLease,
			kb.Epoch, owner, nowMS, ttlMS)
		return err
	})
	return lease, err
}

// AcquireKnowledge acquires a lease for an exact source generation.
func (s *NextcloudContentLeaseStore) AcquireKnowledge(ctx context.Context,
	scope NextcloudContentScope, kind NextcloudContentLeaseKind, owner string, ttl time.Duration,
) (NextcloudContentLease, error) {
	var lease NextcloudContentLease
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		lease, err = s.AcquireKnowledgeInTx(ctx, tx, scope, kind, owner, ttl)
		return err
	})
	return lease, err
}

// WaitForCurrentSourceVersion is the admission barrier for a newly queued
// Nextcloud parser. File creation enqueues before StageNextcloudVersion commits;
// allowing that worker into parsing can turn a harmless scheduling race into
// a terminal BatchIndex failure. The caller holds an exact build lease while
// waiting and must revalidate it after this method returns.
func (s *NextcloudContentLeaseStore) WaitForCurrentSourceVersion(ctx context.Context,
	scope NextcloudContentScope, lease NextcloudContentLease, desiredETag string, maxWait time.Duration,
) error {
	if !scope.valid() || desiredETag == "" || maxWait <= 0 || maxWait > time.Minute {
		return ErrNextcloudContentLeaseInvalid
	}
	if lease.TenantID != scope.TenantID || lease.KnowledgeBaseID != scope.KnowledgeBaseID ||
		lease.KnowledgeID != scope.KnowledgeID || lease.Kind != NextcloudContentBuildLease || lease.Epoch <= 0 {
		return ErrNextcloudContentLeaseInvalid
	}
	waitCtx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		var fence nextcloudContentFence
		fenceQuery := s.db.WithContext(waitCtx).Select("state", "epoch").Where(
			"tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
			scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID).Limit(1).Find(&fence)
		if fenceQuery.Error != nil {
			return fenceQuery.Error
		}
		if fenceQuery.RowsAffected != 1 || fence.State != "open" || fence.Epoch != lease.Epoch {
			return ErrNextcloudContentLeaseDenied
		}
		var version nextcloudSourceVersion
		query := nextcloudVersionQuery(s.db.WithContext(waitCtx), scope.TenantID,
			scope.KnowledgeBaseID, scope.DataSourceID, scope.ExternalID).Limit(1).Find(&version)
		if query.Error != nil {
			return query.Error
		}
		if query.RowsAffected == 1 && version.CandidateKnowledgeID == scope.KnowledgeID &&
			version.DesiredETag == desiredETag &&
			(version.State == "staging" || version.State == "published") {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waitCtx.Done():
			return apperrors.NewProtocolError(fmt.Errorf(
				"nextcloud source version was not staged: %w",
				ErrNextcloudContentLeaseDenied,
			), fmt.Sprintf("Nextcloud source version was not staged: %s", apperrors.PublicMessage(
				ErrNextcloudContentLeaseDenied,
			)))
		case <-ticker.C:
		}
	}
}

// AcquireKnowledgeInTx can join a caller's transaction. It must run before
// acquiring other source/version locks to preserve the KB -> exact order.
func (s *NextcloudContentLeaseStore) AcquireKnowledgeInTx(ctx context.Context,
	tx *gorm.DB, scope NextcloudContentScope, kind NextcloudContentLeaseKind,
	owner string, ttl time.Duration,
) (NextcloudContentLease, error) {
	var lease NextcloudContentLease
	ttlMS, err := nextcloudLeaseTTL(ttl)
	if err != nil || !scope.valid() || owner == "" ||
		(kind != NextcloudContentReadLease && kind != NextcloudContentBuildLease) {
		return lease, ErrNextcloudContentLeaseInvalid
	}
	tx = tx.WithContext(ctx)
	nowMS, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return lease, err
	}
	kb, err := nextcloudLockKBFence(tx, scope.TenantID, scope.KnowledgeBaseID, nowMS)
	if err != nil {
		return lease, err
	}
	if kb.State != "open" {
		return lease, ErrNextcloudContentLeaseDenied
	}
	fence, err := nextcloudLockExactFence(tx, scope, nowMS, true)
	if err != nil {
		return lease, err
	}
	if fence.State != "open" {
		return lease, ErrNextcloudContentLeaseDenied
	}
	return nextcloudInsertLease(tx, scope.TenantID, scope.KnowledgeBaseID,
		scope.KnowledgeID, kind, fence.Epoch, owner, nowMS, ttlMS)
}

// UpgradeKBRead serializes with retirement and preserves the broad lease. A
// caller may release that broad lease only after exact leases cover every
// result it will hydrate. Retirement between search and upgrade denies it.
func (s *NextcloudContentLeaseStore) UpgradeKBRead(ctx context.Context,
	kbLeaseID string, scope NextcloudContentScope, owner string, ttl time.Duration,
) (NextcloudContentLease, error) {
	var lease NextcloudContentLease
	ttlMS, err := nextcloudLeaseTTL(ttl)
	if err != nil || kbLeaseID == "" || !scope.valid() || owner == "" {
		return lease, ErrNextcloudContentLeaseInvalid
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		nowMS, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		kb, err := nextcloudLockKBFence(tx, scope.TenantID, scope.KnowledgeBaseID, nowMS)
		if err != nil {
			return err
		}
		if kb.State != "open" {
			return ErrNextcloudContentLeaseDenied
		}
		var broad nextcloudContentLeaseRow
		if err := tx.Where("lease_id = ?", kbLeaseID).Take(&broad).Error; err != nil {
			return ErrNextcloudContentLeaseExpired
		}
		if broad.TenantID != scope.TenantID || broad.KnowledgeBaseID != scope.KnowledgeBaseID ||
			broad.KnowledgeID != "" || broad.Kind != string(NextcloudContentReadLease) ||
			broad.Epoch != kb.Epoch || broad.OwnerID != owner || broad.ReleasedAtMS != nil ||
			broad.ExpiresAtMS <= nowMS {
			return ErrNextcloudContentLeaseExpired
		}
		fence, err := nextcloudLockExactFence(tx, scope, nowMS, true)
		if err != nil {
			return err
		}
		if fence.State != "open" {
			return ErrNextcloudContentLeaseDenied
		}
		lease, err = nextcloudInsertLease(tx, scope.TenantID, scope.KnowledgeBaseID,
			scope.KnowledgeID, NextcloudContentReadLease, fence.Epoch, owner, nowMS, ttlMS)
		return err
	})
	return lease, err
}

// RenewLease fails closed after retirement, expiry, release, or a reused ID.
// The caller must stop the corresponding read/build when renewal fails.
func (s *NextcloudContentLeaseStore) RenewLease(ctx context.Context, id string,
	ttl time.Duration,
) (NextcloudContentLease, error) {
	var lease NextcloudContentLease
	ttlMS, err := nextcloudLeaseTTL(ttl)
	if err != nil || id == "" {
		return lease, ErrNextcloudContentLeaseInvalid
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row nextcloudContentLeaseRow
		if err := tx.Where("lease_id = ?", id).Take(&row).Error; err != nil {
			return ErrNextcloudContentLeaseExpired
		}
		nowMS, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		kb, err := nextcloudLockKBFence(tx, row.TenantID, row.KnowledgeBaseID, nowMS)
		if err != nil {
			return err
		}
		if kb.State != "open" {
			return ErrNextcloudContentLeaseDenied
		}
		if row.KnowledgeID != "" {
			var exact nextcloudContentFence
			if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
				row.TenantID, row.KnowledgeBaseID, row.KnowledgeID).Take(&exact).Error; err != nil {
				return ErrNextcloudContentLeaseDenied
			}
			if exact.State != "open" || exact.Epoch != row.Epoch {
				return ErrNextcloudContentLeaseDenied
			}
		} else if kb.Epoch != row.Epoch {
			return ErrNextcloudContentLeaseDenied
		}
		result := tx.Exec(`UPDATE nextcloud_content_leases SET expires_at_ms = ?, updated_at_ms = ?
			WHERE lease_id = ? AND released_at_ms IS NULL AND expires_at_ms > ?`,
			nowMS+ttlMS, nowMS, id, nowMS)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrNextcloudContentLeaseExpired
		}
		row.ExpiresAtMS = nowMS + ttlMS
		lease = nextcloudLeasePublic(row)
		return nil
	})
	return lease, err
}

// ReleaseLease releases a durable content lease.
func (s *NextcloudContentLeaseStore) ReleaseLease(ctx context.Context, id string) error {
	if id == "" {
		return ErrNextcloudContentLeaseInvalid
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		nowMS, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		return tx.Exec(`UPDATE nextcloud_content_leases
			SET released_at_ms = ?, updated_at_ms = ?
			WHERE lease_id = ? AND released_at_ms IS NULL`, nowMS, nowMS, id).Error
	})
}

// PruneFinishedLeases bounds lease-table growth without touching a live lease.
// A released row is terminal; an unreleased row is eligible only after its
// expiry has been past retention. Both timestamps are compared with the
// database clock so app-node clock skew cannot shorten the retention period.
func (s *NextcloudContentLeaseStore) PruneFinishedLeases(ctx context.Context,
	retention time.Duration, limit int,
) (int64, error) {
	if retention <= 0 || retention.Milliseconds() == 0 || limit <= 0 {
		return 0, ErrNextcloudContentLeaseInvalid
	}
	if limit > nextcloudContentLeasePruneMaxBatch {
		limit = nextcloudContentLeasePruneMaxBatch
	}
	var pruned int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		nowMS, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		cutoffMS := nowMS - retention.Milliseconds()
		// The outer predicate matters if another transaction changes a row
		// selected by the bounded subquery before DELETE obtains its lock.
		released := tx.Exec(`DELETE FROM nextcloud_content_leases
			WHERE lease_id IN (
				SELECT lease_id FROM nextcloud_content_leases
				WHERE released_at_ms IS NOT NULL AND released_at_ms < ?
				ORDER BY released_at_ms, lease_id LIMIT ?
			) AND released_at_ms IS NOT NULL AND released_at_ms < ?`,
			cutoffMS, limit, cutoffMS)
		if released.Error != nil {
			return released.Error
		}
		pruned = released.RowsAffected
		if pruned >= int64(limit) {
			return nil
		}
		expired := tx.Exec(`DELETE FROM nextcloud_content_leases
			WHERE lease_id IN (
				SELECT lease_id FROM nextcloud_content_leases
				WHERE released_at_ms IS NULL AND expires_at_ms < ?
				ORDER BY expires_at_ms, lease_id LIMIT ?
			) AND released_at_ms IS NULL AND expires_at_ms < ?`,
			cutoffMS, limit-int(pruned), cutoffMS)
		if expired.Error != nil {
			return expired.Error
		}
		pruned += expired.RowsAffected
		return nil
	})
	if err != nil {
		return 0, err
	}
	return pruned, nil
}

// RetireKnowledge retires an exact source generation before collection.
func (s *NextcloudContentLeaseStore) RetireKnowledge(ctx context.Context,
	scope NextcloudContentScope,
) error {
	if !scope.valid() {
		return ErrNextcloudContentLeaseInvalid
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return s.RetireKnowledgeInTx(ctx, tx, scope)
	})
}

// RetireKnowledgeInTx lets a source-version/publication transition close lease
// admission in the same database transaction. Call before holding other locks.
func (s *NextcloudContentLeaseStore) RetireKnowledgeInTx(ctx context.Context,
	tx *gorm.DB, scope NextcloudContentScope,
) error {
	if !scope.valid() {
		return ErrNextcloudContentLeaseInvalid
	}
	tx = tx.WithContext(ctx)
	nowMS, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return err
	}
	if _, err := nextcloudLockKBFence(tx, scope.TenantID, scope.KnowledgeBaseID, nowMS); err != nil {
		return err
	}
	fence, err := nextcloudLockExactFence(tx, scope, nowMS, true)
	if err != nil {
		return err
	}
	if fence.State != "open" {
		return nil
	}
	return tx.Exec(`UPDATE nextcloud_content_fences
		SET state = 'retired', epoch = epoch + 1, retired_at_ms = ?, updated_at_ms = ?
		WHERE tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ? AND state = 'open'`,
		nowMS, nowMS, scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID).Error
}

// RetireKB retires the source generations in a knowledge base.
func (s *NextcloudContentLeaseStore) RetireKB(ctx context.Context,
	tenantID uint64, kbID string,
) error {
	if tenantID == 0 || kbID == "" {
		return ErrNextcloudContentLeaseInvalid
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return s.RetireKBInTx(ctx, tx, tenantID, kbID)
	})
}

// RetireKBInTx retires knowledge-base source generations in the caller's transaction.
func (s *NextcloudContentLeaseStore) RetireKBInTx(ctx context.Context,
	tx *gorm.DB, tenantID uint64, kbID string,
) error {
	tx = tx.WithContext(ctx)
	nowMS, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return err
	}
	kb, err := nextcloudLockKBFence(tx, tenantID, kbID, nowMS)
	if err != nil {
		return err
	}
	if kb.State != "open" {
		return nil
	}
	return tx.Exec(`UPDATE nextcloud_content_fences
		SET state = 'retired', epoch = epoch + 1, retired_at_ms = ?, updated_at_ms = ?
		WHERE tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = '' AND state = 'open'`,
		nowMS, nowMS, tenantID, kbID).Error
}

// ValidateBuildLeaseInTx must run inside the transaction that writes SQL
// derived rows. External index/object writes need their own fenced adapter.
func (s *NextcloudContentLeaseStore) ValidateBuildLeaseInTx(ctx context.Context,
	tx *gorm.DB, lease NextcloudContentLease, scope NextcloudContentScope,
) error {
	if !scope.valid() || lease.ID == "" || lease.Kind != NextcloudContentBuildLease ||
		lease.TenantID != scope.TenantID || lease.KnowledgeBaseID != scope.KnowledgeBaseID ||
		lease.KnowledgeID != scope.KnowledgeID {
		return ErrNextcloudContentLeaseInvalid
	}
	tx = tx.WithContext(ctx)
	nowMS, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return err
	}
	kb, err := nextcloudLockKBFence(tx, scope.TenantID, scope.KnowledgeBaseID, nowMS)
	if err != nil {
		return err
	}
	if kb.State != "open" {
		return ErrNextcloudContentLeaseDenied
	}
	fence, err := nextcloudLockExactFence(tx, scope, nowMS, false)
	if err != nil || fence.State != "open" || fence.Epoch != lease.Epoch {
		return ErrNextcloudContentLeaseDenied
	}
	var row nextcloudContentLeaseRow
	if err := tx.Where("lease_id = ?", lease.ID).Take(&row).Error; err != nil {
		return ErrNextcloudContentLeaseExpired
	}
	if row.TenantID != scope.TenantID || row.KnowledgeBaseID != scope.KnowledgeBaseID ||
		row.KnowledgeID != scope.KnowledgeID || row.Kind != string(NextcloudContentBuildLease) ||
		row.Epoch != fence.Epoch || row.OwnerID != lease.OwnerID ||
		row.ReleasedAtMS != nil || row.ExpiresAtMS <= nowMS {
		return ErrNextcloudContentLeaseExpired
	}
	return nil
}

// Keep the coverage row locked until the claim or exact delete transaction
// commits. If an operator revokes coverage after discovering an uncovered path,
// a claimant must not complete a delete using an earlier coverage observation.
// Call after taking the KB sentinel lock.
func nextcloudRequireContentLeaseCoverage(tx *gorm.DB, scope NextcloudContentScope, nowMS int64) error {
	locked := tx.Exec(`UPDATE nextcloud_content_lease_coverage
		SET activated_at_ms = activated_at_ms
		WHERE tenant_id = ? AND knowledge_base_id = ?`,
		scope.TenantID, scope.KnowledgeBaseID)
	if locked.Error != nil {
		return locked.Error
	}
	if locked.RowsAffected != 1 {
		return ErrNextcloudContentGCUncovered
	}
	var coverage struct {
		ActivatedAtMS     int64  `gorm:"column:activated_at_ms"`
		LegacyDrainedAtMS int64  `gorm:"column:legacy_drained_at_ms"`
		ReaderRevision    string `gorm:"column:reader_revision"`
		BuilderRevision   string `gorm:"column:builder_revision"`
	}
	if err := tx.Table("nextcloud_content_lease_coverage").
		Where("tenant_id = ? AND knowledge_base_id = ?", scope.TenantID, scope.KnowledgeBaseID).
		Take(&coverage).Error; err != nil {
		return ErrNextcloudContentGCUncovered
	}
	if coverage.ActivatedAtMS <= 0 || coverage.LegacyDrainedAtMS < coverage.ActivatedAtMS ||
		coverage.LegacyDrainedAtMS > nowMS || coverage.ReaderRevision == "" ||
		coverage.BuilderRevision == "" {
		return ErrNextcloudContentGCUncovered
	}
	return nil
}

func nextcloudRequireNoActiveContentLeases(tx *gorm.DB, scope NextcloudContentScope, nowMS int64) error {
	var active int64
	if err := tx.Table("nextcloud_content_leases").Where(`tenant_id = ? AND
		knowledge_base_id = ? AND knowledge_id IN ('', ?) AND
		released_at_ms IS NULL AND expires_at_ms > ?`,
		scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID, nowMS).
		Count(&active).Error; err != nil {
		return err
	}
	if active != 0 {
		return ErrNextcloudContentGCBusy
	}
	return nil
}

// ClaimKnowledgeGC is an admission fence only. No physical deletion is wired
// to it. Coverage is absent by default; a future rollout must establish full
// reader/build coverage and drain pre-rollout work before inserting the marker.
func (s *NextcloudContentLeaseStore) ClaimKnowledgeGC(ctx context.Context,
	scope NextcloudContentScope, notBefore time.Time, ttl time.Duration,
) (NextcloudContentGCClaim, error) {
	var claim NextcloudContentGCClaim
	ttlMS, err := nextcloudLeaseTTL(ttl)
	if err != nil || !scope.valid() || notBefore.IsZero() {
		return claim, ErrNextcloudContentLeaseInvalid
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		nowMS, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		if nowMS < notBefore.UnixMilli() {
			return ErrNextcloudContentGCBusy
		}
		if _, err := nextcloudLockKBFence(tx, scope.TenantID, scope.KnowledgeBaseID, nowMS); err != nil {
			return err
		}
		if err := nextcloudRequireContentLeaseCoverage(tx, scope, nowMS); err != nil {
			return err
		}
		fence, err := nextcloudLockExactFence(tx, scope, nowMS, false)
		if err != nil {
			return err
		}
		if fence.State == "deleted" || fence.State == "open" || fence.RetiredAtMS == nil ||
			*fence.RetiredAtMS > nowMS {
			return ErrNextcloudContentLeaseDenied
		}
		// The repository cannot infer the policy-specific 1h/24h window. The
		// caller must pass the persisted GC job's not_before; at minimum it
		// may never predate this durable retirement fence.
		if notBefore.UnixMilli() < *fence.RetiredAtMS {
			return ErrNextcloudContentLeaseInvalid
		}
		if fence.State == "deleting" && fence.ClaimUntilMS != nil && *fence.ClaimUntilMS > nowMS {
			return ErrNextcloudContentGCBusy
		}
		if err := nextcloudRequireNoActiveContentLeases(tx, scope, nowMS); err != nil {
			return err
		}
		claim = NextcloudContentGCClaim{
			Token: uuid.NewString(), Scope: scope,
			Epoch: fence.Epoch + 1, ExpiresAt: time.UnixMilli(nowMS + ttlMS).UTC(),
		}
		result := tx.Exec(`UPDATE nextcloud_content_fences
			SET state = 'deleting', epoch = epoch + 1, claim_token = ?,
				claim_until_ms = ?, updated_at_ms = ?
			WHERE tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?
				AND state IN ('retired', 'deleting')`,
			claim.Token, nowMS+ttlMS, nowMS, scope.TenantID, scope.KnowledgeBaseID,
			scope.KnowledgeID)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrNextcloudContentGCBusy
		}
		return nil
	})
	return claim, err
}

// ValidateGCClaimInTx is the final check for an exact SQL row delete. The
// caller must keep this transaction open through the deletion and receipt.
func (s *NextcloudContentLeaseStore) ValidateGCClaimInTx(ctx context.Context,
	tx *gorm.DB, claim NextcloudContentGCClaim,
) error {
	if !claim.Scope.valid() || claim.Token == "" || claim.Epoch <= 0 {
		return ErrNextcloudContentLeaseInvalid
	}
	tx = tx.WithContext(ctx)
	nowMS, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return err
	}
	if _, err := nextcloudLockKBFence(tx, claim.Scope.TenantID,
		claim.Scope.KnowledgeBaseID, nowMS); err != nil {
		return err
	}
	if err := nextcloudRequireContentLeaseCoverage(tx, claim.Scope, nowMS); err != nil {
		return err
	}
	fence, err := nextcloudLockExactFence(tx, claim.Scope, nowMS, false)
	if err != nil || fence.State != "deleting" || fence.Epoch != claim.Epoch ||
		fence.ClaimToken != claim.Token || fence.ClaimUntilMS == nil ||
		*fence.ClaimUntilMS <= nowMS {
		return ErrNextcloudContentLeaseDenied
	}
	return nextcloudRequireNoActiveContentLeases(tx, claim.Scope, nowMS)
}

// RenewGCClaim extends a still-live claim for bounded external cleanup.
// Losing this renewal requires the worker to stop before any further delete.
func (s *NextcloudContentLeaseStore) RenewGCClaim(ctx context.Context,
	claim NextcloudContentGCClaim, ttl time.Duration,
) (NextcloudContentGCClaim, error) {
	ttlMS, err := nextcloudLeaseTTL(ttl)
	if err != nil {
		return NextcloudContentGCClaim{}, err
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.ValidateGCClaimInTx(ctx, tx, claim); err != nil {
			return err
		}
		nowMS, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		result := tx.Exec(`UPDATE nextcloud_content_fences
			SET claim_until_ms = ?, updated_at_ms = ?
			WHERE tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?
				AND state = 'deleting' AND claim_token = ? AND epoch = ?`,
			nowMS+ttlMS, nowMS, claim.Scope.TenantID, claim.Scope.KnowledgeBaseID,
			claim.Scope.KnowledgeID, claim.Token, claim.Epoch)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrNextcloudContentLeaseDenied
		}
		claim.ExpiresAt = time.UnixMilli(nowMS + ttlMS).UTC()
		return nil
	})
	return claim, err
}

// FinishGCClaim finishes the durable content-collection claim.
func (s *NextcloudContentLeaseStore) FinishGCClaim(ctx context.Context,
	claim NextcloudContentGCClaim, completed bool,
) error {
	if !claim.Scope.valid() || claim.Token == "" {
		return ErrNextcloudContentLeaseInvalid
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.ValidateGCClaimInTx(ctx, tx, claim); err != nil {
			return err
		}
		nowMS, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		state := "retired"
		var deletedAt any
		if completed {
			state = "deleted"
			deletedAt = nowMS
		}
		result := tx.Exec(`UPDATE nextcloud_content_fences
			SET state = ?, deleted_at_ms = ?, claim_token = '', claim_until_ms = NULL,
				updated_at_ms = ?
			WHERE tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?
				AND state = 'deleting' AND claim_token = ? AND epoch = ?`,
			state, deletedAt, nowMS, claim.Scope.TenantID, claim.Scope.KnowledgeBaseID,
			claim.Scope.KnowledgeID, claim.Token, claim.Epoch)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrNextcloudContentLeaseDenied
		}
		return nil
	})
}
