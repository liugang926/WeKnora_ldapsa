package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	// ErrOriginalBodyUnavailable reports a original body unavailable condition.
	ErrOriginalBodyUnavailable = errors.New("original body is unavailable or retention evidence changed")
	ErrOriginalBodyRetained    = errors.New(
		"original body has a current owner, lease, unexpired recovery or unknown copy",
	)
)

type originalBodyPayload struct {
	ID                                              string `gorm:"primaryKey"`
	TenantID                                        uint64
	BodySHA256                                      string
	OriginDomainSHA256                              string
	Payload                                         *string
	BodyBytes                                       int64
	StorageKind, State                              string
	CreatedAtMS, LastCheckedAtMS                    int64
	RetiredAtMS, BodyDeadlineMS, MetadataDeadlineMS *int64
	PurgeToken                                      string
	PurgeEpoch                                      int64
}

func (originalBodyPayload) TableName() string { return "original_body_payloads" }

type originalBodyFieldHead struct {
	SlotID                                                                         string `gorm:"primaryKey"`
	SourceTable, RowKey, ColumnName                                                string
	TenantID                                                                       uint64
	KnowledgeBaseID, KnowledgeID, OwnerKind, OwnerID, SourceIdentitySHA256, BodyID string
	Revision                                                                       int64
	AdmissionID, SourceLineage, OriginalBodyRef                                    string
}

func (originalBodyFieldHead) TableName() string { return "original_body_field_heads" }

type originalBodyParentRef struct {
	BodyID                                    string `gorm:"primaryKey"`
	TenantID                                  uint64 `gorm:"primaryKey"`
	KnowledgeBaseID, KnowledgeID, AdmissionID string `gorm:"primaryKey"`
}

func (originalBodyParentRef) TableName() string { return "original_body_parent_refs" }

type originalBodyLease struct {
	LeaseID               string `gorm:"primaryKey"`
	BodyID, Kind, OwnerID string
	Epoch, ExpiresAtMS    int64
	ReleasedAtMS          *int64
}

func (originalBodyLease) TableName() string { return "original_body_leases" }

type originalBodyPurgeReceipt struct {
	BodyID                                                  string `gorm:"primaryKey"`
	BodySHA256, ScopeSHA256                                 string
	BeforeBytes, AfterBytes, ClearedFields, JournalSequence int64
	JournalHeadSHA256, PurgeToken                           string
	PurgeEpoch, CompletedAtMS                               int64
}

func (originalBodyPurgeReceipt) TableName() string { return "original_body_purge_receipts" }

type originalBodyReference struct {
	Version int    `json:"_original_body_version"`
	SlotID  string `json:"slot_id"`
	BodyID  string `json:"body_id"`
	SHA256  string `json:"sha256"`
}
type originalBodyScope struct {
	Tenant                                                  uint64
	KB, Knowledge, OwnerKind, OwnerID, AdmissionID, Lineage string
}

// OriginalBodyRetentionStore represents original body retention store repository data.
type OriginalBodyRetentionStore struct {
	db      *gorm.DB
	journal *originalBodyJournal
}

// NewOriginalBodyRetentionStore constructs original body retention store.
func NewOriginalBodyRetentionStore(db *gorm.DB) *OriginalBodyRetentionStore {
	return &OriginalBodyRetentionStore{db: db, journal: originalBodyJournalFromEnv()}
}
func bodySHA(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func bodyInternal(tx *gorm.DB) *gorm.DB {
	return tx.Session(
		&gorm.Session{NewDB: true, Context: context.WithValue(tx.Statement.Context, originalBodyInternalKey{}, true)},
	)
}

type originalBodyInternalKey struct{}

// CheckOriginalBodyAvailableInTx checks original body available in tx.
// Shared with actual material/history consumers, including message generation
// origins. This never authenticates cached body by loading a newer current row.
func CheckOriginalBodyAvailableInTx(ctx context.Context, tx *gorm.DB, table, rowID string) error {
	return checkOriginalBodyAvailableForHeads(ctx, tx, table, rowID, nil)
}

func sameOriginalBodyHeads(before, after []originalBodyFieldHead) bool {
	if len(before) != len(after) {
		return false
	}
	byID := make(map[string]originalBodyFieldHead, len(before))
	for _, head := range before {
		byID[head.SlotID] = head
	}
	for _, head := range after {
		if original, ok := byID[head.SlotID]; !ok || original != head {
			return false
		}
	}
	return true
}

func checkOriginalBodyAvailableForHeads(
	ctx context.Context,
	tx *gorm.DB,
	table, rowID string,
	captured []originalBodyFieldHead,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !originalBodySchemaPresent(tx) {
		// Preserve the legacy no-schema compatibility result, but never hide
		// caller cancellation as a successful absent-schema probe.
		return ctx.Err()
	}
	store := NewOriginalBodyRetentionStore(tx)
	if e := store.checkJournalInTx(tx); e != nil {
		return e
	}
	var initialJournal originalBodyJournalHead
	if store.journal != nil {
		if err := bodyInternal(tx).Where("id=1").Take(&initialJournal).Error; err != nil {
			return err
		}
	}
	var heads []originalBodyFieldHead
	if e := bodyInternal(tx).Where("source_table=? AND row_key=?", table, rowID).Find(&heads).Error; e != nil {
		return e
	}
	if captured != nil && !sameOriginalBodyHeads(captured, heads) {
		return ErrOriginalBodyUnavailable
	}
	if len(heads) == 0 {
		if descriptor, ok := bodyDescriptor(table); ok && descriptor.Mode == "vault" {
			return ErrOriginalBodyUnavailable
		}
	}
	for _, head := range heads {
		var body originalBodyPayload
		if e := bodyInternal(tx).Select("id,state").Where("id=?", head.BodyID).Take(&body).Error; e != nil {
			return e
		}
		if body.State != "live" {
			return ErrOriginalBodyUnavailable
		}
		if e := store.checkExternalBody(body.ID); e != nil {
			return e
		}
	}
	var finalHeads []originalBodyFieldHead
	if err := bodyInternal(tx).Where("source_table=? AND row_key=?", table, rowID).Find(&finalHeads).Error; err != nil {
		return err
	}
	if !sameOriginalBodyHeads(heads, finalHeads) {
		return ErrOriginalBodyUnavailable
	}
	if store.journal != nil {
		if err := store.checkJournalInTx(tx); err != nil {
			return err
		}
		var finalJournal originalBodyJournalHead
		if err := bodyInternal(tx).Where("id=1").Take(&finalJournal).Error; err != nil {
			return err
		}
		if initialJournal != finalJournal {
			return ErrOriginalBodyUnavailable
		}
	}
	return ctx.Err()
}

func (s *OriginalBodyRetentionStore) resolveInTx(
	tx *gorm.DB,
	head originalBodyFieldHead,
	ref originalBodyReference,
) (string, error) {
	if ref.Version != 1 || ref.SlotID != head.SlotID || ref.BodyID != head.BodyID {
		return "", ErrOriginalBodyUnavailable
	}
	if e := s.checkJournalInTx(tx); e != nil {
		return "", e
	}
	var body originalBodyPayload
	if e := bodyInternal(tx).Where("id=?", ref.BodyID).Take(&body).Error; e != nil {
		return "", e
	}
	if body.State != "live" || body.BodySHA256 != ref.SHA256 || body.Payload == nil ||
		bodySHA([]byte(*body.Payload)) != body.BodySHA256 {
		return "", ErrOriginalBodyUnavailable
	}
	if e := s.checkExternalBody(body.ID); e != nil {
		return "", e
	}
	return *body.Payload, nil
}

// AcquireLease acquires lease.
func (s *OriginalBodyRetentionStore) AcquireLease(
	ctx context.Context,
	bodyID, kind, owner string,
	ttl time.Duration,
) (string, error) {
	if bodyID == "" || owner == "" || (kind != "read" && kind != "build") || ttl <= 0 || ttl > 30*time.Minute {
		return "", ErrOriginalBodyUnavailable
	}
	var id string
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var e error
		id, e = s.acquireLeaseInTx(tx, bodyID, kind, owner, ttl)
		return e
	})
	return id, err
}

func (s *OriginalBodyRetentionStore) acquireLeaseInTx(
	tx *gorm.DB,
	bodyID, kind, owner string,
	ttl time.Duration,
) (string, error) {
	if e := s.checkJournalInTx(tx); e != nil {
		return "", e
	}
	if e := s.checkExternalBody(bodyID); e != nil {
		return "", e
	}
	var body originalBodyPayload
	if e := bodyInternal(
		tx,
	).Select("id,state,purge_epoch").
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id=?", bodyID).
		Take(&body).
		Error; e != nil {
		return "", e
	}
	if body.State != "live" {
		return "", ErrOriginalBodyUnavailable
	}
	now, e := nextcloudLeaseNowMS(bodyInternal(tx))
	if e != nil {
		return "", e
	}
	id := uuid.NewString()
	return id, bodyInternal(
		tx,
	).Create(&originalBodyLease{LeaseID: id, BodyID: bodyID, Kind: kind, OwnerID: owner, Epoch: body.PurgeEpoch, ExpiresAtMS: now + ttl.Milliseconds()}).
		Error
}

// ReleaseLease releases lease.
func (s *OriginalBodyRetentionStore) ReleaseLease(ctx context.Context, id string) error {
	now, e := nextcloudLeaseNowMS(bodyInternal(s.db))
	if e != nil {
		return e
	}
	return bodyInternal(
		s.db.WithContext(ctx),
	).Model(&originalBodyLease{}).
		Where("lease_id=? AND released_at_ms IS NULL", id).
		Update("released_at_ms", now).
		Error
}

func bodyRefString(slot, id, sha string) string {
	b, _ := json.Marshal(originalBodyReference{1, slot, id, sha})
	return string(b)
}

func bodyReference(raw string) (originalBodyReference, bool) {
	var ref originalBodyReference
	e := json.Unmarshal([]byte(raw), &ref)
	return ref, e == nil && ref.Version == 1 && ref.SlotID != "" && ref.BodyID != "" && ref.SHA256 != ""
}

func bodyIdentity(table, key, col string, scope originalBodyScope) string {
	return bodySHA(
		[]byte(
			fmt.Sprintf(
				"%s\x00%s\x00%s\x00%d\x00%s\x00%s\x00%s\x00%s",
				table,
				key,
				col,
				scope.Tenant,
				scope.KB,
				scope.Knowledge,
				scope.OwnerKind,
				scope.OwnerID,
			),
		),
	)
}

var _ = types.ConnectorTypeNextcloud

type originalBodyGCItem struct {
	JobID, BodyID                       string `gorm:"primaryKey"`
	TenantID                            uint64
	KnowledgeBaseID, KnowledgeID, State string
	EstimatedBytes, CreatedAtMS         int64
}

func (originalBodyGCItem) TableName() string { return "original_body_gc_items" }
