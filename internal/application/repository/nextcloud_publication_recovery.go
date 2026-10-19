package repository

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// ErrNextcloudRecoveryDenied reports a nextcloud recovery denied condition.
var ErrNextcloudRecoveryDenied = apperrors.NewProtocolError(
	errors.New("nextcloud publication recovery evidence denied"), "Nextcloud publication recovery evidence denied",
)

var (
	recoveryHash   = regexp.MustCompile(`^[a-f0-9]{64}$`)
	recoveryID     = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	recoveryStream = regexp.MustCompile(`^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$`)
)

// NextcloudRecoveryJournalEvent represents nextcloud recovery journal event repository data.
type NextcloudRecoveryJournalEvent struct {
	Sequence       string                           `json:"sequence"`
	BindingID      string                           `json:"binding_id"`
	FileID         *string                          `json:"file_id"`
	Kind           string                           `json:"kind"`
	SourceRevision string                           `json:"source_revision"`
	CreatedAt      int64                            `json:"created_at"`
	TargetClosure  *NextcloudRecoveryTargetClosure  `json:"target_closure,omitempty"`
	TenantDeletion *NextcloudRecoveryTenantDeletion `json:"tenant_deletion,omitempty"`
}

// NextcloudRecoveryPlan represents nextcloud recovery plan repository data.
type NextcloudRecoveryPlan struct {
	JournalEvents                       []NextcloudRecoveryJournalEvent    `json:"journal_events,omitempty"`
	TargetClosures                      []NextcloudRecoveryTargetClosure   `json:"target_closures,omitempty"`
	TenantDeletions                     []NextcloudRecoveryTenantDeletion  `json:"tenant_deletions,omitempty"`
	SchemaVersion                       int                                `json:"schema_version"`
	InstanceID                          string                             `json:"instance_id"`
	StreamID                            string                             `json:"stream_id"`
	CheckpointRecordSHA256              string                             `json:"checkpoint_record_sha256"`
	RecoveryRecordSHA256                string                             `json:"recovery_record_sha256"`
	CheckpointSequence                  string                             `json:"checkpoint_sequence"`
	CheckpointDatabaseChainSHA256       string                             `json:"checkpoint_database_chain_sha256"`
	ThroughSequence                     string                             `json:"through_sequence"`
	ThroughDatabaseChainSHA256          string                             `json:"through_database_chain_sha256"`
	DatabasePrefixHashes                map[string]string                  `json:"database_prefix_hashes"`
	CloseBindings                       []NextcloudRecoveryBinding         `json:"close_bindings"`
	WithdrawFiles                       []NextcloudRecoveryFile            `json:"withdraw_files"`
	AuthoritativeReconciliationRequired bool                               `json:"authoritative_reconciliation_required"`
	IngressReopenPermitted              bool                               `json:"ingress_reopen_permitted"`
	LatestContentRevisions              []NextcloudRecoveryContentRevision `json:"latest_content_revisions,omitempty"`
	digest                              string
	raw                                 string
}

// NextcloudRecoveryContentRevision represents nextcloud recovery content revision repository data.
type NextcloudRecoveryContentRevision struct {
	BindingID string `json:"binding_id"`
	FileID    string `json:"file_id"`
	Revision  string `json:"revision"`
	Writing   bool   `json:"writing"`
}

// NextcloudRecoveryBinding represents nextcloud recovery binding repository data.
type NextcloudRecoveryBinding struct {
	BindingID    string `json:"binding_id"`
	LastSequence string `json:"last_sequence"`
}

// NextcloudRecoveryFile represents nextcloud recovery file repository data.
type NextcloudRecoveryFile struct {
	BindingID    string `json:"binding_id"`
	FileID       string `json:"file_id"`
	LastSequence string `json:"last_sequence"`
}

// NextcloudRecoveryAnchor represents nextcloud recovery anchor repository data.
type NextcloudRecoveryAnchor struct {
	InstanceID          string `json:"instance_id"`
	StreamID            string `json:"stream_id"`
	RecordSHA256        string `json:"record_sha256"`
	Sequence            string `json:"sequence"`
	DatabaseChainSHA256 string `json:"database_chain_sha256"`
}

// NextcloudRecoveryPair represents nextcloud recovery pair repository data.
// This tuple is always read from the pairing registry, never reconstructed from
// client metadata. The externally pinned backup inventory binds every field.
type NextcloudRecoveryPair struct {
	OperationID      string `json:"operation_id"`
	TenantID         uint64 `json:"tenant_id"`
	KnowledgeBaseID  string `json:"knowledge_base_id"`
	DataSourceID     string `json:"datasource_id"`
	InstanceID       string `json:"instance_id"`
	BindingID        string `json:"binding_id"`
	BaseURL          string `json:"base_url"`
	ConfigSHA256     string `json:"config_sha256"`
	PublicationEpoch int64  `json:"publication_epoch"`
	KeyID            string `json:"key_id"`
	State            string `json:"state"`
}

// NextcloudRecoveryInventory represents nextcloud recovery inventory repository data.
type NextcloudRecoveryInventory struct {
	SchemaVersion     int                     `json:"schema_version"`
	Anchor            NextcloudRecoveryAnchor `json:"anchor"`
	Pairs             []NextcloudRecoveryPair `json:"pairs"`
	SourceStateSHA256 string                  `json:"source_state_sha256"`
}

// NextcloudRecoveryReceipt represents nextcloud recovery receipt repository data.
type NextcloudRecoveryReceipt struct {
	PlanSHA256             string    `json:"plan_sha256" gorm:"primaryKey"`
	InstanceID             string    `json:"instance_id"`
	StreamID               string    `json:"stream_id"`
	CheckpointRecordSHA256 string    `json:"checkpoint_record_sha256"`
	RecoveryRecordSHA256   string    `json:"recovery_record_sha256"`
	CheckpointSequence     int64     `json:"checkpoint_sequence"`
	ThroughSequence        int64     `json:"through_sequence"`
	InventorySHA256        string    `json:"inventory_sha256"`
	PlanJSON               string    `json:"-"`
	PairCount              int       `json:"pair_count"`
	ClosedAt               time.Time `json:"closed_at"`
}

// TableName returns the persisted table name.
func (NextcloudRecoveryReceipt) TableName() string { return "nextcloud_publication_recovery_receipts" }

type nextcloudRecoveryScope struct {
	PlanSHA256             string `gorm:"primaryKey"`
	OperationID            string `gorm:"primaryKey"`
	TenantID               uint64
	KnowledgeBaseID        string
	DataSourceID           string `gorm:"column:datasource_id"`
	InstanceID             string
	BindingID              string
	PairSHA256             string
	RetiredGenerationsJSON string
	ClosedKBEpoch          int64
	State                  string
	ReconciliationSHA256   string
	ReconciliationJSON     string
	ReconciledAt           *time.Time
}

func (nextcloudRecoveryScope) TableName() string { return "nextcloud_publication_recovery_scopes" }

// NextcloudRecoveryReconciliationReceipt represents nextcloud recovery reconciliation receipt repository data.
type NextcloudRecoveryReconciliationReceipt struct {
	PlanSHA256           string                     `json:"plan_sha256"`
	OperationID          string                     `json:"operation_id"`
	PairSHA256           string                     `json:"pair_sha256"`
	ReconciliationSHA256 string                     `json:"reconciliation_sha256"`
	Manifest             nextcloud.RecoveryManifest `json:"manifest"`
	ReconciledAt         time.Time                  `json:"reconciled_at"`
}

type nextcloudRecoveryRetiredGeneration struct {
	KnowledgeID string `json:"knowledge_id"`
	Epoch       int64  `json:"epoch"`
}

type nextcloudRecoveryRevision struct {
	TenantID             uint64
	KnowledgeBaseID      string
	DataSourceID         string `gorm:"column:datasource_id"`
	ExternalID           string
	Revision             int64
	DesiredETag          string `gorm:"column:desired_etag"`
	CandidateKnowledgeID string
	State                string
	SourceUpdatedAt      time.Time
	RecordedAt           time.Time
	LegacyHistoryUnknown bool
}

// NextcloudPublicationRecoveryStore represents nextcloud publication recovery store repository data.
type NextcloudPublicationRecoveryStore struct{ db *gorm.DB }

// NewNextcloudPublicationRecoveryStore constructs nextcloud publication recovery store.
func NewNextcloudPublicationRecoveryStore(db *gorm.DB) *NextcloudPublicationRecoveryStore {
	return &NextcloudPublicationRecoveryStore{db}
}
func recoverySHA(data []byte) string { hash := sha256.Sum256(data); return hex.EncodeToString(hash[:]) }
func recoveryJSONSHA(v any) (string, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return "", e
	}
	return recoverySHA(b), nil
}

func recoverySequence(v string) (int64, error) {
	n, e := strconv.ParseInt(v, 10, 64)
	if e != nil || n < 0 || strconv.FormatInt(n, 10) != v {
		return 0, ErrNextcloudRecoveryDenied
	}
	return n, nil
}

// Reject duplicate keys even in nested objects. DisallowUnknownFields alone
// accepts duplicate fields and makes signed scope parsing ambiguous.
func recoveryStrictJSON(data []byte, out any) error {
	if len(data) == 0 || len(data) > 16<<20 {
		return ErrNextcloudRecoveryDenied
	}
	d := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		token, e := d.Token()
		if e != nil {
			return e
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return e
				}
				key, ok := k.(string)
				if !ok || seen[key] {
					return ErrNextcloudRecoveryDenied
				}
				seen[key] = true
				if e = walk(); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		case '[':
			for d.More() {
				if e = walk(); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		}
		return ErrNextcloudRecoveryDenied
	}
	if e := walk(); e != nil {
		return ErrNextcloudRecoveryDenied
	}
	if _, e := d.Token(); e != io.EOF {
		return ErrNextcloudRecoveryDenied
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if e := d.Decode(out); e != nil {
		return ErrNextcloudRecoveryDenied
	}
	return nil
}

// VerifyNextcloudRecoveryPlan verifies nextcloud recovery plan.
// Verifies exactly the signed plan bytes emitted by the external Python journal.
// Callers cannot manufacture a verified plan by filling an exported struct.
func VerifyNextcloudRecoveryPlan(envelope, key []byte) (*NextcloudRecoveryPlan, error) {
	var signed struct {
		Plan       string `json:"plan"`
		HMACSHA256 string `json:"hmac_sha256"`
	}
	if len(key) != 32 || recoveryStrictJSON(envelope, &signed) != nil || !recoveryHash.MatchString(signed.HMACSHA256) {
		return nil, ErrNextcloudRecoveryDenied
	}
	signature, _ := hex.DecodeString(signed.HMACSHA256)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(signed.Plan))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, ErrNextcloudRecoveryDenied
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(signed.Plan), &fields) != nil || (len(fields) < 14 || len(fields) > 18) {
		return nil, ErrNextcloudRecoveryDenied
	}
	for _, name := range []string{"schema_version", "instance_id", "stream_id", "checkpoint_record_sha256", "recovery_record_sha256", "checkpoint_sequence", "checkpoint_database_chain_sha256", "through_sequence", "through_database_chain_sha256", "database_prefix_hashes", "close_bindings", "withdraw_files", "authoritative_reconciliation_required", "ingress_reopen_permitted"} {
		if _, ok := fields[name]; !ok {
			return nil, ErrNextcloudRecoveryDenied
		}
	}
	for _, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, ErrNextcloudRecoveryDenied
		}
	}
	var p NextcloudRecoveryPlan
	if recoveryStrictJSON([]byte(signed.Plan), &p) != nil {
		return nil, ErrNextcloudRecoveryDenied
	}
	start, e := recoverySequence(p.CheckpointSequence)
	if e != nil {
		return nil, e
	}
	end, e := recoverySequence(p.ThroughSequence)
	if e != nil || end < start || end-start > 1000000 || p.SchemaVersion != 1 ||
		!recoveryID.MatchString(p.InstanceID) ||
		len(p.InstanceID) > 64 ||
		!recoveryStream.MatchString(p.StreamID) ||
		!p.AuthoritativeReconciliationRequired ||
		p.IngressReopenPermitted {
		return nil, ErrNextcloudRecoveryDenied
	}
	for _, v := range []string{p.CheckpointRecordSHA256, p.RecoveryRecordSHA256, p.CheckpointDatabaseChainSHA256, p.ThroughDatabaseChainSHA256} {
		if !recoveryHash.MatchString(v) {
			return nil, ErrNextcloudRecoveryDenied
		}
	}
	if len(p.DatabasePrefixHashes) != int(end-start+1) ||
		p.DatabasePrefixHashes[p.CheckpointSequence] != p.CheckpointDatabaseChainSHA256 ||
		p.DatabasePrefixHashes[p.ThroughSequence] != p.ThroughDatabaseChainSHA256 {
		return nil, ErrNextcloudRecoveryDenied
	}
	for i := start; i <= end; i++ {
		if !recoveryHash.MatchString(p.DatabasePrefixHashes[strconv.FormatInt(i, 10)]) {
			return nil, ErrNextcloudRecoveryDenied
		}
		if i == math.MaxInt64 {
			break
		}
	}
	if err := validateRecoveryTargetClosures(fields, &p); err != nil {
		return nil, err
	}
	if err := validateRecoveryTenantDeletions(fields, &p); err != nil {
		return nil, err
	}
	if raw, ok := fields["journal_events"]; ok {
		var rows []map[string]json.RawMessage
		if json.Unmarshal(raw, &rows) != nil || len(rows) != int(end-start) {
			return nil, ErrNextcloudRecoveryDenied
		}
		chain := p.CheckpointDatabaseChainSHA256
		for i, event := range p.JournalEvents {
			seq, se := recoverySequence(event.Sequence)
			_, re := recoverySequence(event.SourceRevision)
			file := "-"
			if event.FileID != nil {
				n, e := recoverySequence(*event.FileID)
				if e != nil || n < 1 {
					return nil, ErrNextcloudRecoveryDenied
				}
				file = *event.FileID
			}
			validKind := false
			for _, kind := range []string{"upsert", "metadata", "delete", "subtree_scan", "subtree_moved", "subtree_deleted", "reconcile", "withdrawn", "eligible", "binding_stop", "binding_resume", "binding_retired", "binding_created", "content_write_begin", "content_write_complete", "target_deleted", "tenant_deleted"} {
				if event.Kind == kind {
					validKind = true
				}
			}
			expectedKeys := 6
			if event.Kind == "target_deleted" {
				expectedKeys = 7
				proof, err := validateRecoveryTargetClosure(rows[i]["target_closure"], p.InstanceID)
				if err != nil || event.TargetClosure == nil || proof != *event.TargetClosure {
					return nil, ErrNextcloudRecoveryDenied
				}
			}
			if event.Kind == "tenant_deleted" {
				expectedKeys = 7
				proof, err := validateRecoveryTenantDeletion(rows[i]["tenant_deletion"], p.InstanceID)
				if err != nil || event.TenantDeletion == nil || proof != *event.TenantDeletion {
					return nil, ErrNextcloudRecoveryDenied
				}
			}
			if len(rows[i]) != expectedKeys || se != nil || seq != start+int64(i)+1 || re != nil ||
				!recoveryID.MatchString(event.BindingID) ||
				event.CreatedAt < 0 ||
				!validKind {
				return nil, ErrNextcloudRecoveryDenied
			}
			lines := []string{
				chain,
				event.Sequence,
				event.BindingID,
				file,
				event.Kind,
				event.SourceRevision,
				strconv.FormatInt(event.CreatedAt, 10),
			}
			if event.Kind == "target_deleted" {
				lines = append(lines, targetClosureJournalLines(*event.TargetClosure)...)
				closed, _ := canonicalPositiveTargetNumber(event.TargetClosure.Target.ClosedAt)
				if closed > event.CreatedAt+300 {
					return nil, ErrNextcloudRecoveryDenied
				}
			}
			if event.Kind == "tenant_deleted" {
				lines = append(lines, tenantDeletionJournalLines(*event.TenantDeletion)...)
				closed, _ := canonicalPositiveTargetNumber(event.TenantDeletion.Tenant.ClosedAt)
				if closed > event.CreatedAt+300 {
					return nil, ErrNextcloudRecoveryDenied
				}
			}
			chain = recoverySHA([]byte(strings.Join(lines, "\n")))
			if chain != p.DatabasePrefixHashes[event.Sequence] {
				return nil, ErrNextcloudRecoveryDenied
			}
		}
		if chain != p.ThroughDatabaseChainSHA256 {
			return nil, ErrNextcloudRecoveryDenied
		}
	}
	bindings := map[string]int64{}
	previous := ""
	for _, b := range p.CloseBindings {
		seq, e := recoverySequence(b.LastSequence)
		if e != nil || seq <= start || seq > end || !recoveryID.MatchString(b.BindingID) || b.BindingID <= previous {
			return nil, ErrNextcloudRecoveryDenied
		}
		bindings[b.BindingID] = seq
		previous = b.BindingID
	}
	previous = ""
	previousFile := ""
	for _, f := range p.WithdrawFiles {
		seq, e := recoverySequence(f.LastSequence)
		fid, fe := recoverySequence(f.FileID)
		if e != nil || fe != nil || fid < 1 || seq <= start || seq > end || bindings[f.BindingID] < seq ||
			f.BindingID < previous ||
			(f.BindingID == previous && f.FileID <= previousFile) {
			return nil, ErrNextcloudRecoveryDenied
		}
		previous = f.BindingID
		previousFile = f.FileID
	}
	previous = ""
	previousFile = ""
	if raw, ok := fields["latest_content_revisions"]; ok {
		var rows []map[string]json.RawMessage
		if json.Unmarshal(raw, &rows) != nil || len(rows) > 100000 {
			return nil, ErrNextcloudRecoveryDenied
		}
		for _, row := range rows {
			if len(row) != 4 ||
				(!bytes.Equal(bytes.TrimSpace(row["writing"]), []byte("true")) && !bytes.Equal(bytes.TrimSpace(row["writing"]), []byte("false"))) {
				return nil, ErrNextcloudRecoveryDenied
			}
		}
	}
	for _, f := range p.LatestContentRevisions {
		fid, e := recoverySequence(f.FileID)
		_, re := recoverySequence(f.Revision)
		if e != nil || re != nil || fid < 1 || bindings[f.BindingID] == 0 || !recoveryID.MatchString(f.BindingID) ||
			f.BindingID < previous ||
			(f.BindingID == previous && f.FileID <= previousFile) {
			return nil, ErrNextcloudRecoveryDenied
		}
		previous, previousFile = f.BindingID, f.FileID
	}
	p.digest = recoverySHA([]byte(signed.Plan))
	p.raw = signed.Plan
	return &p, nil
}

func recoveryPair(p NextcloudSourcePairing) NextcloudRecoveryPair {
	return NextcloudRecoveryPair{
		p.OperationID,
		p.TenantID,
		p.KnowledgeBaseID,
		p.DataSourceID,
		p.InstanceID,
		p.BindingID,
		p.BaseURL,
		p.ConfigSHA,
		p.PublicationEpoch,
		p.KeyID,
		p.State,
	}
}

// Capture inventories the actual local DB. It must be retained and independently
// pinned outside restored volumes together with the external checkpoint anchor.
func (s *NextcloudPublicationRecoveryStore) Capture(
	ctx context.Context,
	anchor NextcloudRecoveryAnchor,
) (*NextcloudRecoveryInventory, error) {
	if !recoveryID.MatchString(anchor.InstanceID) || !recoveryStream.MatchString(anchor.StreamID) ||
		!recoveryHash.MatchString(anchor.RecordSHA256) ||
		!recoveryHash.MatchString(anchor.DatabaseChainSHA256) {
		return nil, ErrNextcloudRecoveryDenied
	}
	if _, e := recoverySequence(anchor.Sequence); e != nil {
		return nil, e
	}
	var result *NextcloudRecoveryInventory
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var pairs []NextcloudSourcePairing
		if e := tx.Where("nextcloud_instance_id=?", anchor.InstanceID).
			Order("operation_id").
			Find(&pairs).
			Error; e != nil {
			return e
		}
		now, e := nextcloudLeaseNowMS(tx)
		if e != nil {
			return e
		}
		for _, p := range pairs {
			if _, e = nextcloudLockKBFence(tx, p.TenantID, p.KnowledgeBaseID, now); e != nil {
				return e
			}
		}
		result, e = s.captureInTx(tx, anchor)
		return e
	})
	return result, err
}

func (s *NextcloudPublicationRecoveryStore) captureInTx(
	tx *gorm.DB,
	a NextcloudRecoveryAnchor,
) (*NextcloudRecoveryInventory, error) {
	var pairs []NextcloudSourcePairing
	if e := recoveryPairCurrentQuery(tx, a.InstanceID).Order("operation_id").Find(&pairs).Error; e != nil {
		return nil, e
	}
	if len(pairs) == 0 {
		return nil, ErrNextcloudRecoveryDenied
	}
	// A source whose instance cannot be resolved cannot be classified as absent.
	var unpaired int64
	if e := tx.Table("data_sources ds").
		Where(("ds.type = ? AND ds.deleted_at IS NULL AND NOT EXISTS (SELECT 1 FROM nextcl" +
			"oud_source_pairings p WHERE p.datasource_id=ds.id AND p.tenant_id=ds.tenan" +
			"t_id AND p.knowledge_base_id=ds.knowledge_base_id)"), types.ConnectorTypeNextcloud).
		Count(&unpaired).
		Error; e != nil {
		return nil, e
	}
	if unpaired != 0 {
		return nil, ErrNextcloudRecoveryDenied
	}
	inv := &NextcloudRecoveryInventory{SchemaVersion: 1, Anchor: a, Pairs: make([]NextcloudRecoveryPair, 0, len(pairs))}
	states := make([]any, 0, len(pairs))
	for _, p := range pairs {
		if p.TenantID == 0 || p.TenantID > math.MaxInt64 || p.OperationID == "" || p.KnowledgeBaseID == "" ||
			p.DataSourceID == "" ||
			p.PublicationEpoch < 0 ||
			p.KeyID == "" ||
			(p.State != "active" && p.State != "pending") {
			return nil, ErrNextcloudRecoveryDenied
		}
		var kb types.KnowledgeBase
		if e := tx.Unscoped().
			Select("id", "tenant_id", "deleted_at").
			Where("id=? AND tenant_id=?", p.KnowledgeBaseID, p.TenantID).
			Take(&kb).
			Error; e != nil {
			return nil, ErrNextcloudRecoveryDenied
		}
		if kb.DeletedAt.Valid {
			var op types.NextcloudOnboardingOperation
			var closure NextcloudTargetClosure
			if tx.Where("operation_id=?", p.OperationID).Take(&op).Error != nil ||
				tx.Where("operation_id=?", p.OperationID).Take(&closure).Error != nil ||
				!validTargetClosure(closure, op) {
				return nil, ErrNextcloudRecoveryDenied
			}
		}
		var ds types.DataSource
		if e := tx.Where(("id=? AND tenant_id=? AND knowledge_base_id=? AND type=? AND deleted_at IS " +
			"NULL"), p.DataSourceID, p.TenantID, p.KnowledgeBaseID, types.ConnectorTypeNextcloud).
			Take(&ds).
			Error; e != nil {
			return nil, ErrNextcloudRecoveryDenied
		}
		base, hash, binding, e := NextcloudEventDataSourceIdentity(ds.Config)
		parsed, parseErr := ds.ParseConfig()
		configuredKey := ""
		if parsed != nil {
			configuredKey, _ = parsed.Credentials["key_id"].(string)
		}
		if parseErr != nil || configuredKey != p.KeyID || e != nil || base != p.BaseURL || hash != p.ConfigSHA ||
			binding != p.BindingID {
			return nil, ErrNextcloudRecoveryDenied
		}
		var versions []nextcloudSourceVersion
		if e = tx.Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=?", p.TenantID, p.KnowledgeBaseID, p.DataSourceID).
			Order("external_id").
			Find(&versions).
			Error; e != nil {
			return nil, e
		}
		var revisions []nextcloudRecoveryRevision
		if e = tx.Table("nextcloud_source_revisions").
			Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=?", p.TenantID, p.KnowledgeBaseID, p.DataSourceID).
			Order("external_id,revision").
			Find(&revisions).
			Error; e != nil {
			return nil, e
		}
		latest := map[string]nextcloudRecoveryRevision{}
		for _, revision := range revisions {
			prior := latest[revision.ExternalID]
			if revision.Revision != prior.Revision+1 {
				return nil, ErrNextcloudRecoveryDenied
			}
			latest[revision.ExternalID] = revision
		}
		for _, version := range versions {
			revision, ok := latest[version.ExternalID]
			if !ok || revision.DesiredETag != version.DesiredETag ||
				revision.CandidateKnowledgeID != version.CandidateKnowledgeID ||
				revision.State != version.State ||
				!revision.SourceUpdatedAt.Equal(version.UpdatedAt) {
				return nil, ErrNextcloudRecoveryDenied
			}
		}
		if len(latest) != len(versions) {
			return nil, ErrNextcloudRecoveryDenied
		}
		var rows []types.Knowledge
		if e = tx.Unscoped().
			Where("tenant_id=? AND knowledge_base_id=? AND channel=?", p.TenantID, p.KnowledgeBaseID, types.ConnectorTypeNextcloud).
			Order("id").
			Find(&rows).
			Error; e != nil {
			return nil, e
		}
		rowIDs := map[string]bool{}
		knowledge := make([]any, 0, len(rows))
		for _, k := range rows {
			m, e := strictLineageKnowledgeMetadata(k.Metadata)
			fid, fe := recoverySequence(m["nextcloud_file_id"])
			if e != nil || fe != nil || fid < 1 || m["datasource_id"] != p.DataSourceID ||
				m["nextcloud_instance_id"] != p.InstanceID ||
				m["nextcloud_binding_id"] != p.BindingID ||
				m["source_resource_id"] != p.BindingID ||
				m["external_id"] != "nextcloud:"+p.InstanceID+":"+m["nextcloud_file_id"] {
				return nil, ErrNextcloudRecoveryDenied
			}
			rowIDs[k.ID] = true
			knowledge = append(knowledge, struct {
				ID                        string
				Metadata                  types.JSON
				FilePath, FileType        string
				StorageSize               int64
				ParseStatus, EnableStatus string
				Deleted                   bool
			}{k.ID, k.Metadata, k.FilePath, k.FileType, k.StorageSize, k.ParseStatus, k.EnableStatus, k.DeletedAt.Valid})
		}
		for _, v := range versions {
			suffix := strings.TrimPrefix(v.ExternalID, "nextcloud:"+p.InstanceID+":")
			fid, fe := recoverySequence(suffix)
			if fe != nil || fid < 1 || suffix == v.ExternalID ||
				(v.CandidateKnowledgeID != "" && !rowIDs[v.CandidateKnowledgeID]) {
				return nil, ErrNextcloudRecoveryDenied
			}
		}
		var fences []nextcloudContentFence
		if e = tx.Where("tenant_id=? AND knowledge_base_id=?", p.TenantID, p.KnowledgeBaseID).
			Order("knowledge_id").
			Find(&fences).
			Error; e != nil {
			return nil, e
		}
		inv.Pairs = append(inv.Pairs, recoveryPair(p))
		states = append(states, struct {
			Pair      NextcloudRecoveryPair
			Status    string
			Versions  []nextcloudSourceVersion
			Revisions []nextcloudRecoveryRevision
			Knowledge []any
			Fences    []nextcloudContentFence
		}{recoveryPair(p), ds.Status, versions, revisions, knowledge, fences})
	}
	var e error

	if tx.Migrator().HasTable(&types.NextcloudOnboardingOperation{}) {
		controls, err := nextcloudControlMetadataInventory(tx)
		if err != nil {
			return nil, err
		}
		states = append(states, map[string]any{"auto_space_control_metadata": controls})
	}
	inv.SourceStateSHA256, e = recoveryJSONSHA(states)
	return inv, e
}

// DecodeNextcloudRecoveryInventory decodes nextcloud recovery inventory.
func DecodeNextcloudRecoveryInventory(data []byte, pin string) (*NextcloudRecoveryInventory, error) {
	if !recoveryHash.MatchString(pin) || recoverySHA(data) != pin {
		return nil, ErrNextcloudRecoveryDenied
	}
	var inv NextcloudRecoveryInventory
	if recoveryStrictJSON(data, &inv) != nil || inv.SchemaVersion != 1 || len(inv.Pairs) == 0 ||
		!recoveryHash.MatchString(inv.SourceStateSHA256) {
		return nil, ErrNextcloudRecoveryDenied
	}
	return &inv, nil
}

// Apply retires every restored pair for the anchored instance, including pairs
// untouched by the replay interval. It preserves rows and physical objects.
func (s *NextcloudPublicationRecoveryStore) Apply(
	ctx context.Context,
	p *NextcloudRecoveryPlan,
	inv *NextcloudRecoveryInventory,
	pin string,
) (*NextcloudRecoveryReceipt, error) {
	if p == nil || p.digest == "" || p.raw == "" || recoverySHA([]byte(p.raw)) != p.digest || inv == nil {
		return nil, ErrNextcloudRecoveryDenied
	}
	var original NextcloudRecoveryPlan
	if recoveryStrictJSON([]byte(p.raw), &original) != nil {
		return nil, ErrNextcloudRecoveryDenied
	}
	provided, _ := recoveryJSONSHA(p)
	verified, _ := recoveryJSONSHA(original)
	if provided != verified {
		return nil, ErrNextcloudRecoveryDenied
	}
	ih, e := recoveryJSONSHA(inv)
	if e != nil || ih != pin || inv.SchemaVersion != 1 || inv.Anchor.InstanceID != p.InstanceID ||
		inv.Anchor.StreamID != p.StreamID ||
		inv.Anchor.RecordSHA256 != p.CheckpointRecordSHA256 ||
		inv.Anchor.Sequence != p.CheckpointSequence ||
		inv.Anchor.DatabaseChainSHA256 != p.CheckpointDatabaseChainSHA256 {
		return nil, ErrNextcloudRecoveryDenied
	}
	var result NextcloudRecoveryReceipt
	evidencePinned := false
	e = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Serialize with every writer before checking the pinned restored snapshot.
		nowMS, e := nextcloudLeaseNowMS(tx)
		if e != nil {
			return e
		}
		for _, pair := range inv.Pairs {
			if _, e := nextcloudLockKBFence(tx, pair.TenantID, pair.KnowledgeBaseID, nowMS); e != nil {
				return e
			}
		}
		var existing NextcloudRecoveryReceipt
		er := tx.Where("instance_id=? AND stream_id=? AND checkpoint_record_sha256=?", p.InstanceID, p.StreamID, p.CheckpointRecordSHA256).
			Take(&existing).
			Error
		if er == nil {
			if existing.PlanSHA256 != p.digest || existing.InventorySHA256 != pin {
				return ErrNextcloudRecoveryDenied
			}
			existing.ClosedAt = existing.ClosedAt.UTC()
			result = existing
			if err := s.checkReceiptScopes(tx, existing, inv); err != nil {
				return err
			}
			evidencePinned = true
			if err := requireRecoveryPermanentFactOperationsInTx(tx, p); err != nil {
				return err
			}
			return requireRecoveryPermanentFactsRetainedInTx(tx, p)
		}
		if !errors.Is(er, gorm.ErrRecordNotFound) {
			return er
		}
		var prior int64
		if e := tx.Model(&NextcloudRecoveryReceipt{}).
			Where("instance_id=? AND (stream_id<>? OR through_sequence>?)", p.InstanceID, p.StreamID, mustRecoverySequence(p.CheckpointSequence)).
			Count(&prior).
			Error; e != nil {
			return e
		}
		if prior != 0 {
			return ErrNextcloudRecoveryDenied
		}
		current, e := s.captureInTx(tx, inv.Anchor)
		if e != nil {
			return e
		}
		actual, e := recoveryJSONSHA(current)
		if e != nil || actual != pin {
			return ErrNextcloudRecoveryDenied
		}
		evidencePinned = true
		if err := requireRecoveryPermanentFactOperationsInTx(tx, p); err != nil {
			return err
		}
		// Unknown interval bindings are proven absent only after the complete paired
		// source registry and pinned inventory have matched. They authorize nothing.
		now := time.Now().UTC().Truncate(time.Microsecond)
		leases := NewNextcloudContentLeaseStore(s.db)
		result = NextcloudRecoveryReceipt{
			p.digest,
			p.InstanceID,
			p.StreamID,
			p.CheckpointRecordSHA256,
			p.RecoveryRecordSHA256,
			mustRecoverySequence(p.CheckpointSequence),
			mustRecoverySequence(p.ThroughSequence),
			pin,
			p.raw,
			len(inv.Pairs),
			now,
		}
		if e := tx.Create(&result).Error; e != nil {
			return e
		}
		for _, pair := range inv.Pairs {
			var rows []types.Knowledge
			if e := tx.Unscoped().
				Where("tenant_id=? AND knowledge_base_id=? AND channel=?", pair.TenantID, pair.KnowledgeBaseID, types.ConnectorTypeNextcloud).
				Order("id").
				Find(&rows).
				Error; e != nil {
				return e
			}
			for _, row := range rows {
				metadata, e := strictLineageKnowledgeMetadata(row.Metadata)
				if e != nil {
					return e
				}
				scope := NextcloudContentScope{
					pair.TenantID,
					pair.KnowledgeBaseID,
					row.ID,
					pair.DataSourceID,
					metadata["external_id"],
				}
				if e = leases.RetireKnowledgeInTx(ctx, tx, scope); e != nil {
					return e
				}
				if e = writeNextcloudETag(tx, &row, ""); e != nil {
					return e
				}
			}
			if e := tx.Model(&nextcloudSourceVersion{}).
				Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=?", pair.TenantID, pair.KnowledgeBaseID, pair.DataSourceID).
				Updates(map[string]any{"state": "tombstone", "candidate_knowledge_id": "", "desired_etag": "", "updated_at": now}).
				Error; e != nil {
				return e
			}
			for _, file := range p.WithdrawFiles {
				if file.BindingID != pair.BindingID {
					continue
				}
				v := nextcloudSourceVersion{
					TenantID:        pair.TenantID,
					KnowledgeBaseID: pair.KnowledgeBaseID,
					DataSourceID:    pair.DataSourceID,
					ExternalID:      "nextcloud:" + pair.InstanceID + ":" + file.FileID,
					State:           "tombstone",
					UpdatedAt:       now,
				}
				if e := tx.Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND external_id=?", v.TenantID, v.KnowledgeBaseID, v.DataSourceID, v.ExternalID).
					FirstOrCreate(&v).
					Error; e != nil {
					return e
				}
			}
			if e := leases.RetireKBInTx(ctx, tx, pair.TenantID, pair.KnowledgeBaseID); e != nil {
				return e
			}
			// Restored GC claims are as stale as restored workers. Revoke their epochs
			// without touching objects; unknown external inventory cannot be collected.
			if e := tx.Exec(
				("UPDATE nextcloud_content_fences SET state='retired',epoch=epoch+1,claim_to" +
					"ken='',claim_until_ms=NULL,retired_at_ms=?,updated_at_ms=? WHERE tenant_id" +
					"=? AND knowledge_base_id=? AND state='deleting'"),
				nowMS,
				nowMS,
				pair.TenantID,
				pair.KnowledgeBaseID,
			).Error; e != nil {
				return e
			}
			if e := tx.Exec(
				"DELETE FROM nextcloud_content_lease_coverage WHERE tenant_id=? AND knowledge_base_id=?",
				pair.TenantID,
				pair.KnowledgeBaseID,
			).Error; e != nil {
				return e
			}
			if e := tx.Model(&types.DataSource{}).
				Where("id=? AND tenant_id=?", pair.DataSourceID, pair.TenantID).
				Update("status", types.DataSourceStatusPaused).
				Error; e != nil {
				return e
			}
			if e := tx.Model(&types.SyncLog{}).
				Where("data_source_id=? AND tenant_id=? AND status=?", pair.DataSourceID, pair.TenantID, types.SyncLogStatusRunning).
				Updates(map[string]any{"status": types.SyncLogStatusCanceled, "finished_at": now, "worker_active": false, "worker_attempt_token": "", "error_message": "publication_recovery_closed"}).
				Error; e != nil {
				return e
			}
			if e := tx.Table("nextcloud_candidate_retry_jobs").
				Where("datasource_id=? AND tenant_id=?", pair.DataSourceID, pair.TenantID).
				Updates(map[string]any{"state": "manual", "lease_token": nil, "lease_until": nil, "sync_log_id": nil, "last_error_code": "publication_recovery_closed", "updated_at": now}).
				Error; e != nil {
				return e
			}
			var kbFence nextcloudContentFence
			if e := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=''", pair.TenantID, pair.KnowledgeBaseID).
				Take(&kbFence).
				Error; e != nil {
				return e
			}
			retired := make([]nextcloudRecoveryRetiredGeneration, 0, len(rows))
			for _, row := range rows {
				var fence nextcloudContentFence
				if e := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", pair.TenantID, pair.KnowledgeBaseID, row.ID).
					Take(&fence).
					Error; e != nil {
					return e
				}
				if fence.State == "open" {
					return ErrNextcloudRecoveryDenied
				}
				retired = append(retired, nextcloudRecoveryRetiredGeneration{row.ID, fence.Epoch})
			}
			retiredJSON, e := json.Marshal(retired)
			if e != nil {
				return e
			}
			pairHash, e := recoveryJSONSHA(pair)
			if e != nil {
				return e
			}
			if e = tx.Create(
				&nextcloudRecoveryScope{
					PlanSHA256:             p.digest,
					OperationID:            pair.OperationID,
					TenantID:               pair.TenantID,
					KnowledgeBaseID:        pair.KnowledgeBaseID,
					DataSourceID:           pair.DataSourceID,
					InstanceID:             pair.InstanceID,
					BindingID:              pair.BindingID,
					PairSHA256:             pairHash,
					RetiredGenerationsJSON: string(retiredJSON),
					ClosedKBEpoch:          kbFence.Epoch,
					State:                  "closed",
				},
			).Error; e != nil {
				return e
			}
		}
		if e := applyRecoveryTargetClosuresInTx(ctx, tx, p); e != nil {
			return e
		}
		if e := applyRecoveryTenantDeletionsInTx(tx, p); e != nil {
			return e
		}
		return closeNextcloudControlsInTx(tx, p.InstanceID, p.digest, pin)
	})
	if e != nil {
		if evidencePinned && errors.Is(e, ErrNextcloudRecoveryUnknownOperation) {
			// Keep the established instance restore gate closed even though the body
			// transaction rolls back. No partial receipt or fabricated op is committed.
			closeContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			defer cancel()
			closedErr := s.db.WithContext(closeContext).Transaction(func(tx *gorm.DB) error {
				if !tx.Migrator().HasTable(&nextcloudControlRecoveryGate{}) {
					return ErrNextcloudRecoveryDenied
				}
				return closeNextcloudControlsInTx(tx, p.InstanceID, p.digest, pin)
			})
			if closedErr != nil {
				return nil, errors.Join(ErrNextcloudRecoveryDenied, e, closedErr)
			}
			return nil, errors.Join(ErrNextcloudRecoveryDenied, e)
		}
		return nil, e
	}
	return &result, nil
}
func mustRecoverySequence(v string) int64 { n, _ := recoverySequence(v); return n }

func (s *NextcloudPublicationRecoveryStore) checkReceiptScopes(
	tx *gorm.DB,
	r NextcloudRecoveryReceipt,
	inv *NextcloudRecoveryInventory,
) error {
	var scopes []nextcloudRecoveryScope
	if e := tx.Where("plan_sha256=?", r.PlanSHA256).Find(&scopes).Error; e != nil {
		return e
	}
	if len(scopes) != len(inv.Pairs) || len(scopes) != r.PairCount {
		return ErrNextcloudRecoveryDenied
	}
	for _, pair := range inv.Pairs {
		var current NextcloudSourcePairing
		if e := tx.Where("operation_id=? AND tenant_id=?", pair.OperationID, pair.TenantID).
			Take(&current).
			Error; e != nil {
			return ErrNextcloudRecoveryDenied
		}
		currentHash, _ := recoveryJSONSHA(recoveryPair(current))
		hash, _ := recoveryJSONSHA(pair)
		if currentHash != hash {
			return ErrNextcloudRecoveryDenied
		}
		found := false
		for _, scope := range scopes {
			if scope.OperationID == pair.OperationID && scope.PairSHA256 == hash {
				found = true
			}
		}
		if !found {
			return ErrNextcloudRecoveryDenied
		}
		for _, scope := range scopes {
			if scope.OperationID == pair.OperationID {
				if e := s.checkRecoveryScopeInTx(tx, scope); e != nil {
					return e
				}
			}
		}
	}
	return nil
}

// Reconcile is available only to the local maintenance CLI. It probes the
// persisted machine configuration itself; no request boolean, URL or personal
// impersonation can supply its identity/complete-manifest evidence. This permits
// NEW generations. Global ingress and authoritative directory checks remain an
// independent cross-service recovery gate.
func (s *NextcloudPublicationRecoveryStore) Reconcile(
	ctx context.Context,
	planSHA, operationID string,
) (*NextcloudRecoveryReconciliationReceipt, error) {
	var scope nextcloudRecoveryScope
	var receipt NextcloudRecoveryReceipt
	if !recoveryHash.MatchString(planSHA) || operationID == "" {
		return nil, ErrNextcloudRecoveryDenied
	}
	if e := s.db.WithContext(ctx).
		Where("plan_sha256=? AND operation_id=?", planSHA, operationID).
		Take(&scope).
		Error; e != nil {
		return nil, ErrNextcloudRecoveryDenied
	}
	if e := s.db.WithContext(ctx).Where("plan_sha256=?", planSHA).Take(&receipt).Error; e != nil {
		return nil, e
	}
	var pair NextcloudSourcePairing
	var ds types.DataSource
	if e := s.db.WithContext(ctx).
		Where("operation_id=? AND tenant_id=?", operationID, scope.TenantID).
		Take(&pair).
		Error; e != nil {
		return nil, ErrNextcloudRecoveryDenied
	}
	hash, _ := recoveryJSONSHA(recoveryPair(pair))
	if hash != scope.PairSHA256 || pair.State != "active" {
		return nil, ErrNextcloudRecoveryDenied
	}
	if e := s.db.WithContext(ctx).
		Where("id=? AND tenant_id=? AND knowledge_base_id=? AND deleted_at IS NULL", scope.DataSourceID, scope.TenantID, scope.KnowledgeBaseID).
		Take(&ds).
		Error; e != nil {
		return nil, e
	}
	base, configSHA, binding, e := NextcloudEventDataSourceIdentity(ds.Config)
	if e != nil || base != pair.BaseURL || configSHA != pair.ConfigSHA || binding != pair.BindingID {
		return nil, ErrNextcloudRecoveryDenied
	}
	cfg, e := ds.ParseConfig()
	if e != nil {
		return nil, e
	}
	cfg.Type = types.ConnectorTypeNextcloud
	remotePair := nextcloud.SourcePairingCommit{
		OperationID:     pair.OperationID,
		InstanceID:      pair.InstanceID,
		BindingID:       pair.BindingID,
		TenantID:        pair.TenantID,
		KnowledgeBaseID: pair.KnowledgeBaseID,
		DataSourceID:    pair.DataSourceID,
	}
	if e = nextcloud.CommitSourcePairing(ctx, cfg, remotePair); e != nil {
		return nil, e
	}
	observed, e := nextcloud.InspectRecoveryManifest(ctx, cfg)
	if e != nil {
		return nil, e
	}
	if observed.Identity.InstanceID != pair.InstanceID || observed.Identity.BindingID != pair.BindingID ||
		observed.Identity.BaseURL != pair.BaseURL ||
		observed.Identity.PublicationState != "active" ||
		observed.Identity.PublicationEpoch <= pair.PublicationEpoch {
		return nil, ErrNextcloudRecoveryDenied
	}
	var plan NextcloudRecoveryPlan
	if recoveryStrictJSON([]byte(receipt.PlanJSON), &plan) != nil {
		return nil, ErrNextcloudRecoveryDenied
	}
	for _, withdraw := range plan.WithdrawFiles {
		if withdraw.BindingID != pair.BindingID {
			continue
		}
		for _, file := range observed.Files {
			if strconv.FormatInt(file.FileID, 10) == withdraw.FileID {
				return nil, ErrNextcloudRecoveryDenied
			}
		}
	}
	evidenceSHA, e := recoveryJSONSHA(struct {
		PlanSHA256, PairSHA256 string
		Manifest               nextcloud.RecoveryManifest
	}{planSHA, scope.PairSHA256, observed})
	if e != nil {
		return nil, e
	}
	if e = nextcloud.CommitSourcePairing(ctx, cfg, remotePair); e != nil {
		return nil, e
	}
	result := &NextcloudRecoveryReconciliationReceipt{
		PlanSHA256:           planSHA,
		OperationID:          operationID,
		PairSHA256:           scope.PairSHA256,
		ReconciliationSHA256: evidenceSHA,
		Manifest:             observed,
	}
	e = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		nowMS, e := nextcloudLeaseNowMS(tx)
		if e != nil {
			return e
		}
		fence, e := nextcloudLockKBFence(tx, scope.TenantID, scope.KnowledgeBaseID, nowMS)
		if e != nil {
			return e
		}
		var current nextcloudRecoveryScope
		if e = tx.Where("plan_sha256=? AND operation_id=?", planSHA, operationID).Take(&current).Error; e != nil {
			return e
		}
		var currentPair NextcloudSourcePairing
		if e = tx.Where("operation_id=?", operationID).Take(&currentPair).Error; e != nil {
			return e
		}
		ph, _ := recoveryJSONSHA(recoveryPair(currentPair))
		if ph != scope.PairSHA256 {
			return ErrNextcloudRecoveryDenied
		}
		if e = s.checkRecoveryScopeInTx(tx, current); e != nil {
			return e
		}
		if current.State == "reconciled" {
			if current.ReconciliationSHA256 != evidenceSHA || current.ReconciledAt == nil ||
				current.ReconciliationJSON == "" {
				return ErrNextcloudRecoveryDenied
			}
			if recoveryStrictJSON([]byte(current.ReconciliationJSON), result) != nil {
				return ErrNextcloudRecoveryDenied
			}
			return nil
		}
		var currentDS types.DataSource
		if e = tx.Where(("id=? AND tenant_id=? AND knowledge_base_id=? AND status=? AND deleted_at I" +
			"S NULL"), scope.DataSourceID, scope.TenantID, scope.KnowledgeBaseID, types.DataSourceStatusPaused).
			Take(&currentDS).
			Error; e != nil {
			return ErrNextcloudRecoveryDenied
		}
		currentBase, currentSHA, currentBinding, e := NextcloudEventDataSourceIdentity(currentDS.Config)
		if e != nil || currentBase != pair.BaseURL || currentSHA != pair.ConfigSHA || currentBinding != pair.BindingID {
			return ErrNextcloudRecoveryDenied
		}
		if current.State != "closed" || fence.State != "retired" || fence.Epoch == math.MaxInt64 {
			return ErrNextcloudRecoveryDenied
		}
		// Leave unknown physical/GC coverage blocked after rollback. Reconciliation
		// must not turn retirement into permission to erase unobserved copies.
		if e = tx.Exec(
			"DELETE FROM nextcloud_content_lease_coverage WHERE tenant_id=? AND knowledge_base_id=?",
			scope.TenantID,
			scope.KnowledgeBaseID,
		).Error; e != nil {
			return e
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		result.ReconciledAt = now
		resultJSON, e := json.Marshal(result)
		if e != nil {
			return e
		}
		if e = tx.Model(&nextcloudRecoveryScope{}).
			Where("plan_sha256=? AND operation_id=? AND state='closed'", planSHA, operationID).
			Updates(map[string]any{"state": "reconciled", "reconciliation_sha256": evidenceSHA, "reconciliation_json": string(resultJSON), "reconciled_at": now}).
			Error; e != nil {
			return e
		}
		if e = tx.Exec(
			("UPDATE nextcloud_content_fences SET state='open',epoch=epoch+1,retired_at_" +
				"ms=NULL,updated_at_ms=? WHERE tenant_id=? AND knowledge_base_id=? AND know" +
				"ledge_id='' AND state='retired'"),
			nowMS,
			scope.TenantID,
			scope.KnowledgeBaseID,
		).Error; e != nil {
			return e
		}
		// Reset stale deletion cursors: old generations stay tombstoned; the next
		// sync must download and stage the current authoritative files as new IDs.
		update := tx.Model(&types.DataSource{}).
			Where("id=? AND tenant_id=? AND status=?", scope.DataSourceID, scope.TenantID, types.DataSourceStatusPaused).
			Updates(map[string]any{"status": types.DataSourceStatusActive, "last_sync_cursor": nil})
		if update.Error != nil {
			return update.Error
		}
		if update.RowsAffected != 1 {
			return ErrNextcloudRecoveryDenied
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	return result, nil
}

// A receipt is not proof that a partial restore preserved its closed rows. Every
// retry checks the retired generations and monotone KB epoch persisted at close.
func (s *NextcloudPublicationRecoveryStore) checkRecoveryScopeInTx(tx *gorm.DB, scope nextcloudRecoveryScope) error {
	var retired []nextcloudRecoveryRetiredGeneration
	if scope.ClosedKBEpoch <= 0 || scope.RetiredGenerationsJSON == "" ||
		recoveryStrictJSON([]byte(scope.RetiredGenerationsJSON), &retired) != nil {
		return ErrNextcloudRecoveryDenied
	}
	var kb nextcloudContentFence
	if e := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=''", scope.TenantID, scope.KnowledgeBaseID).
		Take(&kb).
		Error; e != nil {
		return ErrNextcloudRecoveryDenied
	}
	if kb.Epoch < scope.ClosedKBEpoch || (scope.State == "closed" && kb.State == "open") ||
		(scope.State == "reconciled" && kb.Epoch <= scope.ClosedKBEpoch) {
		return ErrNextcloudRecoveryDenied
	}
	for _, generation := range retired {
		var f nextcloudContentFence
		if generation.KnowledgeID == "" || generation.Epoch <= 0 {
			return ErrNextcloudRecoveryDenied
		}
		if e := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=? AND datasource_id=?", scope.TenantID, scope.KnowledgeBaseID, generation.KnowledgeID, scope.DataSourceID).
			Take(&f).
			Error; e != nil {
			return ErrNextcloudRecoveryDenied
		}
		if f.State == "open" || f.Epoch < generation.Epoch {
			return ErrNextcloudRecoveryDenied
		}
	}
	if scope.State == "closed" {
		var ds types.DataSource
		if e := tx.Where("id=? AND tenant_id=? AND knowledge_base_id=? AND deleted_at IS NULL", scope.DataSourceID, scope.TenantID, scope.KnowledgeBaseID).
			Take(&ds).
			Error; e != nil ||
			ds.Status != types.DataSourceStatusPaused {
			return ErrNextcloudRecoveryDenied
		}
		var count int64
		if e := tx.Model(&nextcloudSourceVersion{}).
			Where(("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND state<>'tombst" +
				"one'"), scope.TenantID, scope.KnowledgeBaseID, scope.DataSourceID).
			Count(&count).
			Error; e != nil {
			return e
		}
		if count != 0 {
			return ErrNextcloudRecoveryDenied
		}
		if e := tx.Unscoped().
			Model(&types.Knowledge{}).
			Where(("tenant_id=? AND knowledge_base_id=? AND channel=? AND COALESCE(metadata->>" +
				"'nextcloud_etag','')<>''"), scope.TenantID, scope.KnowledgeBaseID, types.ConnectorTypeNextcloud).
			Count(&count).
			Error; e != nil {
			return e
		}
		if count != 0 {
			return ErrNextcloudRecoveryDenied
		}
		if e := tx.Model(&types.SyncLog{}).
			Where("tenant_id=? AND data_source_id=? AND status=?", scope.TenantID, scope.DataSourceID, types.SyncLogStatusRunning).
			Count(&count).
			Error; e != nil {
			return e
		}
		if count != 0 {
			return ErrNextcloudRecoveryDenied
		}
	}
	return nil
}
