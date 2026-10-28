package repository

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"

	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// Only the connector's original fetch and the durable exact build lease may
// create this purpose. It never supplies a Caller or a public read capability.
type (
	scopedOnboardingConfigurationPurposeKey struct{}
	scopedOnboardingConfigurationReadKey    struct{}
	scopedOnboardingConfigurationPurpose    struct {
		originalFetch  bool
		tenant         uint64
		kb, datasource string
		parent         context.Context
		check          func(context.Context) error
		checkInTx      func(context.Context, *gorm.DB) error
	}
)

// ScopedOnboardingConfigurationRead represents scoped onboarding configuration read repository data.
type ScopedOnboardingConfigurationRead interface {
	Context(context.Context) context.Context
	Checkpoint(context.Context) error
	Close() error
}
type scopedOnboardingConfigurationReadSlot struct {
	mu          sync.Mutex
	parent      context.Context
	tenant      uint64
	kb          string
	lineage     *types.SourceLineage
	read        ScopedOnboardingConfigurationRead
	closed      bool
	projections []scopedConfigurationProjection
	err         error
}
type scopedOnboardingConfigurationSourceBeginner interface {
	BeginKnowledgeConfigurationSources(
		context.Context,
		uint64,
		string,
		*types.SourceLineage,
	) (context.Context, func(), error)
}

func scopedOnboardingConfigurationPurposeFromContext(ctx context.Context, tenant uint64, kb string) (string, error) {
	if ctx == nil {
		return "", types.ErrSourceLineageUnknown
	}
	purpose, ok := ctx.Value(scopedOnboardingConfigurationPurposeKey{}).(scopedOnboardingConfigurationPurpose)
	if !ok || purpose.tenant != tenant || purpose.kb != kb || purpose.datasource == "" || purpose.check == nil ||
		purpose.parent == nil {
		return "", types.ErrSourceLineageUnknown
	}
	if err := ctx.Err(); err != nil {
		return "", context.Cause(ctx)
	}
	if err := purpose.parent.Err(); err != nil {
		return "", context.Cause(purpose.parent)
	}
	if err := purpose.check(ctx); err != nil {
		return "", err
	}
	return purpose.datasource, nil
}

func scopedOnboardingConfigurationPurposeInTx(ctx context.Context, tx *gorm.DB, tenant uint64, kb string) error {
	purpose, ok := ctx.Value(scopedOnboardingConfigurationPurposeKey{}).(scopedOnboardingConfigurationPurpose)
	if !ok || purpose.tenant != tenant || purpose.kb != kb || purpose.datasource == "" || purpose.parent == nil ||
		purpose.checkInTx == nil {
		return types.ErrSourceLineageUnknown
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if err := purpose.parent.Err(); err != nil {
		return context.Cause(purpose.parent)
	}
	return purpose.checkInTx(ctx, tx)
}

// NextcloudOnboardingConfigurationStore returns the nextcloud onboarding configuration store result.
func (s *KnowledgeInputProvenanceStore) NextcloudOnboardingConfigurationStore() *NextcloudOnboardingStore {
	if s == nil || s.db == nil {
		return nil
	}
	return NewNextcloudOnboardingStore(s.db)
}

// This only reads the original receipt and monotonic controls. Configuration
// text is loaded by the consumer after its source and physical guard is held.
func (s *KnowledgeInputProvenanceStore) scopedConfigurationLineageMetadata(
	ctx context.Context,
	tenant uint64,
	kb string,
) (*types.SourceLineage, error) {
	var owner struct {
		ID       string
		TenantID uint64
	}
	if err := s.db.WithContext(ctx).
		Table("knowledge_bases").
		Select("id", "tenant_id").
		Where("id = ? AND tenant_id = ? AND deleted_at IS NULL", kb, tenant).
		Take(&owner).
		Error; err != nil {
		return nil, err
	}
	revision, err := knowledgeConfigurationControlRevision(s.db.WithContext(ctx), kb)
	if err != nil {
		return nil, err
	}
	var origin knowledgeConfigurationInputOrigin
	if err := s.db.WithContext(ctx).
		Select("origin_id", "tenant_id", "knowledge_base_id", "config_revision", "source_lineage").
		Where("tenant_id = ? AND knowledge_base_id = ? AND config_revision = ?", tenant, kb, revision).
		Take(&origin).
		Error; err != nil {
		return nil, err
	}
	var lineage types.SourceLineage
	if origin.OriginID == "" || json.Unmarshal([]byte(origin.SourceLineage), &lineage) != nil ||
		lineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	return types.UnionSourceLineage(&lineage)
}

func (s *KnowledgeInputProvenanceStore) beginScopedConfigurationPurpose(
	ctx context.Context,
	purpose scopedOnboardingConfigurationPurpose,
) (context.Context, func(), error) {
	noop := func() {}
	if s == nil || s.db == nil || ctx == nil {
		return ctx, noop, types.ErrSourceLineageUnknown
	}
	purpose.parent = ctx
	slot := &scopedOnboardingConfigurationReadSlot{parent: ctx, tenant: purpose.tenant, kb: purpose.kb}
	current := context.WithValue(
		context.WithValue(logprivacy.SuppressContent(ctx), scopedOnboardingConfigurationPurposeKey{}, purpose),
		scopedOnboardingConfigurationReadKey{},
		slot,
	)
	closeRead := func() { CloseScopedOnboardingConfigurationReads(current) }
	if _, err := scopedOnboardingConfigurationPurposeFromContext(current, purpose.tenant, purpose.kb); err != nil {
		closeRead()
		return ctx, noop, err
	}
	lineage, err := s.scopedConfigurationLineageMetadata(current, purpose.tenant, purpose.kb)
	if err != nil {
		closeRead()
		return ctx, noop, err
	}
	beginner, ok := s.originalInputSources.(scopedOnboardingConfigurationSourceBeginner)
	if !ok {
		closeRead()
		return ctx, noop, types.ErrSourceLineageUnknown
	}
	current, _, err = beginner.BeginKnowledgeConfigurationSources(current, purpose.tenant, purpose.kb, lineage)
	if err != nil {
		closeRead()
		return ctx, noop, err
	}
	if current == nil {
		closeRead()
		return ctx, noop, types.ErrSourceLineageUnknown
	}
	return current, closeRead, nil
}

// This is a metadata/control check, separate from the config-byte lease. A
// legacy independently configured real pair needs no onboarding actor, but its
// exact current pairing, encrypted connector identity and recovery gate remain
// mandatory. Call it before acquiring/hydrating a configuration body.
func scopedConfigurationPairInTx(
	tx *gorm.DB,
	tenant uint64,
	kb, datasource string,
) (NextcloudSourcePairing, uint64, error) {
	var pair NextcloudSourcePairing
	if err := requireNextcloudTargetOpenInTx(tx, tenant, kb); err != nil {
		return pair, 0, err
	}
	if err := tx.Where(("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND state = " +
		"'active'"), tenant, kb, datasource).
		Take(&pair).
		Error; err != nil {
		return pair, 0, err
	}
	closed, err := nextcloudControlsClosed(tx, pair.InstanceID)
	if err != nil {
		return pair, 0, err
	}
	if closed {
		return pair, 0, ErrKnowledgeInputChanged
	}
	var ds types.DataSource
	if err := tx.Select("id", "tenant_id", "knowledge_base_id", "type", "status", "config").
		Where(
			("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND type = ? AND status" +
				" IN (?, ?) AND deleted_at IS NULL"),
			datasource,
			tenant,
			kb,
			types.ConnectorTypeNextcloud,
			types.DataSourceStatusActive,
			types.DataSourceStatusError,
		).
		Take(&ds).
		Error; err != nil {
		return pair, 0, err
	}
	base, digest, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
	if err != nil || base != pair.BaseURL || digest != pair.ConfigSHA || binding != pair.BindingID ||
		pair.OperationID == "" ||
		pair.InstanceID == "" {
		return pair, 0, ErrKnowledgeInputChanged
	}
	revision, err := nextcloudUploadSourceRevision(tx, datasource)
	return pair, revision, err
}

func sameScopedConfigurationPair(a, b NextcloudSourcePairing) bool {
	return a.OperationID == b.OperationID && a.TenantID == b.TenantID && a.KnowledgeBaseID == b.KnowledgeBaseID &&
		a.DataSourceID == b.DataSourceID &&
		a.InstanceID == b.InstanceID &&
		a.BindingID == b.BindingID &&
		a.BaseURL == b.BaseURL &&
		a.ConfigSHA == b.ConfigSHA &&
		a.PublicationEpoch == b.PublicationEpoch &&
		a.KeyID == b.KeyID &&
		a.State == b.State
}

// BeginNextcloudOriginalFetchConfigurationRead is called by the actual sync
// producer before GetKnowledgeBase/ResolveProcessConfig/connector transport.
// A supplied object is matched to the actual active data source and its
// monotonic original fetch version; UUIDs alone never create this purpose.
func (s *KnowledgeInputProvenanceStore) BeginNextcloudOriginalFetchConfigurationRead(
	ctx context.Context,
	ds *types.DataSource,
) (context.Context, func(), error) {
	noop := func() {}
	if s == nil || s.db == nil || ds == nil || ds.TenantID == 0 || ds.KnowledgeBaseID == "" ||
		ds.Type != types.ConnectorTypeNextcloud ||
		ds.Status != types.DataSourceStatusActive && ds.Status != types.DataSourceStatusError {
		return ctx, noop, ErrKnowledgeInputChanged
	}
	wanted, err := KnowledgeInputDigest(ds.Config)
	if err != nil {
		return ctx, noop, err
	}
	pair, revision, err := scopedConfigurationPairInTx(s.db.WithContext(ctx), ds.TenantID, ds.KnowledgeBaseID, ds.ID)
	if err != nil {
		return ctx, noop, err
	}
	tenant, kb, id := ds.TenantID, ds.KnowledgeBaseID, ds.ID
	checkInTx := func(current context.Context, tx *gorm.DB) error {
		var actual types.DataSource
		if err := tx.WithContext(current).
			Select("id", "tenant_id", "knowledge_base_id", "type", "status", "config").
			Where(("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND type = ? AND status" +
				" IN (?, ?)"), id, tenant, kb, types.ConnectorTypeNextcloud,
				types.DataSourceStatusActive, types.DataSourceStatusError).
			Take(&actual).
			Error; err != nil {
			return err
		}
		digest, err := KnowledgeInputDigest(actual.Config)
		if err != nil || digest != wanted {
			return ErrKnowledgeInputChanged
		}
		currentPair, now, err := scopedConfigurationPairInTx(tx.WithContext(current), tenant, kb, id)
		if err != nil {
			return err
		}
		if now != revision || !sameScopedConfigurationPair(pair, currentPair) {
			return ErrKnowledgeInputChanged
		}
		return nil
	}
	check := func(current context.Context) error {
		return s.db.WithContext(current).Transaction(func(tx *gorm.DB) error { return checkInTx(current, tx) })
	}
	return s.beginScopedConfigurationPurpose(
		ctx,
		scopedOnboardingConfigurationPurpose{
			originalFetch: true,
			tenant:        tenant,
			kb:            kb,
			datasource:    id,
			check:         check,
			checkInTx:     checkInTx,
		},
	)
}

// BeginNextcloudBuildConfigurationRead is a fresh worker boundary. It does
// not accept an actor, a data-source selector or a client-provided purpose.
// The real SQL lease store validates the durable row inside its own transaction
// even if a caller supplied a different implementation in its context.
func (s *KnowledgeInputProvenanceStore) BeginNextcloudBuildConfigurationRead(
	ctx context.Context,
) (context.Context, func(), error) {
	noop := func() {}
	if ctx == nil {
		return ctx, noop, types.ErrSourceLineageUnknown
	}
	if _, present := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext); !present {
		return ctx, noop, nil
	}
	if s == nil || s.db == nil {
		return ctx, noop, types.ErrSourceLineageUnknown
	}
	lease, scope, err := NextcloudBuildInputLease(ctx)
	if err != nil {
		return ctx, noop, err
	}
	pair, revision, err := scopedConfigurationPairInTx(
		s.db.WithContext(ctx),
		scope.TenantID,
		scope.KnowledgeBaseID,
		scope.DataSourceID,
	)
	if err != nil {
		return ctx, noop, err
	}
	checkInTx := func(current context.Context, tx *gorm.DB) error {
		if err := NewNextcloudContentLeaseStore(s.db).ValidateBuildLeaseInTx(current, tx, lease, scope); err != nil {
			return err
		}
		currentPair, now, err := scopedConfigurationPairInTx(
			tx,
			scope.TenantID,
			scope.KnowledgeBaseID,
			scope.DataSourceID,
		)
		if err != nil {
			return err
		}
		if now != revision || !sameScopedConfigurationPair(pair, currentPair) {
			return ErrKnowledgeInputChanged
		}
		// Extract only the immutable source identity, never the arbitrary
		// metadata blob or knowledge body before the actual body guard.
		var k struct {
			ID              string
			TenantID        uint64
			KnowledgeBaseID string
			Channel         string
			DataSourceID    string
			ExternalID      string
		}
		projection := ("id,tenant_id,knowledge_base_id,channel,json_extract(metadata,'$.datasource" +
			"_id') AS data_source_id,json_extract(metadata,'$.external_id') AS external" +
			"_id")
		switch tx.Name() {
		case "postgres":
			projection = ("id,tenant_id,knowledge_base_id,channel,metadata->>'datasource_id' AS data_" +
				"source_id,metadata->>'external_id' AS external_id")
		case "sqlite":
		default:
			return ErrKnowledgeInputChanged
		}
		if err := tx.Table("knowledges").
			Select(projection).
			Where(
				"id = ? AND tenant_id = ? AND knowledge_base_id = ? AND deleted_at IS NULL",
				scope.KnowledgeID,
				scope.TenantID,
				scope.KnowledgeBaseID,
			).
			Take(&k).
			Error; err != nil {
			return err
		}
		if k.Channel != types.ConnectorTypeNextcloud || k.DataSourceID != scope.DataSourceID ||
			k.ExternalID != scope.ExternalID {
			return ErrKnowledgeInputChanged
		}
		return nil
	}
	check := func(current context.Context) error {
		if err := CheckNextcloudBuildLease(current); err != nil {
			return err
		}
		return s.db.WithContext(current).Transaction(func(tx *gorm.DB) error { return checkInTx(current, tx) })
	}
	return s.beginScopedConfigurationPurpose(
		ctx,
		scopedOnboardingConfigurationPurpose{
			tenant:     scope.TenantID,
			kb:         scope.KnowledgeBaseID,
			datasource: scope.DataSourceID,
			check:      check,
			checkInTx:  checkInTx,
		},
	)
}

// HoldScopedOnboardingConfigurationRead returns the hold scoped onboarding configuration read result.
func HoldScopedOnboardingConfigurationRead(
	ctx context.Context,
	tenant uint64,
	kb string,
	lineage *types.SourceLineage,
	read ScopedOnboardingConfigurationRead,
) error {
	if _, err := scopedOnboardingConfigurationPurposeFromContext(ctx, tenant, kb); err != nil {
		return err
	}
	slot, ok := ctx.Value(scopedOnboardingConfigurationReadKey{}).(*scopedOnboardingConfigurationReadSlot)
	if !ok || slot == nil || slot.tenant != tenant || slot.kb != kb || read == nil {
		return types.ErrSourceLineageUnknown
	}
	copied, err := types.UnionSourceLineage(lineage)
	if err != nil {
		return err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.closed || slot.read != nil {
		return ErrKnowledgeInputChanged
	}
	slot.lineage, slot.read = copied, read
	return nil
}

// CheckScopedOnboardingConfigurationSources checks scoped onboarding configuration sources.
// A configuration check must match the original configuration's full source
// union. A build may also check its subset after the separately verified exact
// unpublished source is removed. Other inherited inputs keep public guards.
func CheckScopedOnboardingConfigurationSources(
	ctx context.Context,
	tenant uint64,
	kb string,
	lineage *types.SourceLineage,
	subset bool,
) (bool, error) {
	if ctx == nil {
		return false, types.ErrSourceLineageUnknown
	}
	slot, ok := ctx.Value(scopedOnboardingConfigurationReadKey{}).(*scopedOnboardingConfigurationReadSlot)
	if !ok || slot == nil || slot.tenant != tenant || slot.kb != kb {
		return false, nil
	}
	slot.mu.Lock()
	if slot.closed {
		slot.mu.Unlock()
		return true, ErrKnowledgeInputChanged
	}
	if err := slot.parent.Err(); err != nil {
		slot.mu.Unlock()
		return true, context.Cause(slot.parent)
	}
	wanted, err := types.UnionSourceLineage(lineage)
	if err != nil {
		slot.mu.Unlock()
		return true, err
	}
	if slot.read == nil || slot.lineage == nil {
		slot.mu.Unlock()
		return false, nil
	}
	if !subset && !reflect.DeepEqual(wanted, slot.lineage) {
		slot.mu.Unlock()
		return false, nil
	}
	if subset {
		for _, source := range wanted.Sources {
			found := false
			for _, admitted := range slot.lineage.Sources {
				found = found || source == admitted
			}
			if !found {
				slot.mu.Unlock()
				return false, nil
			}
		}
	}
	if err := checkScopedConfigurationProjections(slot); err != nil {
		slot.mu.Unlock()
		return true, err
	}
	read := slot.read
	slot.mu.Unlock()
	if _, err := scopedOnboardingConfigurationPurposeFromContext(ctx, tenant, kb); err != nil {
		return true, err
	}
	return true, read.Checkpoint(ctx)
}

// CloseScopedOnboardingConfigurationReads returns the close scoped onboarding configuration reads result.
func CloseScopedOnboardingConfigurationReads(ctx context.Context) {
	if ctx == nil {
		return
	}
	slot, _ := ctx.Value(scopedOnboardingConfigurationReadKey{}).(*scopedOnboardingConfigurationReadSlot)
	if slot == nil {
		return
	}
	slot.mu.Lock()
	if slot.closed {
		slot.mu.Unlock()
		return
	}
	slot.closed = true
	read := slot.read
	slot.mu.Unlock()
	// Close can wait for a heartbeat/base-pool connection. Never hold the
	// slot lock across that wait while an existing SQL consumer needs it.
	if read != nil {
		_ = read.Close()
	}
}

// NextcloudBuildConfigurationReadRequired returns the nextcloud build configuration read required result.
func NextcloudBuildConfigurationReadRequired(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	_, present := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext)
	return present
}

// CheckScopedOnboardingConfigurationReads checks scoped onboarding configuration reads.
// Check before loading any original configuration/body snapshot. The guard's
// own recheck is deliberately nonrecursive: it reads metadata, then freshly
// authorized leased configuration bytes, without calling this snapshot API.
func CheckScopedOnboardingConfigurationReads(ctx context.Context) error {
	if ctx == nil {
		return types.ErrSourceLineageUnknown
	}
	slot, _ := ctx.Value(scopedOnboardingConfigurationReadKey{}).(*scopedOnboardingConfigurationReadSlot)
	if slot == nil {
		return nil
	}
	slot.mu.Lock()
	if slot.closed {
		slot.mu.Unlock()
		return ErrKnowledgeInputChanged
	}
	if err := slot.parent.Err(); err != nil {
		slot.mu.Unlock()
		return context.Cause(slot.parent)
	}
	if err := checkScopedConfigurationProjections(slot); err != nil {
		slot.mu.Unlock()
		return err
	}
	read, tenant, kb := slot.read, slot.tenant, slot.kb
	slot.mu.Unlock()
	if _, err := scopedOnboardingConfigurationPurposeFromContext(ctx, tenant, kb); err != nil {
		return err
	}
	if read != nil {
		return read.Checkpoint(ctx)
	}
	return nil
}
