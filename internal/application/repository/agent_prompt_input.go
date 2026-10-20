package repository

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// KnowledgeBasePromptMetadata is the bounded projection covered by original
// configuration and display producers. Descriptions, generated profiles,
// document counts and candidate titles are separate inputs, never implied here.
type KnowledgeBasePromptMetadata struct {
	TenantID                            uint64
	KnowledgeBaseID, Name, Type         string
	IsTemporary, EverHadNextcloudSource bool
	Capabilities                        []string
	SourceLineage                       *types.SourceLineage
}

// KnowledgeBasePromptInputSnapshot cannot be constructed from caller labels.
// The private original facts and monotonic revisions survive every recheck.
type KnowledgeBasePromptInputSnapshot struct {
	metadata      KnowledgeBasePromptMetadata
	configuration knowledgeConfigurationInputOrigin
	display       knowledgeDisplayInputOrigin
	caller        types.Caller
	principal     types.Principal
	owner         string
	binding       string
	physical      *knowledgeBasePromptPhysicalRead
}

// Metadata returns a detached projection; changing it cannot alter the receipt.
func (s *KnowledgeBasePromptInputSnapshot) Metadata() KnowledgeBasePromptMetadata {
	if s == nil {
		return KnowledgeBasePromptMetadata{}
	}
	result := s.metadata
	result.Capabilities = append([]string{}, s.metadata.Capabilities...)
	result.SourceLineage, _ = types.UnionSourceLineage(s.metadata.SourceLineage)
	return result
}

func knowledgeBasePromptBinding(s *KnowledgeBasePromptInputSnapshot) (string, error) {
	return KnowledgeInputDigest(struct {
		Metadata      KnowledgeBasePromptMetadata
		Configuration knowledgeConfigurationInputOrigin
		Display       knowledgeDisplayInputOrigin
		Caller        types.Caller
		Principal     types.Principal
		Owner         string
	}{s.metadata, s.configuration, s.display, s.caller, s.principal, s.owner})
}

func captureKnowledgeBasePromptInputInTx(
	ctx context.Context,
	tx *gorm.DB,
	tenant uint64,
	kbID string,
) (*KnowledgeBasePromptInputSnapshot, error) {
	caller, callerOK := ctx.Value(types.CallerContextKey).(types.Caller)
	principal, principalOK := ctx.Value(types.PrincipalContextKey).(types.Principal)
	if tenant == 0 || kbID == "" || !callerOK || caller.TenantID == 0 || !principalOK || !principal.Valid() ||
		principal != principal.Normalize() {
		return nil, types.ErrSourceLineageUnknown
	}
	// Worker configuration purposes are not public metadata read capabilities.
	if ctx.Value(scopedOnboardingConfigurationReadKey{}) != nil ||
		ctx.Value(scopedOnboardingConfigurationPurposeKey{}) != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if err := requireNextcloudTargetOpenInTx(tx, tenant, kbID); err != nil {
		return nil, err
	}
	if err := requireNextcloudTargetTenantLiveInTx(tx, tenant); err != nil {
		return nil, err
	}
	columns := append([]string{}, knowledgeConfigurationOriginalBodyColumns...)
	columns = append(
		columns,
		"id",
		"tenant_id",
		"name",
		"type",
		"is_temporary",
		"ever_had_nextcloud_source",
		"embedding_model_id",
		"summary_model_id",
	)
	var kb types.KnowledgeBase
	if err := tx.Select(columns).Where("id = ? AND tenant_id = ?", kbID, tenant).Take(&kb).Error; err != nil {
		return nil, err
	}
	configurationLineage, err := readKnowledgeConfigurationOrigin(tx, &kb)
	if err != nil {
		return nil, err
	}
	configRevision, err := knowledgeConfigurationControlRevision(tx, kbID)
	if err != nil {
		return nil, err
	}
	var config knowledgeConfigurationInputOrigin
	if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND config_revision = ?", tenant, kbID, configRevision).
		Take(&config).
		Error; err != nil {
		return nil, err
	}
	name, err := readKnowledgeDisplayOrigin(tx, "kb-name", kb.ID, tenant, kb.ID, kb.Name)
	if err != nil {
		return nil, err
	}
	var display knowledgeDisplayInputOrigin
	if err := tx.Where(("kind = 'kb-name' AND row_id = ? AND tenant_id = ? AND knowledge_base_id = " +
		"? AND revision = ?"), kbID, tenant, kbID, name.Revision).
		Take(&display).
		Error; err != nil {
		return nil, err
	}
	lineage, err := types.UnionSourceLineage(configurationLineage, name.SourceLineage)
	if err != nil {
		return nil, err
	}
	capabilities := []string{}
	if kb.IsWikiEnabled() {
		capabilities = append(capabilities, "wiki")
	}
	if kb.IsVectorEnabled() || kb.IsKeywordEnabled() {
		capabilities = append(capabilities, "chunks")
	}
	snapshot := &KnowledgeBasePromptInputSnapshot{
		metadata: KnowledgeBasePromptMetadata{
			TenantID:               tenant,
			KnowledgeBaseID:        kb.ID,
			Name:                   kb.Name,
			Type:                   kb.Type,
			IsTemporary:            kb.IsTemporary,
			EverHadNextcloudSource: kb.EverHadNextcloudSource,
			Capabilities:           capabilities,
			SourceLineage:          lineage,
		},
		configuration: config,
		display:       display,
		caller:        caller,
		principal:     principal,
		owner:         types.SessionOwnerIDFromContext(ctx),
	}
	snapshot.binding, err = knowledgeBasePromptBinding(snapshot)
	return snapshot, err
}

// CaptureKnowledgeBasePromptInputs reads original KB-name and configuration
// admissions without enumerating any file. Current caller/share/group/Agent
// grants remain the consuming service's independently refreshed authority.
func (s *KnowledgeInputProvenanceStore) CaptureKnowledgeBasePromptInputs(
	ctx context.Context,
	tenant uint64,
	kbID string,
) (*KnowledgeBasePromptInputSnapshot, error) {
	if ctx == nil || s == nil || s.db == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	physical, err := s.beginKnowledgeBasePromptPhysicalRead(ctx, tenant, kbID)
	if err != nil {
		return nil, err
	}
	var result *KnowledgeBasePromptInputSnapshot
	err = s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var err error
		result, err = captureKnowledgeBasePromptInputInTx(ctx, tx, tenant, kbID)
		return err
	})
	if err != nil {
		_ = physical.Close()
		return nil, err
	}
	if result.configuration.OriginID != physical.plan.OriginID ||
		result.configuration.ConfigRevision != physical.plan.ConfigRevision ||
		result.configuration.InputDigest != physical.plan.ConfigDigest ||
		result.display.OriginID != physical.origin {
		_ = physical.Close()
		return nil, ErrKnowledgeInputChanged
	}
	result.physical = physical
	if err := s.RecheckKnowledgeBasePromptInputs(ctx, result); err != nil {
		_ = physical.Close()
		return nil, err
	}
	return result, nil
}

// RecheckKnowledgeBasePromptInputs keeps the original admissions and actor
// binding. A current name/config row cannot certify a prior unknown body.
func (s *KnowledgeInputProvenanceStore) RecheckKnowledgeBasePromptInputs(
	ctx context.Context,
	original *KnowledgeBasePromptInputSnapshot,
) error {
	if original == nil || original.physical == nil {
		return types.ErrSourceLineageUnknown
	}
	if err := original.physical.check(ctx); err != nil {
		return err
	}
	binding, err := knowledgeBasePromptBinding(original)
	if err != nil || binding != original.binding {
		return ErrKnowledgeInputChanged
	}
	return s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		current, err := captureKnowledgeBasePromptInputInTx(
			ctx,
			tx,
			original.metadata.TenantID,
			original.metadata.KnowledgeBaseID,
		)
		if err != nil {
			return err
		}
		if current.binding != original.binding {
			return ErrKnowledgeInputChanged
		}
		// Also validate the exact original JSON rather than relying on an opaque
		// hash as proof of producer completeness.
		var lineage types.SourceLineage
		if json.Unmarshal([]byte(original.configuration.SourceLineage), &lineage) != nil {
			return types.ErrSourceLineageUnknown
		}
		return lineage.RequireComplete()
	})
}

const agentPromptInputTTL = 2 * time.Minute

type knowledgeBasePromptPhysicalRead struct {
	mu                          sync.Mutex
	store                       *KnowledgeInputProvenanceStore
	plan                        *KnowledgeConfigurationOriginalBodyReadPlan
	configLeases, displayLeases []string
	displayHeads                []originalBodyFieldHead
	origin                      string
	ctx                         context.Context
	cancel                      context.CancelCauseFunc
	stop, done                  chan struct{}
	closed                      bool
	err                         error
}

func (s *KnowledgeInputProvenanceStore) beginKnowledgeBasePromptPhysicalRead(
	ctx context.Context,
	tenant uint64,
	kb string,
) (*knowledgeBasePromptPhysicalRead, error) {
	plan, err := s.KnowledgeConfigurationBodyReadPlan(ctx, tenant, kb)
	if err != nil {
		return nil, err
	}
	var original struct{ OriginID string }
	revision, err := knowledgeDisplayRevision(s.db.WithContext(ctx), "kb-name", kb)
	if err != nil {
		return nil, err
	}
	if err := s.db.WithContext(ctx).
		Model(&knowledgeDisplayInputOrigin{}).
		Select("origin_id").
		Where(("kind='kb-name' AND row_id=? AND tenant_id=? AND knowledge_base_id=? AND re" +
			"vision=?"), kb, tenant, kb, revision).
		Take(&original).
		Error; err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancelCause(ctx)
	read := &knowledgeBasePromptPhysicalRead{
		store:  s,
		plan:   plan,
		origin: original.OriginID,
		ctx:    lifetime,
		cancel: cancel,
		stop:   make(chan struct{}),
	}
	fail := func(err error) (*knowledgeBasePromptPhysicalRead, error) { _ = read.Close(); return nil, err }
	if original.OriginID == "" {
		return fail(types.ErrSourceLineageUnknown)
	}
	if err := bodyInternal(
		s.db.WithContext(ctx),
	).Where(("source_table='knowledge_display_input_origins' AND row_key=? AND column_na" +
		"me='actual_input'"), original.OriginID).
		Order("slot_id").
		Find(&read.displayHeads).
		Error; err != nil {
		return fail(err)
	}
	if len(read.displayHeads) == 0 {
		return fail(ErrOriginalBodyUnavailable)
	}
	owner := "agent-prompt:" + uuid.NewString()
	read.configLeases, err = s.AcquireKnowledgeConfigurationBodyReadLeases(ctx, plan, owner, agentPromptInputTTL)
	if err != nil {
		return fail(err)
	}
	for _, head := range read.displayHeads {
		if head.TenantID != tenant || head.KnowledgeBaseID != kb || head.BodyID == "" {
			return fail(ErrOriginalBodyUnavailable)
		}
		id, err := NewOriginalBodyRetentionStore(
			s.db,
		).AcquireLease(ctx, head.BodyID, "read", owner, agentPromptInputTTL)
		if err != nil {
			return fail(err)
		}
		read.displayLeases = append(read.displayLeases, id)
	}
	if err := read.check(ctx); err != nil {
		return fail(err)
	}
	read.done = make(chan struct{})
	go read.heartbeat()
	return read, nil
}

func (r *knowledgeBasePromptPhysicalRead) check(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.err != nil {
		return ErrOriginalBodyUnavailable
	}
	if err := r.ctx.Err(); err != nil {
		return context.Cause(r.ctx)
	}
	if err := r.store.RecheckKnowledgeConfigurationBodyReadLeases(
		ctx,
		r.plan,
		r.configLeases,
		agentPromptInputTTL,
	); err != nil {
		return err
	}
	var heads []originalBodyFieldHead
	if err := bodyInternal(
		r.store.db.WithContext(ctx),
	).Where("source_table='knowledge_display_input_origins' AND row_key=? AND column_name='actual_input'", r.origin).
		Order("slot_id").
		Find(&heads).
		Error; err != nil {
		return err
	}
	if !reflect.DeepEqual(heads, r.displayHeads) {
		return ErrOriginalBodyUnavailable
	}
	for _, id := range r.displayLeases {
		if err := NewOriginalBodyRetentionStore(r.store.db).RenewLease(ctx, id, agentPromptInputTTL); err != nil {
			return err
		}
	}
	return nil
}

func (r *knowledgeBasePromptPhysicalRead) heartbeat() {
	defer close(r.done)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(r.ctx, 20*time.Second)
			err := r.check(ctx)
			cancel()
			if err != nil {
				r.mu.Lock()
				r.err = err
				r.cancel(err)
				r.mu.Unlock()
				return
			}
		}
	}
}

func (r *knowledgeBasePromptPhysicalRead) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.cancel(ErrOriginalBodyUnavailable)
	close(r.stop)
	done := r.done
	r.mu.Unlock()
	if done != nil {
		<-done
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var failures []error
	for _, id := range append(append([]string{}, r.configLeases...), r.displayLeases...) {
		failures = append(failures, NewOriginalBodyRetentionStore(r.store.db).ReleaseLease(ctx, id))
	}
	return errors.Join(failures...)
}

// Close releases the original config/name payload leases after every model
// and buffered HTTP consumer has finished with the retained prompt.
func (s *KnowledgeBasePromptInputSnapshot) Close() error {
	if s == nil {
		return nil
	}
	return s.physical.Close()
}
