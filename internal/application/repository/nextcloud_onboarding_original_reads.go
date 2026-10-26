package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const onboardingConfigurationReadTTL = 2 * time.Minute

// NextcloudOnboardingConfigurationRead represents nextcloud onboarding configuration read repository data.
// This guard has no ordinary actor role. The credential approves exactly one
// template; every inherited original separately requires current scoped WK
// entitlement for the actual GUID and a fresh signed Nextcloud READ decision.
type NextcloudOnboardingConfigurationRead struct {
	mu                                 sync.Mutex
	store                              *NextcloudOnboardingStore
	credential                         types.NextcloudOnboardingCredential
	operation                          *types.NextcloudOnboardingOperation
	manager                            *NextcloudOnboardingManagerPurpose
	request                            *types.NextcloudOnboardingRequest
	directory, guid, datasource, owner string
	tenant                             uint64
	kb                                 string
	origin                             knowledgeConfigurationInputOrigin
	lineage                            *types.SourceLineage
	configPlan                         *KnowledgeConfigurationOriginalBodyReadPlan
	configBodyLeases                   []string
	sourceHeads                        map[string][]originalBodyFieldHead
	sourceParents                      map[string][]originalBodyParentRef
	sourceBodyLeases                   map[string]string
	sourceBodies                       map[string]originalBodyPayload
	leases                             []NextcloudContentLease
	closed                             atomic.Bool
	lifetime                           context.Context
	cancel                             context.CancelCauseFunc
	heartbeatStop                      chan struct{}
	heartbeatDone                      chan struct{}
	heartbeatError                     error
}

// BeginScopedOnboardingConfigurationRead can be reached only from the actual
// connector or the durable exact build-lease purpose installed by the store.
// Arbitrary tenant/KB/lineage arguments never create a worker capability.
func (s *NextcloudOnboardingStore) BeginScopedOnboardingConfigurationRead(
	ctx context.Context,
	tenant uint64,
	kb string,
	lineage *types.SourceLineage,
) (*NextcloudOnboardingConfigurationRead, error) {
	dsID, err := scopedOnboardingConfigurationPurposeFromContext(ctx, tenant, kb)
	if err != nil {
		return nil, err
	}
	tx := s.db.WithContext(ctx)
	var pair NextcloudSourcePairing
	if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND state='active'", tenant, kb, dsID).
		Take(&pair).
		Error; err != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	var op types.NextcloudOnboardingOperation
	if err := tx.Where(("operation_id=? AND tenant_id=? AND knowledge_base_id=? AND instance_id=? A" +
		"ND binding_id=? AND state='active'"), pair.OperationID, tenant, kb, pair.InstanceID, pair.BindingID).
		Take(&op).
		Error; err != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	var cred types.NextcloudOnboardingCredential
	if err := tx.Where("id=? AND enabled=?", op.CredentialID, true).Take(&cred).Error; err != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if op.TenantID != cred.TenantID || op.InstanceID != cred.InstanceID ||
		op.ProcessingTemplateID != cred.TemplateKBID ||
		op.ActorDirectoryID != cred.DirectoryID {
		return nil, types.ErrSourceLineageUnknown
	}
	manager, err := currentOnboardingManagerPurpose(tx, cred, op)
	if err != nil {
		return nil, err
	}
	copied := op
	return s.beginOnboardingConfigurationRead(
		ctx,
		cred,
		manager.ActorDirectoryID,
		manager.ActorObjectGUID,
		tenant,
		kb,
		lineage,
		&copied,
		nil,
		dsID,
	)
}

func (s *NextcloudOnboardingStore) beginOnboardingTemplateRead(
	ctx context.Context,
	c types.NextcloudOnboardingCredential,
	req types.NextcloudOnboardingRequest,
) (*NextcloudOnboardingConfigurationRead, error) {
	_, lineage, err := ensureKnowledgeConfigurationMetadata(ctx, s.db, c.TenantID, c.TemplateKBID)
	if err != nil {
		return nil, err
	}
	copied2 := req
	return s.beginOnboardingConfigurationRead(
		ctx,
		c,
		req.ActorDirectoryID,
		req.ActorObjectGUID,
		c.TenantID,
		c.TemplateKBID,
		lineage,
		nil,
		&copied2,
		"",
	)
}

func (s *NextcloudOnboardingStore) beginOnboardingExistingConfigurationRead(
	ctx context.Context,
	c types.NextcloudOnboardingCredential,
	req types.NextcloudOnboardingRequest,
) (*NextcloudOnboardingConfigurationRead, error) {
	if req.Mode != "existing" || req.KnowledgeBaseID == "" {
		return nil, ErrNextcloudOnboardingDenied
	}
	actor, err := s.Actor(ctx, c, req.ActorDirectoryID, req.ActorObjectGUID)
	if err != nil {
		return nil, err
	}
	ctx = nextcloudCandidateContext(ctx, c.TenantID, actor)
	allowed, err := s.candidateAccessible(ctx, c.TenantID, req.KnowledgeBaseID)
	if err != nil || !allowed {
		return nil, ErrNextcloudKBUnavailable
	}
	if _, _, err := onboardingExistingTarget(s.db.WithContext(ctx), c.TenantID, req.KnowledgeBaseID); err != nil {
		return nil, err
	}
	_, lineage, err := ensureKnowledgeConfigurationMetadata(ctx, s.db, c.TenantID, req.KnowledgeBaseID)
	if err != nil {
		return nil, err
	}
	copied3 := req
	return s.beginOnboardingConfigurationRead(
		ctx,
		c,
		req.ActorDirectoryID,
		req.ActorObjectGUID,
		c.TenantID,
		req.KnowledgeBaseID,
		lineage,
		nil,
		&copied3,
		"",
	)
}

func (s *NextcloudOnboardingStore) beginOnboardingConfigurationRead(
	ctx context.Context,
	c types.NextcloudOnboardingCredential,
	dir, guid string,
	tenant uint64,
	kb string,
	lineage *types.SourceLineage,
	op *types.NextcloudOnboardingOperation,
	req *types.NextcloudOnboardingRequest,
	dsID string,
) (*NextcloudOnboardingConfigurationRead, error) {
	if ctx == nil || lineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	origin, actualLineage, err := onboardingConfigurationMetadata(s.db.WithContext(ctx), tenant, kb)
	if err != nil {
		return nil, err
	}
	canonical, err := types.UnionSourceLineage(lineage)
	if err != nil || !reflect.DeepEqual(canonical, actualLineage) {
		return nil, types.ErrSourceLineageUnknown
	}
	r := &NextcloudOnboardingConfigurationRead{
		store:            s,
		credential:       c,
		operation:        op,
		request:          req,
		directory:        dir,
		guid:             guid,
		datasource:       dsID,
		tenant:           tenant,
		kb:               kb,
		origin:           origin,
		lineage:          actualLineage,
		sourceHeads:      map[string][]originalBodyFieldHead{},
		sourceParents:    map[string][]originalBodyParentRef{},
		sourceBodyLeases: map[string]string{},
		sourceBodies:     map[string]originalBodyPayload{},
	}
	fail := func(err error) (*NextcloudOnboardingConfigurationRead, error) { _ = r.Close(); return nil, err }
	if op != nil {
		r.manager, err = currentOnboardingManagerPurpose(s.db.WithContext(ctx), c, *op)
		if err != nil {
			return fail(err)
		}
		if r.manager.ActorDirectoryID != dir || r.manager.ActorObjectGUID != guid {
			return fail(ErrKnowledgeInputChanged)
		}
	}
	// All authority checks precede config/body SELECTs and durable lease writes.
	if err := r.checkAuthority(ctx, s.db.WithContext(ctx)); err != nil {
		return fail(err)
	}
	r.lifetime, r.cancel = context.WithCancelCause(ctx)
	owner := "nc-config-read:" + uuid.NewString()
	r.owner = owner
	leaseStore := NewNextcloudContentLeaseStore(s.db)
	scopes := map[string]types.KnowledgeSearchScope{fmt.Sprintf("%d:%s", tenant, kb): {TenantID: tenant, KBID: kb}}
	for _, source := range actualLineage.Sources {
		scopes[fmt.Sprintf("%d:%s", source.TenantID, source.KnowledgeBaseID)] = types.KnowledgeSearchScope{
			TenantID: source.TenantID,
			KBID:     source.KnowledgeBaseID,
		}
	}
	keys := make([]string, 0, len(scopes))
	for key := range scopes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		scope := scopes[key]
		lease, err := leaseStore.AcquireKBRead(ctx, scope.TenantID, scope.KBID, owner, onboardingConfigurationReadTTL)
		if err != nil {
			return fail(err)
		}
		r.leases = append(r.leases, lease)
	}
	for _, source := range actualLineage.Sources {
		scope := NextcloudContentScope{
			TenantID:        source.TenantID,
			KnowledgeBaseID: source.KnowledgeBaseID,
			KnowledgeID:     source.KnowledgeID,
			DataSourceID:    source.DataSourceID,
			ExternalID:      source.ExternalID,
		}
		lease, err := leaseStore.AcquireKnowledge(
			ctx,
			scope,
			NextcloudContentReadLease,
			owner,
			onboardingConfigurationReadTTL,
		)
		if err != nil {
			return fail(err)
		}
		r.leases = append(r.leases, lease)
		if uint64(lease.Epoch) != source.Revision {
			return fail(ErrKnowledgeInputChanged)
		}
	}
	provenance := NewKnowledgeInputProvenanceStore(s.db)
	r.configPlan, err = provenance.KnowledgeConfigurationBodyReadPlan(ctx, tenant, kb)
	if err != nil {
		return fail(err)
	}
	if r.configPlan.OriginID != origin.OriginID || r.configPlan.ConfigRevision != origin.ConfigRevision ||
		r.configPlan.ConfigDigest != origin.InputDigest {
		return fail(ErrKnowledgeInputChanged)
	}
	r.configBodyLeases, err = provenance.AcquireKnowledgeConfigurationBodyReadLeases(
		ctx,
		r.configPlan,
		owner,
		onboardingConfigurationReadTTL,
	)
	if err != nil {
		return fail(err)
	}
	// Original knowledge heads and scoped vault parent references are control
	// metadata. Hold their actual payload leases; a broad lease is not substituted
	// for a body lease or for a current source decision.
	for _, source := range actualLineage.Sources {
		var heads []originalBodyFieldHead
		if err := bodyInternal(
			s.db.WithContext(ctx),
		).Where(
			("source_table='knowledges' AND row_key=? AND tenant_id=? AND knowledge_base" +
				"_id=? AND knowledge_id=?"),
			source.KnowledgeID,
			source.TenantID,
			source.KnowledgeBaseID,
			source.KnowledgeID,
		).
			Order("slot_id").
			Find(&heads).
			Error; err != nil {
			return fail(err)
		}
		if len(heads) == 0 {
			return fail(ErrOriginalBodyUnavailable)
		}
		r.sourceHeads[source.KnowledgeID] = heads
		for _, head := range heads {
			var parents []originalBodyParentRef
			if err := bodyInternal(
				s.db.WithContext(ctx),
			).Where("body_id=?", head.BodyID).
				Order("tenant_id,knowledge_base_id,knowledge_id,admission_id").
				Find(&parents).
				Error; err != nil {
				return fail(err)
			}
			for _, parent := range parents {
				if _, ok := scopes[fmt.Sprintf("%d:%s", parent.TenantID, parent.KnowledgeBaseID)]; !ok {
					return fail(ErrOriginalBodyUnavailable)
				}
			}
			r.sourceParents[head.BodyID] = parents
			if _, held := r.sourceBodyLeases[head.BodyID]; held {
				continue
			}
			var body originalBodyPayload
			if err := bodyInternal(
				s.db.WithContext(ctx),
			).Select("id,tenant_id,body_sha256,origin_domain_sha256,body_bytes,storage_kind,state,purge_epoch").
				Where("id=?", head.BodyID).
				Take(&body).
				Error; err != nil {
				return fail(err)
			}
			if body.State != "live" || body.BodySHA256 == "" {
				return fail(ErrOriginalBodyUnavailable)
			}
			id, err := NewOriginalBodyRetentionStore(
				s.db,
			).AcquireLease(ctx, head.BodyID, "read", owner, onboardingConfigurationReadTTL)
			if err != nil {
				return fail(err)
			}
			r.sourceBodies[head.BodyID] = body
			r.sourceBodyLeases[head.BodyID] = id
		}
	}
	if err := r.Checkpoint(ctx); err != nil {
		return fail(err)
	}
	r.startHeartbeat()
	return r, nil
}

// Context returns the context result.
func (r *NextcloudOnboardingConfigurationRead) Context(ctx context.Context) context.Context {
	if r == nil {
		return ctx
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.lineage.Sources) > 0 {
		ctx = logprivacy.SuppressContent(ctx)
	}
	coverage, _ := ctx.Value(faqReadLeaseCoverageKey{}).(faqReadLeaseCoverage)
	all := append([]NextcloudContentLease{}, coverage.Leases...)
	seen := map[string]bool{}
	for _, lease := range all {
		seen[lease.ID] = true
	}
	for _, lease := range r.leases {
		if !seen[lease.ID] {
			all = append(all, lease)
			seen[lease.ID] = true
		}
	}
	covered := context.WithValue(ctx, faqReadLeaseCoverageKey{}, faqReadLeaseCoverage{Leases: all})
	if r.lifetime == nil {
		return covered
	}
	child, cancel := context.WithCancelCause(covered)
	stop := context.AfterFunc(r.lifetime, func() { cancel(context.Cause(r.lifetime)) })
	context.AfterFunc(child, func() { stop() })
	if r.lifetime.Err() != nil {
		cancel(context.Cause(r.lifetime))
	}
	return child
}

func (r *NextcloudOnboardingConfigurationRead) checkAuthority(ctx context.Context, tx *gorm.DB) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c, err := currentOnboardingCredential(tx, r.credential)
	if err != nil {
		return err
	}
	if err := currentOnboardingActor(tx, c, r.directory, r.guid); err != nil {
		return err
	}
	if err := requireNextcloudTargetOpenInTx(tx, r.tenant, r.kb); err != nil {
		return err
	}
	if r.operation != nil {
		manager, err := currentOnboardingManagerPurpose(tx, c, *r.operation)
		if err != nil {
			return err
		}
		if !sameOnboardingManagerPurpose(manager, r.manager) || manager.ActorDirectoryID != r.directory ||
			manager.ActorObjectGUID != r.guid {
			return ErrKnowledgeInputChanged
		}
		var op types.NextcloudOnboardingOperation
		if err := tx.Where("operation_id=? AND state='active'", r.operation.OperationID).Take(&op).Error; err != nil {
			return err
		}
		if !sameOnboardingOperation(op, *r.operation) {
			return ErrKnowledgeInputChanged
		}
		var pair NextcloudSourcePairing
		if err := tx.Where(("operation_id=? AND tenant_id=? AND knowledge_base_id=? AND datasource_id=?" +
			" AND state='active'"), op.OperationID, r.tenant, r.kb, r.datasource).
			Take(&pair).
			Error; err != nil {
			return err
		}
		if pair.InstanceID != c.InstanceID || pair.BindingID != op.BindingID || pair.BaseURL != c.BaseURL {
			return ErrKnowledgeInputChanged
		}
		var target types.DataSource
		if err := tx.Select("id,tenant_id,knowledge_base_id,type,status,config").
			Where(
				("id=? AND tenant_id=? AND knowledge_base_id=? AND type=? AND status=? AND d" +
					"eleted_at IS NULL"),
				r.datasource,
				r.tenant,
				r.kb,
				types.ConnectorTypeNextcloud,
				types.DataSourceStatusActive,
			).
			Take(&target).
			Error; err != nil {
			return err
		}
		base, sha, binding, err := NextcloudEventDataSourceIdentity(target.Config)
		if err != nil || base != pair.BaseURL || sha != pair.ConfigSHA || binding != pair.BindingID {
			return ErrKnowledgeInputChanged
		}
		config, err := target.ParseConfig()
		if err != nil || config == nil || config.Settings["onboarding_operation_id"] != op.OperationID {
			return ErrKnowledgeInputChanged
		}
	} else {
		if r.request == nil || r.tenant != c.TenantID || r.request.ActorDirectoryID != r.directory ||
			r.request.ActorObjectGUID != r.guid ||
			r.request.ProcessingTemplateID != c.TemplateKBID {
			return ErrNextcloudOnboardingDenied
		}
		expected := c.TemplateKBID
		if r.request.Mode == "existing" {
			expected = r.request.KnowledgeBaseID
		}
		if r.kb != expected {
			return ErrNextcloudOnboardingDenied
		}
	}

	origin, lineage, err := onboardingConfigurationMetadata(tx, r.tenant, r.kb)
	if err != nil {
		return err
	}
	if !sameOnboardingConfigurationMetadata(origin, r.origin) || !reflect.DeepEqual(lineage, r.lineage) {
		return ErrKnowledgeInputChanged
	}
	for _, source := range r.lineage.Sources {
		if err := r.checkSource(ctx, tx, source); err != nil {
			return err
		}
	}
	return nil
}

func (r *NextcloudOnboardingConfigurationRead) checkSource(
	ctx context.Context,
	tx *gorm.DB,
	source types.SourceIdentity,
) error {
	if source.Validate() != nil {
		return types.ErrSourceLineageUnknown
	}
	revision, err := types.NormalizeNextcloudContentRevision(source.ContentRevision)
	if err != nil {
		return err
	}
	if err := requireNextcloudTargetOpenInTx(tx, source.TenantID, source.KnowledgeBaseID); err != nil {
		return err
	}
	closed, err := nextcloudControlsClosed(tx, source.InstanceID)
	if err != nil {
		return err
	}
	if closed {
		return access.ErrNextcloudPublicationUnavailable
	}
	var pair NextcloudSourcePairing
	if err := tx.Where(
		("operation_id=? AND tenant_id=? AND knowledge_base_id=? AND datasource_id=?" +
			" AND nextcloud_instance_id=? AND binding_id=? AND state='active'"),
		source.PairOperationID,
		source.TenantID,
		source.KnowledgeBaseID,
		source.DataSourceID,
		source.InstanceID,
		source.BindingID,
	).
		Take(&pair).
		Error; err != nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	var op types.NextcloudOnboardingOperation
	if err := tx.Where(
		("operation_id=? AND tenant_id=? AND knowledge_base_id=? AND instance_id=? A" +
			"ND binding_id=? AND actor_directory_id=? AND state='active'"),
		pair.OperationID,
		source.TenantID,
		source.KnowledgeBaseID,
		source.InstanceID,
		source.BindingID,
		r.directory,
	).
		Take(&op).
		Error; err != nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	var sourceCred types.NextcloudOnboardingCredential
	if err := tx.Where(
		"id=? AND tenant_id=? AND instance_id=? AND directory_id=? AND enabled=?",
		op.CredentialID,
		source.TenantID,
		source.InstanceID,
		r.directory,
		true,
	).
		Take(&sourceCred).
		Error; err != nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if sourceCred.TemplateKBID != op.ProcessingTemplateID || sourceCred.BaseURL != pair.BaseURL {
		return access.ErrNextcloudPublicationUnavailable
	}
	var version nextcloudSourceVersion
	if err := nextcloudVersionQuery(
		tx,
		source.TenantID,
		source.KnowledgeBaseID,
		source.DataSourceID,
		source.ExternalID,
	).Take(&version).
		Error; err != nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if version.State != "published" || version.CandidateKnowledgeID != source.KnowledgeID ||
		version.DesiredETag != source.ETag ||
		version.DesiredContentRevision != revision {
		return ErrKnowledgeInputChanged
	}
	var fence nextcloudContentFence
	if err := tx.Where(
		"tenant_id=? AND knowledge_base_id=? AND knowledge_id=?",
		source.TenantID,
		source.KnowledgeBaseID,
		source.KnowledgeID,
	).
		Take(&fence).
		Error; err != nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if fence.State != "open" || fence.Epoch <= 0 || uint64(fence.Epoch) != source.Revision ||
		fence.DataSourceID != source.DataSourceID ||
		fence.ExternalID != source.ExternalID {
		return ErrKnowledgeInputChanged
	}
	var knowledge types.Knowledge
	if err := tx.Select("id,tenant_id,knowledge_base_id,channel,parse_status,enable_status").
		Where(
			"id=? AND tenant_id=? AND knowledge_base_id=? AND deleted_at IS NULL",
			source.KnowledgeID,
			source.TenantID,
			source.KnowledgeBaseID,
		).
		Take(&knowledge).
		Error; err != nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if knowledge.Channel != types.ConnectorTypeNextcloud || knowledge.ParseStatus != types.ParseStatusCompleted ||
		knowledge.EnableStatus != "enabled" {
		return ErrKnowledgeInputChanged
	}
	// Select only source control selectors, never the complete metadata JSON or
	// its manual text leaf. A changed current identity is refused before READ.
	field := func(key, alias string) string {
		if tx.Name() == "postgres" {
			return "COALESCE(metadata::jsonb->>'" + key + "','') AS " + alias
		}
		return "COALESCE(json_extract(metadata,'$." + key + "'),'') AS " + alias
	}
	var control struct {
		DataSource      string
		External        string
		Instance        string
		Binding         string
		File            string
		ETag            string
		TargetETag      string
		Resource        string
		ContentRevision string
	}
	fields := []string{
		field("datasource_id", "data_source"),
		field("external_id", "external"),
		field("nextcloud_instance_id", "instance"),
		field("nextcloud_binding_id", "binding"),
		field("nextcloud_file_id", "file"),
		field("nextcloud_etag", "e_tag"),
		field("nextcloud_target_etag", "target_e_tag"),
		field("source_resource_id", "resource"),
		field("nextcloud_content_revision", "content_revision"),
	}
	if err := tx.Table("knowledges").
		Select(strings.Join(fields, ",")).
		Where(
			"id=? AND tenant_id=? AND knowledge_base_id=? AND deleted_at IS NULL",
			source.KnowledgeID,
			source.TenantID,
			source.KnowledgeBaseID,
		).
		Take(&control).
		Error; err != nil {
		return err
	}
	currentRevision, err := types.NormalizeNextcloudContentRevision(control.ContentRevision)
	if err != nil || control.DataSource != source.DataSourceID || control.External != source.ExternalID ||
		control.Instance != source.InstanceID ||
		control.Binding != source.BindingID ||
		control.File != source.FileID ||
		control.ETag != source.ETag ||
		control.TargetETag != source.ETag ||
		control.Resource != source.BindingID ||
		currentRevision != revision {
		return ErrKnowledgeInputChanged
	}
	var observed struct {
		Revision             int64
		DesiredETag          string `gorm:"column:desired_etag"`
		CandidateKnowledgeID string
		State                string
		SourceUpdatedAt      time.Time
	}
	if err := tx.Table("nextcloud_source_revisions").
		Select("revision,desired_etag,candidate_knowledge_id,state,source_updated_at").
		Where(
			"tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND external_id=?",
			source.TenantID,
			source.KnowledgeBaseID,
			source.DataSourceID,
			source.ExternalID,
		).
		Order("revision DESC").
		Take(&observed).
		Error; err != nil {
		return err
	}
	if observed.Revision <= 0 || observed.DesiredETag != source.ETag ||
		observed.CandidateKnowledgeID != source.KnowledgeID ||
		observed.State != "published" ||
		!observed.SourceUpdatedAt.Equal(version.UpdatedAt) {
		return ErrKnowledgeInputChanged
	}
	var material knowledgeInputMaterialReceipt
	if err := tx.Where("row_kind='knowledge' AND row_id=?", source.KnowledgeID).Take(&material).Error; err != nil {
		return types.ErrSourceLineageUnknown
	}
	materialRevision, err := inputRowRevision(tx, "knowledge", source.KnowledgeID)
	if err != nil || material.RowRevision != materialRevision || material.BodyDigest == "" {
		return ErrKnowledgeInputChanged
	}
	var admission knowledgeInputAdmission
	if err := tx.Where(
		("admission_id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=? " +
			"AND build_generation=? AND origin_kind='nextcloud'"),
		material.AdmissionID,
		source.TenantID,
		source.KnowledgeBaseID,
		source.KnowledgeID,
		source.Revision,
	).
		Take(&admission).
		Error; err != nil {
		return types.ErrSourceLineageUnknown
	}
	var admitted types.SourceLineage
	if json.Unmarshal([]byte(admission.SourceLineage), &admitted) != nil || admitted.RequireComplete() != nil {
		return types.ErrSourceLineageUnknown
	}
	exact := false
	for _, parent := range admitted.Sources {
		found := false
		for _, known := range r.lineage.Sources {
			if known == parent {
				found = true
				break
			}
		}
		if !found {
			return types.ErrSourceLineageUnknown
		}
		if parent == source {
			exact = true
		}
	}
	if !exact {
		return types.ErrSourceLineageUnknown
	}
	sourceConfigurationRevision, err := knowledgeConfigurationControlRevision(tx, source.KnowledgeBaseID)
	if err != nil || sourceConfigurationRevision != admission.ConfigRevision {
		return ErrKnowledgeInputChanged
	}

	// The current directory identity may have no WK UserID before first login.
	// Entitlement comes from exact scoped ledger subjects, never a new role.
	query := ("SELECT COUNT(*) FROM nextcloud_source_grants sg JOIN directory_identities " +
		"i ON i.directory_id=sg.directory_id AND i.object_guid=? AND i.status='acti" +
		"ve'\n JOIN directories d ON d.id=i.directory_id AND i.snapshot_version=d.s" +
		"napshot_version\n WHERE sg.operation_id=? AND sg.tenant_id=? AND sg.knowle" +
		"dge_base_id=? AND sg.directory_id=? AND sg.permission_version=? AND sg.ori" +
		"gin='nextcloud'\n AND ((sg.kind='user' AND sg.principal_id=i.id AND sg.obj" +
		"ect_guid=i.object_guid) OR (sg.kind='group' AND EXISTS(SELECT 1 FROM direc" +
		"tory_group_memberships gm JOIN directory_groups g ON g.id=gm.group_id AND " +
		"g.directory_id=d.id AND g.status='active' AND g.snapshot_version=d.snapsho" +
		"t_version WHERE gm.identity_id=i.id AND gm.group_id=sg.principal_id AND gm" +
		".directory_id=d.id AND gm.snapshot_version=d.snapshot_version AND g.object" +
		"_guid=sg.object_guid)))")
	args := []any{r.guid, op.OperationID, source.TenantID, source.KnowledgeBaseID, r.directory, op.PermissionVersion}
	if tx.Migrator().HasTable("nextcloud_source_status_epochs") {
		query += (" AND sg.publication_epoch=COALESCE((SELECT MAX(e.publication_epoch) FROM n" +
			"extcloud_source_status_epochs e JOIN nextcloud_event_connections c ON c.co" +
			"nnection_id=e.connection_id AND c.status='active' WHERE e.pair_operation_i" +
			"d=? AND c.datasource_id=?),?)")
		args = append(args, pair.OperationID, source.DataSourceID, pair.PublicationEpoch)
	} else {
		query += " AND sg.publication_epoch=?"
		args = append(args, pair.PublicationEpoch)
	}
	var count int64
	if err := tx.Raw(query, args...).Scan(&count).Error; err != nil {
		return err
	}
	if count == 0 || op.PermissionVersion == 0 {
		return errors.Join(ErrNextcloudOnboardingDenied, access.ErrNextcloudPublicationDenied)
	}
	var ds types.DataSource
	if err := tx.Select("id,tenant_id,knowledge_base_id,type,status,config").
		Where(
			("id=? AND tenant_id=? AND knowledge_base_id=? AND type=? AND status=? AND d" +
				"eleted_at IS NULL"),
			source.DataSourceID,
			source.TenantID,
			source.KnowledgeBaseID,
			types.ConnectorTypeNextcloud,
			types.DataSourceStatusActive,
		).
		Take(&ds).
		Error; err != nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	base, configSHA, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
	if err != nil || base != pair.BaseURL || configSHA != pair.ConfigSHA || binding != source.BindingID {
		return access.ErrNextcloudPublicationUnavailable
	}
	cfg, err := ds.ParseConfig()
	if err != nil || cfg == nil || cfg.Settings["onboarding_operation_id"] != op.OperationID {
		return access.ErrNextcloudPublicationUnavailable
	}
	fileID, _ := strconv.ParseInt(source.FileID, 10, 64)
	decision, err := nextcloud.AuthorizeCurrentFile(
		types.WithNextcloudContentRevision(ctx, source.ContentRevision),
		cfg,
		source.InstanceID,
		source.BindingID,
		r.directory,
		r.guid,
		fileID,
	)
	if err != nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if !decision.Allow {
		return errors.Join(ErrNextcloudOnboardingDenied, access.ErrNextcloudPublicationDenied)
	}
	if decision.SourceETag != source.ETag || decision.SourceContentRevision != revision {
		return ErrKnowledgeInputChanged
	}
	return nil
}

func (r *NextcloudOnboardingConfigurationRead) checkInTx(ctx context.Context, tx *gorm.DB) error {
	if r.closed.Load() {
		return ErrNextcloudContentLeaseExpired
	}
	if r.operation != nil {
		if err := scopedOnboardingConfigurationPurposeInTx(ctx, tx, r.tenant, r.kb); err != nil {
			return err
		}
	}
	if err := r.checkAuthority(ctx, tx); err != nil {
		return err
	}
	if r.configPlan == nil {
		return ErrOriginalBodyUnavailable
	}
	provenance := NewKnowledgeInputProvenanceStore(tx)
	if err := provenance.RecheckKnowledgeConfigurationBodyReadLeasesInTx(
		ctx,
		tx,
		r.configPlan,
		r.configBodyLeases,
	); err != nil {
		return err
	}
	coverage, _ := ctx.Value(faqReadLeaseCoverageKey{}).(faqReadLeaseCoverage)
	all := append(append([]NextcloudContentLease{}, coverage.Leases...), r.leases...)
	leased := context.WithValue(ctx, faqReadLeaseCoverageKey{}, faqReadLeaseCoverage{all})
	scopes := []types.KnowledgeSearchScope{{TenantID: r.tenant, KBID: r.kb}}
	for _, source := range r.lineage.Sources {
		scopes = append(scopes, types.KnowledgeSearchScope{TenantID: source.TenantID, KBID: source.KnowledgeBaseID})
	}
	if err := validateFAQReadLeaseCoverageInTx(tx.WithContext(leased), scopes, r.lineage); err != nil {
		return err
	}
	now, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return err
	}
	for _, id := range r.configBodyLeases {
		var lease originalBodyLease
		if err := bodyInternal(tx).Where("lease_id=?", id).Take(&lease).Error; err != nil {
			return err
		}
		if lease.OwnerID != r.owner || lease.Kind != "read" || lease.ReleasedAtMS != nil || lease.ExpiresAtMS <= now {
			return ErrOriginalBodyUnavailable
		}
	}
	for kg, captured := range r.sourceHeads {
		var heads []originalBodyFieldHead
		if err := bodyInternal(
			tx,
		).Where("source_table='knowledges' AND row_key=?", kg).
			Order("slot_id").
			Find(&heads).
			Error; err != nil {
			return err
		}
		if !sameOriginalBodyHeads(captured, heads) {
			return ErrOriginalBodyUnavailable
		}
		if err := checkOriginalBodyAvailableForHeads(ctx, tx, "knowledges", kg, captured); err != nil {
			return err
		}
	}
	for bodyID, leaseID := range r.sourceBodyLeases {
		var parents []originalBodyParentRef
		if err := bodyInternal(
			tx,
		).Where("body_id=?", bodyID).
			Order("tenant_id,knowledge_base_id,knowledge_id,admission_id").
			Find(&parents).
			Error; err != nil {
			return err
		}
		if !reflect.DeepEqual(parents, r.sourceParents[bodyID]) {
			return ErrOriginalBodyUnavailable
		}
		var body originalBodyPayload
		if err := bodyInternal(
			tx,
		).Select("id,tenant_id,body_sha256,origin_domain_sha256,body_bytes,storage_kind,state,purge_epoch").
			Where("id=?", bodyID).
			Take(&body).
			Error; err != nil {
			return err
		}
		if !reflect.DeepEqual(body, r.sourceBodies[bodyID]) || body.State != "live" {
			return ErrOriginalBodyUnavailable
		}
		var lease originalBodyLease
		if err := bodyInternal(tx).Where("lease_id=? AND body_id=?", leaseID, bodyID).Take(&lease).Error; err != nil {
			return err
		}
		if lease.Kind != "read" || lease.OwnerID != r.owner || lease.ReleasedAtMS != nil || lease.ExpiresAtMS <= now ||
			lease.Epoch != body.PurgeEpoch {
			return ErrOriginalBodyUnavailable
		}
	}
	if r.operation != nil {
		if err := scopedOnboardingConfigurationPurposeInTx(ctx, tx, r.tenant, r.kb); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// Checkpoint checks point.
func (r *NextcloudOnboardingConfigurationRead) Checkpoint(ctx context.Context) error {
	if r == nil || ctx == nil {
		return types.ErrSourceLineageUnknown
	}
	// Purpose rechecks use the base store before any transaction. They must not
	// recursively enter a knowledge snapshot or try a second pool-1 connection.
	if r.operation != nil {
		ds, err := scopedOnboardingConfigurationPurposeFromContext(ctx, r.tenant, r.kb)
		if err != nil {
			return err
		}
		if ds != r.datasource {
			return types.ErrSourceLineageUnknown
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed.Load() {
		return ErrNextcloudContentLeaseExpired
	}
	if r.heartbeatError != nil {
		return r.heartbeatError
	}
	if r.lifetime != nil && r.lifetime.Err() != nil {
		return context.Cause(r.lifetime)
	}
	if err := r.renewLeases(ctx); err != nil {
		return err
	}
	return r.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return r.checkInTx(ctx, tx) })
}

// Model/connector calls can outlive the lease TTL. The heartbeat renews only
// our committed lease IDs. It grants no source READ; consumer checkpoints still
// run fresh authority, and any renewal failure cancels all owned read contexts.
func (r *NextcloudOnboardingConfigurationRead) renewLeases(ctx context.Context) error {
	for _, lease := range r.leases {
		if _, err := NewNextcloudContentLeaseStore(
			r.store.db,
		).RenewLease(ctx, lease.ID, onboardingConfigurationReadTTL); err != nil {
			return err
		}
	}
	if err := NewKnowledgeInputProvenanceStore(
		r.store.db,
	).RecheckKnowledgeConfigurationBodyReadLeases(
		ctx,
		r.configPlan,
		r.configBodyLeases,
		onboardingConfigurationReadTTL,
	); err != nil {
		return err
	}
	for _, id := range r.sourceBodyLeases {
		if err := NewOriginalBodyRetentionStore(
			r.store.db,
		).RenewLease(ctx, id, onboardingConfigurationReadTTL); err != nil {
			return err
		}
	}
	return nil
}

func (r *NextcloudOnboardingConfigurationRead) startHeartbeat() {
	r.heartbeatStop = make(chan struct{})
	r.heartbeatDone = make(chan struct{})
	go func() {
		defer close(r.heartbeatDone)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-r.heartbeatStop:
				return
			case <-r.lifetime.Done():
				return
			case <-ticker.C:
				r.mu.Lock()
				if r.closed.Load() {
					r.mu.Unlock()
					return
				}
				renewCtx, cancel := context.WithTimeout(r.lifetime, 30*time.Second)
				err := r.renewLeases(renewCtx)
				cancel()
				if err != nil {
					r.heartbeatError = err
					r.cancel(err)
					r.mu.Unlock()
					return
				}
				r.mu.Unlock()
			}
		}
	}()
}

// Close returns the close result.
func (r *NextcloudOnboardingConfigurationRead) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed.Load() {
		r.mu.Unlock()
		return nil
	}
	r.closed.Store(true)
	if r.cancel != nil {
		r.cancel(ErrNextcloudContentLeaseExpired)
	}
	if r.heartbeatStop != nil {
		close(r.heartbeatStop)
	}
	done := r.heartbeatDone
	r.mu.Unlock()
	if done != nil {
		<-done
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var errs []error
	for _, lease := range r.leases {
		errs = append(errs, NewNextcloudContentLeaseStore(r.store.db).ReleaseLease(ctx, lease.ID))
	}
	for _, id := range r.configBodyLeases {
		errs = append(errs, NewOriginalBodyRetentionStore(r.store.db).ReleaseLease(ctx, id))
	}
	for _, id := range r.sourceBodyLeases {
		errs = append(errs, NewOriginalBodyRetentionStore(r.store.db).ReleaseLease(ctx, id))
	}
	return errors.Join(errs...)
}

// ConfigurationProjection is the only configuration hydration allowed to a
// held worker purpose. It returns a new SQL-decoded value, with no display name,
// description or generated profile implicitly selected by GetKnowledgeBase.
func (r *NextcloudOnboardingConfigurationRead) ConfigurationProjection(
	ctx context.Context,
) (*types.KnowledgeBase, error) {
	if err := r.Checkpoint(ctx); err != nil {
		return nil, err
	}
	owned := r.Context(ctx)
	var result *types.KnowledgeBase
	err := r.store.db.WithContext(owned).Transaction(func(tx *gorm.DB) error {
		if err := r.checkInTx(owned, tx); err != nil {
			return err
		}
		var actual types.KnowledgeBase
		if err := tx.Select(knowledgeWorkerConfigurationColumns).
			Where("id=? AND tenant_id=? AND deleted_at IS NULL", r.kb, r.tenant).
			Take(&actual).
			Error; err != nil {
			return err
		}
		digest, err := knowledgeConfigurationOriginDigest(&actual)
		if err != nil {
			return err
		}
		if digest != r.origin.InputDigest {
			return ErrKnowledgeInputChanged
		}
		if err := r.checkInTx(owned, tx); err != nil {
			return err
		}
		result = &actual
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := r.Checkpoint(owned); err != nil {
		return nil, err
	}
	return result, nil
}

// ConfigurationProjectionInTx runs inside the caller's existing transaction.
// It never opens a second connection, renews a lease or invokes the purpose
// checker/snapshot. Captured plans/scope are immutable; owned cancellation and
// actual lease rows reject concurrent Close or a failed heartbeat.
func (r *NextcloudOnboardingConfigurationRead) ConfigurationProjectionInTx(
	ctx context.Context,
	tx *gorm.DB,
) (*types.KnowledgeBase, error) {
	if r == nil || ctx == nil || tx == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if r.lifetime != nil && r.lifetime.Err() != nil {
		return nil, context.Cause(r.lifetime)
	}
	if err := r.checkInTx(ctx, tx); err != nil {
		return nil, err
	}
	var actual types.KnowledgeBase
	if err := tx.WithContext(ctx).
		Select(knowledgeWorkerConfigurationColumns).
		Where("id=? AND tenant_id=? AND deleted_at IS NULL", r.kb, r.tenant).
		Take(&actual).
		Error; err != nil {
		return nil, err
	}
	digest, err := knowledgeConfigurationOriginDigest(&actual)
	if err != nil {
		return nil, err
	}
	if digest != r.origin.InputDigest {
		return nil, ErrKnowledgeInputChanged
	}
	if err := r.checkInTx(ctx, tx); err != nil {
		return nil, err
	}
	if r.lifetime != nil && r.lifetime.Err() != nil {
		return nil, context.Cause(r.lifetime)
	}
	return &actual, nil
}
