package repository

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const parentCopyBodyField = "parent_materials.*.Messages"

func parentCopyTable(table string) bool {
	return table == "message_generated_field_admissions" || table == "message_fork_parent_origins"
}

func parentCopySnapshots(raw string) ([]*types.MessageMaterialSnapshot, error) {
	var out []*types.MessageMaterialSnapshot
	if strings.HasPrefix(strings.TrimSpace(raw), "{") {
		var one types.MessageMaterialSnapshot
		if e := json.Unmarshal([]byte(raw), &one); e != nil {
			return nil, e
		}
		out = append(out, &one)
	} else if e := json.Unmarshal([]byte(raw), &out); e != nil {
		return nil, e
	}
	return out, nil
}

// Parent-copy evidence comes only from immutable original admissions and the
// bytes actually stored in the copy. Current message/KB pointers are never an
// inventory authority. Metadata-only generated parents carry the original
// proof identities; their generated dependencies are traversed separately.
type parentCopyEvidence struct {
	tx          *gorm.DB
	scopes      []originalBodyLeaseScope
	scopeKeys   map[originalBodyLeaseScope]bool
	sources     map[types.SourceIdentity]knowledgeInputAdmission
	visiting    map[string]bool
	done        map[string]bool
	count       int
	parsedBytes int
}

func (v *parentCopyEvidence) addScope(scope originalBodyLeaseScope) error {
	scope.BodyID = ""
	if !v.scopeKeys[scope] {
		if len(v.scopeKeys) >= 4096 {
			return ErrOriginalBodyRetained
		}
		v.scopeKeys[scope] = true
		v.scopes = append(v.scopes, scope)
	}
	return nil
}

func (v *parentCopyEvidence) snapshots(raw string) ([]*types.MessageMaterialSnapshot, error) {
	v.parsedBytes += len(raw)
	if len(raw) > 8<<20 || v.parsedBytes > 32<<20 {
		return nil, ErrOriginalBodyRetained
	}
	return parentCopySnapshots(raw)
}

func (v *parentCopyEvidence) addLineage(lineage *types.SourceLineage) error {
	if lineage.RequireComplete() != nil {
		return ErrOriginalBodyRetained
	}
	for _, source := range lineage.Sources {
		if _, found := v.sources[source]; !found {
			var original knowledgeInputAdmission
			var candidates []knowledgeInputAdmission
			if v.tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=? AND build_generation=? AND origin_kind='nextcloud'", source.TenantID, source.KnowledgeBaseID, source.KnowledgeID, source.Revision).
				Limit(2).
				Find(&candidates).
				Error != nil ||
				len(candidates) != 1 {
				return ErrOriginalBodyRetained
			}
			original = candidates[0]
			var captured types.SourceLineage
			if json.Unmarshal([]byte(original.SourceLineage), &captured) != nil || captured.RequireComplete() != nil {
				return ErrOriginalBodyRetained
			}
			matched := false
			for _, dependency := range captured.Sources {
				if dependency == source {
					matched = true
				}
				declared := false
				for _, current := range lineage.Sources {
					if current == dependency {
						declared = true
					}
				}
				if !declared {
					return ErrOriginalBodyRetained
				}
			}
			if !matched {
				return ErrOriginalBodyRetained
			}
			v.sources[source] = original
		}
		if err := v.addScope(
			originalBodyLeaseScope{TenantID: source.TenantID, KnowledgeBaseID: source.KnowledgeBaseID},
		); err != nil {
			return err
		}
		if len(v.sources) > types.SourceLineageMaxSources {
			return ErrOriginalBodyRetained
		}
	}
	return nil
}

func (v *parentCopyEvidence) proof(
	snapshot *types.MessageMaterialSnapshot,
	proof *types.MessageMaterialProof,
	message *types.Message,
	depth int,
) error {
	v.count++
	if depth > 32 || v.count > 4096 || proof == nil || proof.AdmissionID == "" || proof.GenerationID == "" {
		return ErrOriginalBodyRetained
	}
	var fact messageMaterialAdmission
	if v.tx.Where("id=?", proof.AdmissionID).Take(&fact).Error != nil {
		return ErrOriginalBodyRetained
	}
	var origin messageGenerationOrigin
	if v.tx.Select(messageGenerationMetadataFields).Where("id=?", proof.GenerationID).Take(&origin).Error != nil {
		return ErrOriginalBodyRetained
	}
	if fact.GenerationID != origin.ID || fact.MessageID != proof.MessageID ||
		fact.SessionID != snapshot.Request.SessionID ||
		proof.SessionID != fact.SessionID ||
		fact.TenantID != snapshot.Request.TenantID ||
		proof.TenantID != fact.TenantID ||
		fact.OwnerID != snapshot.SessionOwnerID ||
		fact.OwnerRevision != snapshot.SessionOwnerRevision ||
		proof.OwnerID != fact.OwnerID ||
		proof.OwnerRevision != fact.OwnerRevision ||
		fact.Revision != proof.Revision ||
		fact.BodyDigest != proof.BodyDigest ||
		origin.MessageID != fact.MessageID ||
		origin.TenantID != fact.TenantID ||
		origin.SessionID != fact.SessionID ||
		origin.OwnerID != fact.OwnerID ||
		origin.OwnerRevision != fact.OwnerRevision {
		return ErrOriginalBodyRetained
	}
	var lineage types.SourceLineage
	var execution types.MessageExecutionContext
	if json.Unmarshal([]byte(fact.SourceLineage), &lineage) != nil || lineage.RequireComplete() != nil ||
		json.Unmarshal([]byte(fact.ExecutionContext), &execution) != nil ||
		!faqInputEqual(execution, proof.Execution) ||
		fact.AgentID != proof.AgentID ||
		fact.AgentTenantID != proof.AgentTenantID {
		return ErrOriginalBodyRetained
	}
	if message != nil {
		if message.ID != fact.MessageID || message.SessionID != fact.SessionID || origin.Role != message.Role ||
			origin.RequestID != message.RequestID {
			return ErrOriginalBodyRetained
		}
		restored := *message
		restored.SourceLineage, restored.ExecutionContext, restored.AgentTenantID = &lineage, execution, fact.AgentTenantID
		digest, err := messageMaterialDigest(&restored)
		if err != nil || digest != fact.BodyDigest {
			return ErrOriginalBodyRetained
		}
	}
	if err := v.addLineage(&lineage); err != nil {
		return err
	}
	var originalScopes []originalBodyLeaseScope
	if err := v.tx.Table("original_body_lease_scopes s").
		Select("s.*").
		Joins("JOIN original_body_field_heads h ON h.body_id=s.body_id").
		Where("h.source_table='message_generation_origins' AND h.row_key=?", origin.ID).
		Find(&originalScopes).
		Error; err != nil {
		return err
	}
	if bodyLeaseScopesComplete(originalScopes, fact.TenantID) {
		for _, scope := range originalScopes {
			if err := v.addScope(scope); err != nil {
				return err
			}
		}
	} else {
		// A source identity itself fixes the original tenant/KB/document tuple.
		// An ordinary old execution selector with no such immutable resolution
		// remains unknown, even if its current KB pointer happens to be readable.
		kbIDs := append(append([]string{}, execution.KnowledgeBaseIDs...), execution.AgentKnowledgeBaseIDs...)
		for _, tag := range execution.TagScopes {
			kbIDs = append(kbIDs, tag.KnowledgeBaseID)
		}
		for _, kb := range kbIDs {
			found := false
			for _, source := range lineage.Sources {
				if source.KnowledgeBaseID == kb {
					found = true
				}
			}
			if !found {
				return ErrOriginalBodyRetained
			}
		}
		for _, id := range execution.KnowledgeIDs {
			found := false
			for _, source := range lineage.Sources {
				if source.KnowledgeID == id {
					found = true
				}
			}
			if !found {
				return ErrOriginalBodyRetained
			}
		}
	}
	proofLineages := []*types.SourceLineage{&lineage}
	covered := map[string]bool{}
	for _, field := range proof.GeneratedFields {
		if field == nil || covered[field.Field] {
			return ErrOriginalBodyRetained
		}
		if err := v.generated(snapshot, &fact, field, message, depth+1); err != nil {
			return err
		}
		covered[field.Field] = true
		proofLineages = append(proofLineages, field.SourceLineage)
	}
	expectedLineage, err := types.UnionSourceLineage(proofLineages...)
	if err != nil || !faqInputEqual(expectedLineage, proof.SourceLineage) {
		return ErrOriginalBodyRetained
	}
	if message != nil && message.Role == "user" &&
		(len(message.Images) > 0 && !covered["images"] || len(message.Attachments) > 0 && !covered["attachments"] || message.RenderedContent != "" && !covered["rendered_content"] || message.ContextCheckpoint != nil && !covered["context_checkpoint"] || len(message.AgentSteps) > 0 || len(message.KnowledgeReferences) > 0 || len(message.UsedMemories) > 0) {
		return ErrOriginalBodyRetained
	}
	return nil
}

func (v *parentCopyEvidence) generated(
	snapshot *types.MessageMaterialSnapshot,
	fact *messageMaterialAdmission,
	proof *types.MessageGeneratedFieldProof,
	message *types.Message,
	depth int,
) error {
	if depth > 32 || proof.AdmissionID == "" || v.visiting[proof.AdmissionID] {
		return ErrOriginalBodyRetained
	}
	var admission messageGeneratedFieldAdmission
	if v.tx.Where("id=?", proof.AdmissionID).Take(&admission).Error != nil {
		return ErrOriginalBodyRetained
	}
	if admission.MessageID != fact.MessageID || admission.GenerationID != fact.GenerationID ||
		admission.SessionID != fact.SessionID ||
		admission.TenantID != fact.TenantID ||
		admission.Field != proof.Field ||
		admission.FieldRevision != proof.Revision ||
		admission.BodyDigest != proof.BodyDigest {
		return ErrOriginalBodyRetained
	}
	var lineage types.SourceLineage
	if json.Unmarshal([]byte(admission.SourceLineage), &lineage) != nil || lineage.RequireComplete() != nil ||
		!faqInputEqual(&lineage, proof.SourceLineage) {
		return ErrOriginalBodyRetained
	}
	if message != nil {
		var value any
		switch proof.Field {
		case "images":
			value = message.Images
		case "attachments":
			value = message.Attachments
		case "rendered_content":
			value = message.RenderedContent
		case "context_checkpoint":
			value = message.ContextCheckpoint
		default:
			return ErrOriginalBodyRetained
		}
		digest, err := KnowledgeInputDigest(value)
		if err != nil {
			return ErrOriginalBodyRetained
		}
		if digest != admission.BodyDigest {
			// The real old Message serializer used omitempty for these arrays.
			// A missing JSON leaf can represent its original typed empty slice.
			// Try only that empty shape against the immutable original digest;
			// omitted nonempty fields still fail, and no original seal changes.
			if proof.Field == "images" && len(message.Images) == 0 {
				digest, err = KnowledgeInputDigest(types.MessageImages{})
			}
			if proof.Field == "attachments" && len(message.Attachments) == 0 {
				digest, err = KnowledgeInputDigest(types.MessageAttachments{})
			}
			if err != nil || digest != admission.BodyDigest {
				return ErrOriginalBodyRetained
			}
		}
	}
	if err := v.addLineage(&lineage); err != nil {
		return err
	}
	if v.done[admission.ID] {
		return nil
	}
	v.visiting[admission.ID] = true
	defer delete(v.visiting, admission.ID)
	parents, err := v.snapshots(admission.ParentMaterials)
	if err != nil {
		return ErrOriginalBodyRetained
	}
	var previous messageMaterialAdmission
	if v.tx.Where("id=?", admission.PreviousAdmissionID).Take(&previous).Error != nil ||
		previous.GenerationID != admission.GenerationID ||
		previous.MessageID != admission.MessageID ||
		previous.SessionID != admission.SessionID ||
		previous.TenantID != admission.TenantID ||
		previous.Revision != admission.PreviousRevision ||
		admission.PreviousMembership == 0 ||
		admission.ResultRevision < admission.PreviousRevision {
		return ErrOriginalBodyRetained
	}
	if len(parents) == 0 {
		// Only actual authored initial images/attachments legitimately have no
		// consumed parents. Their actor is the original generation actor, not a
		// later generated callback's actor. Removing a later callback's parents
		// must never turn a derived field into an original empty-scope field.
		var original messageGenerationOrigin
		if v.tx.Select(messageGenerationMetadataFields).
			Where("id=?", admission.GenerationID).
			Take(&original).
			Error != nil ||
			original.Role != "user" ||
			admission.Field != "images" && admission.Field != "attachments" ||
			admission.Actor != original.Actor ||
			len(lineage.Sources) != 0 ||
			admission.ResultRevision != previous.Revision {
			return ErrOriginalBodyRetained
		}
	}
	for _, parent := range parents {
		if err := v.snapshot(parent, false, depth+1); err != nil {
			return err
		}
		for _, original := range parent.Proofs {
			if original.MessageID == admission.MessageID &&
				(original.AdmissionID != admission.PreviousAdmissionID || original.Revision != admission.PreviousRevision || parent.MembershipRevision != admission.PreviousMembership) {
				return ErrOriginalBodyRetained
			}
			// Every parent source must have been accounted by this immutable
			// generated admission, never introduced by a current lookup.
			dependencies := []*types.SourceLineage{original.SourceLineage}
			for _, field := range original.GeneratedFields {
				if field == nil {
					return ErrOriginalBodyRetained
				}
				dependencies = append(dependencies, field.SourceLineage)
			}
			for _, dependency := range dependencies {
				if dependency.RequireComplete() != nil {
					return ErrOriginalBodyRetained
				}
				for _, source := range dependency.Sources {
					found := false
					for _, declared := range lineage.Sources {
						if declared == source {
							found = true
						}
					}
					if !found {
						return ErrOriginalBodyRetained
					}
				}
			}
		}
	}
	v.done[admission.ID] = true
	return nil
}

func (v *parentCopyEvidence) snapshot(snapshot *types.MessageMaterialSnapshot, requireBodies bool, depth int) error {
	if depth > 32 || snapshot == nil || snapshot.Request.TenantID == 0 || snapshot.Request.SessionID == "" ||
		snapshot.SessionOwnerID == "" ||
		snapshot.SessionOwnerRevision == 0 ||
		snapshot.MembershipRevision == 0 ||
		snapshot.MetadataDigest == "" {
		return ErrOriginalBodyRetained
	}
	byID := map[string]*types.Message{}
	for _, message := range snapshot.Messages {
		if message == nil || byID[message.ID] != nil {
			return ErrOriginalBodyRetained
		}
		byID[message.ID] = message
	}
	if (requireBodies || len(byID) > 0) && len(byID) != len(snapshot.Proofs) {
		return ErrOriginalBodyRetained
	}
	seen := map[string]bool{}
	for _, proof := range snapshot.Proofs {
		if proof == nil || seen[proof.MessageID] {
			return ErrOriginalBodyRetained
		}
		seen[proof.MessageID] = true
		if (requireBodies || len(byID) > 0) && byID[proof.MessageID] == nil {
			return ErrOriginalBodyRetained
		}
		if err := v.proof(snapshot, proof, byID[proof.MessageID], depth+1); err != nil {
			return err
		}
	}
	if !snapshot.Request.AllSession {
		if len(snapshot.Request.MessageIDs) != len(seen) {
			return ErrOriginalBodyRetained
		}
		for _, id := range snapshot.Request.MessageIDs {
			if !seen[id] {
				return ErrOriginalBodyRetained
			}
		}
	}
	return nil
}

func validateParentCopyEvidence(
	tx *gorm.DB,
	d originalBodyFieldDescriptor,
	key string,
	scope originalBodyScope,
) (*parentCopyEvidence, error) {
	row, err := bodySourceRow(tx, d, key)
	if err != nil {
		return nil, err
	}
	raw := bodyText(row["parent_materials"])
	if len(raw) > 8<<20 {
		return nil, ErrOriginalBodyRetained
	}
	v := &parentCopyEvidence{
		tx:        bodyInternal(tx),
		scopeKeys: map[originalBodyLeaseScope]bool{},
		sources:   map[types.SourceIdentity]knowledgeInputAdmission{},
		visiting:  map[string]bool{},
		done:      map[string]bool{},
	}
	if err := v.addScope(originalBodyLeaseScope{TenantID: scope.Tenant}); err != nil {
		return nil, err
	}
	snapshots, err := v.snapshots(raw)
	if err != nil {
		return nil, ErrOriginalBodyRetained
	}
	for _, snapshot := range snapshots {
		if snapshot != nil && len(snapshot.Messages) == 0 {
			continue
		}
		if err := v.snapshot(snapshot, true, 0); err != nil {
			return nil, err
		}
	}
	return v, nil
}

func parentCopyScopesMatch(bodyID string, actual, expected []originalBodyLeaseScope) bool {
	if len(actual) != len(expected) {
		return false
	}
	values := map[originalBodyLeaseScope]bool{}
	for _, scope := range actual {
		if scope.BodyID != bodyID {
			return false
		}
		scope.BodyID = ""
		values[scope] = true
	}
	for _, scope := range expected {
		scope.BodyID = ""
		if !values[scope] {
			return false
		}
	}
	return len(values) == len(expected)
}

func parentCopyScopeEvidence(
	tx *gorm.DB,
	head originalBodyFieldHead,
	stored []originalBodyLeaseScope,
) (*parentCopyEvidence, error) {
	descriptor, ok := bodyDescriptor(head.SourceTable)
	if !ok {
		return nil, ErrOriginalBodyRetained
	}
	evidence, err := validateParentCopyEvidence(tx, descriptor, head.RowKey, originalBodyScope{Tenant: head.TenantID})
	if err != nil {
		return nil, err
	}
	if !parentCopyScopesMatch(head.BodyID, stored, evidence.scopes) {
		return nil, ErrOriginalBodyRetained
	}
	return evidence, nil
}

func (s *OriginalBodyRetentionStore) recordParentCopyScopes(
	tx *gorm.DB,
	id string,
	d originalBodyFieldDescriptor,
	key string,
	scope originalBodyScope,
) error {
	evidence, e := validateParentCopyEvidence(tx, d, key, scope)
	if errors.Is(e, ErrOriginalBodyRetained) {
		return nil
	}
	if e != nil {
		return e
	}
	var existing []originalBodyLeaseScope
	if e := bodyInternal(tx).Where("body_id=?", id).Find(&existing).Error; e != nil {
		return e
	}
	if len(existing) > 0 {
		// A present historical marker is an immutable assertion, not an empty
		// slot for a later scan to fill. Mismatch stays unknown and is never
		// upgraded by appending the missing scopes from fresh evidence.
		if !parentCopyScopesMatch(id, existing, evidence.scopes) {
			return nil
		}
		return nil
	}
	for _, original := range evidence.sources {
		// The journal's immutable parent-ref digest now names the actual original
		// knowledge admission/build generation, rather than just its current KB.
		ref := originalBodyParentRef{
			BodyID:          id,
			TenantID:        original.TenantID,
			KnowledgeBaseID: original.KnowledgeBaseID,
			KnowledgeID:     original.KnowledgeID,
			AdmissionID:     original.AdmissionID,
		}
		if e := bodyInternal(tx).Clauses(clause.OnConflict{DoNothing: true}).Create(&ref).Error; e != nil {
			return e
		}
	}
	for i := range evidence.scopes {
		evidence.scopes[i].BodyID = id
	}
	return bodyInternal(tx).Clauses(clause.OnConflict{DoNothing: true}).Create(&evidence.scopes).Error
}

func parentCopyReplace(raw, old, replacement string) (string, error) {
	actual, e := legacyParentMessageBodies(raw)
	if e != nil || actual != old {
		return "", ErrOriginalBodyUnavailable
	}
	if replacement != "" {
		return "", ErrOriginalBodyUnavailable
	}
	var nodes []map[string]json.RawMessage
	object := strings.HasPrefix(strings.TrimSpace(raw), "{")
	if object {
		var one map[string]json.RawMessage
		if e := json.Unmarshal([]byte(raw), &one); e != nil {
			return "", e
		}
		nodes = append(nodes, one)
	} else if e := json.Unmarshal([]byte(raw), &nodes); e != nil {
		return "", e
	}
	for _, node := range nodes {
		if _, exists := node["Messages"]; exists {
			node["Messages"] = json.RawMessage("null")
		}
	}
	var out []byte
	if object {
		out, e = json.Marshal(nodes[0])
	} else {
		out, e = json.Marshal(nodes)
	}
	return string(out), e
}

func parentCopyMetadataSelect(tx *gorm.DB, column string, array bool) string {
	if array {
		if tx.Dialector.Name() == "postgres" {
			return "(SELECT COALESCE(jsonb_agg(v - 'Messages'),'[]'::jsonb)::text FROM jsonb_array_elements(CAST(" + column + " AS jsonb)) v)"
		}
		return "(SELECT COALESCE(json_group_array(json_remove(value,'$.Messages')),'[]') FROM json_each(" + column + "))"
	}
	if tx.Dialector.Name() == "postgres" {
		return "(CAST(" + column + " AS jsonb)-'Messages')::text"
	}
	return "json_remove(" + column + ",'$.Messages')"
}

// Source closure is a durable local version/fence/withdrawal fact, never a
// transient HTTP denial or a later readable KB pointer. Active/ambiguous
// source generations keep old derived copies; exact original scope locks and
// current read/build leases remain independently required by the purge gate.
func parentCopySourceDeadline(tx *gorm.DB, source types.SourceIdentity) (int64, error) {
	now, err := nextcloudLeaseNowMS(bodyInternal(tx))
	if err != nil {
		return 0, err
	}
	originalScope := NextcloudContentScope{
		TenantID:        source.TenantID,
		KnowledgeBaseID: source.KnowledgeBaseID,
		KnowledgeID:     source.KnowledgeID,
		DataSourceID:    source.DataSourceID,
		ExternalID:      source.ExternalID,
	}
	if nextcloudRequireContentLeaseCoverage(bodyInternal(tx), originalScope, now) != nil {
		return 0, ErrOriginalBodyRetained
	}
	if err := nextcloudRequireNoActiveContentLeases(bodyInternal(tx), originalScope, now); err != nil {
		return 0, ErrOriginalBodyRetained
	}
	var version nextcloudSourceVersion
	if err := nextcloudVersionQuery(
		bodyInternal(tx).Clauses(clause.Locking{Strength: "UPDATE"}),
		source.TenantID,
		source.KnowledgeBaseID,
		source.DataSourceID,
		source.ExternalID,
	).Take(&version).
		Error; err != nil {
		return 0, ErrOriginalBodyRetained
	}
	withdrawn := false
	if tx.Migrator().HasTable(&NextcloudIndexedWithdrawal{}) {
		var count int64
		if err := bodyInternal(
			tx,
		).Model(&NextcloudIndexedWithdrawal{}).
			Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND state=?", source.TenantID, source.KnowledgeBaseID, source.DataSourceID, "withdrawn_inventory_incomplete").
			Count(&count).
			Error; err != nil {
			return 0, err
		}
		withdrawn = count > 0
	}
	if !withdrawn && version.State != "tombstone" && version.CandidateKnowledgeID == source.KnowledgeID {
		return 0, ErrOriginalBodyRetained
	}
	if !withdrawn && version.State != "tombstone" {
		var current types.DataSource
		if bodyInternal(
			tx,
		).Where("id=? AND tenant_id=? AND knowledge_base_id=?", source.DataSourceID, source.TenantID, source.KnowledgeBaseID).
			Take(&current).
			Error != nil ||
			current.Status != types.DataSourceStatusActive {
			return 0, ErrOriginalBodyRetained
		}
	}
	var fence nextcloudContentFence
	if err := bodyInternal(
		tx,
	).Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", source.TenantID, source.KnowledgeBaseID, source.KnowledgeID).
		Take(&fence).
		Error; err != nil {
		return 0, ErrOriginalBodyRetained
	}
	if (fence.State != "retired" && fence.State != "deleted") || fence.RetiredAtMS == nil ||
		fence.DataSourceID != source.DataSourceID ||
		fence.ExternalID != source.ExternalID ||
		fence.Epoch <= int64(source.Revision) {
		return 0, ErrOriginalBodyRetained
	}
	deadline := *fence.RetiredAtMS
	if !withdrawn && version.State != "tombstone" {
		deadline += 3600000
	}
	return deadline, nil
}

func (s *OriginalBodyRetentionStore) parentCopyCurrent(
	tx *gorm.DB,
	head originalBodyFieldHead,
	row map[string]any,
	now int64,
) (bool, int64, error) {
	// Every recorded body head is checked by bodyEligibility. A missing
	// current receipt alone never closes a historical consumer or shared owner.
	var scopes []originalBodyLeaseScope
	if e := bodyInternal(tx).Where("body_id=?", head.BodyID).Find(&scopes).Error; e != nil {
		return true, 0, e
	}
	if !bodyLeaseScopesComplete(scopes, head.TenantID) {
		return true, 0, ErrOriginalBodyRetained
	}
	evidence, err := parentCopyScopeEvidence(tx, head, scopes)
	if err != nil {
		return true, 0, err
	}
	dependencyDeadline := int64(0)
	for source := range evidence.sources {
		deadline, err := parentCopySourceDeadline(tx, source)
		if err != nil {
			return true, 0, err
		}
		if deadline > dependencyDeadline {
			dependencyDeadline = deadline
		}
	}
	messageID := bodyText(row["message_id"])
	if head.SourceTable == "message_fork_parent_origins" {
		var origin messageGenerationOrigin
		if e := bodyInternal(
			tx,
		).Select(messageGenerationMetadataFields).
			Where("id=?", head.RowKey).
			Take(&origin).
			Error; e != nil {
			return true, 0, e
		}
		messageID = origin.MessageID
	}
	var message struct {
		ID, SessionID string
		DeletedAt     gorm.DeletedAt
	}
	e := bodyInternal(
		tx,
	).Unscoped().
		Table("messages").
		Select("id,session_id,deleted_at").
		Where("id=?", messageID).
		Take(&message).
		Error
	if e != nil {
		return true, 0, ErrOriginalBodyRetained
	}
	if message.DeletedAt.Valid {
		return false, max(message.DeletedAt.Time.UnixMilli()+3600000, dependencyDeadline), nil
	}
	var session struct{ DeletedAt gorm.DeletedAt }
	if e := bodyInternal(
		tx,
	).Unscoped().
		Table("sessions").
		Select("deleted_at").
		Where("id=?", message.SessionID).
		Take(&session).
		Error; e != nil {
		return true, 0, ErrOriginalBodyRetained
	}
	if session.DeletedAt.Valid {
		return false, max(session.DeletedAt.Time.UnixMilli()+3600000, dependencyDeadline), nil
	}
	// A missing incoming current field receipt is not a durable termination
	// receipt for every original consumer. Until its message/session actually
	// closes, retain even an orphan/obsolete admission rather than assuming an
	// old task or saved control can no longer consume or restore this body.

	return true, 0, nil
}
