package repository

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type messageGeneratedFieldAdmission struct {
	FieldRevision                                                  uint64
	ID                                                             string `gorm:"primaryKey"`
	MessageID                                                      string
	GenerationID                                                   string
	SessionID                                                      string
	Field                                                          string
	BodyDigest                                                     string
	SourceLineage                                                  string
	ParentMaterials                                                string
	Actor                                                          string
	PreviousAdmissionID                                            string
	TenantID, PreviousRevision, PreviousMembership, ResultRevision uint64
}

func (messageGeneratedFieldAdmission) TableName() string { return "message_generated_field_admissions" }

type messageGeneratedFieldReceipt struct {
	MessageID, Field string `gorm:"primaryKey"`
	AdmissionID      string
}

func (messageGeneratedFieldReceipt) TableName() string { return "message_generated_field_receipts" }

type (
	messageGeneratedFieldKey      struct{}
	messageGeneratedFieldBoundary struct {
		Store                               *KnowledgeInputProvenanceStore
		MessageID, SessionID, Field, Digest string
		Parents                             []*types.MessageMaterialSnapshot
		Sources                             *types.SourceLineage
	}
)

// WithActualMessageGeneratedField binds actual message generated field.
// Only the actual model/checkpoint callback binds its exact value and original
// consumed material. It cannot be decoded from a request or current row.
func (s *KnowledgeInputProvenanceStore) WithActualMessageGeneratedField(
	ctx context.Context,
	session, message, field string,
	value any,
	parents []*types.MessageMaterialSnapshot,
	lineage *types.SourceLineage,
) (context.Context, error) {
	if session == "" || message == "" ||
		field != "context_checkpoint" && field != "rendered_content" && field != "images" {
		return ctx, types.ErrSourceLineageUnknown
	}
	if lineage.RequireComplete() != nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	if len(parents) == 0 {
		return ctx, types.ErrSourceLineageUnknown
	}
	originals := []*types.SourceLineage{lineage}
	for _, parent := range parents {
		if parent == nil {
			return ctx, types.ErrSourceLineageUnknown
		}
		if err := s.RecheckMessageMaterialInputs(ctx, parent); err != nil {
			return ctx, err
		}
		for _, proof := range parent.Proofs {
			originals = append(originals, proof.SourceLineage)
		}
	}
	sources, err := types.UnionSourceLineage(originals...)
	if err != nil {
		return ctx, err
	}
	digest, err := KnowledgeInputDigest(value)
	if err != nil {
		return ctx, err
	}
	encoded, err := json.Marshal(parents)
	if err != nil {
		return ctx, err
	}
	var copied []*types.MessageMaterialSnapshot
	if err = json.Unmarshal(encoded, &copied); err != nil {
		return ctx, err
	}
	return context.WithValue(
		ctx,
		messageGeneratedFieldKey{},
		messageGeneratedFieldBoundary{s, message, session, field, digest, copied, sources},
	), nil
}

func (r *messageRepository) writeGeneratedField(
	ctx context.Context,
	sessionID, messageID, field string,
	value any,
) error {
	if r.materialStore == nil {
		return types.ErrSourceLineageUnknown
	}
	store := r.materialStore
	boundary, ok := ctx.Value(messageGeneratedFieldKey{}).(messageGeneratedFieldBoundary)
	if !ok || boundary.Store != store || boundary.SessionID != sessionID || boundary.MessageID != messageID ||
		boundary.Field != field {
		return types.ErrSourceLineageUnknown
	}
	digest, err := KnowledgeInputDigest(value)
	if err != nil {
		return err
	}
	if digest != boundary.Digest {
		return ErrKnowledgeInputChanged
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, parent := range boundary.Parents {
			actual, err := store.captureMessageMaterialInTx(tx, parent.Request)
			if err != nil {
				return err
			}
			a, b := *actual, *parent
			a.Messages = nil
			b.Messages = nil
			if !faqInputEqual(&a, &b) {
				return ErrKnowledgeInputChanged
			}
		}
		fresh, session, ownerRev, err := store.currentMessageOwnerInTx(tx, sessionID)
		if err != nil {
			return err
		}
		var origin messageGenerationOrigin
		if err := tx.Select(messageGenerationMetadataFields).
			Where("message_id=? AND session_id=? AND tenant_id=?", messageID, sessionID, session.TenantID).
			Take(&origin).
			Error; err != nil {
			return err
		}
		if origin.OwnerID != session.UserID || origin.OwnerRevision != ownerRev {
			return ErrKnowledgeInputChanged
		}
		if err := store.checkMessageOriginalBodyAvailability(tx, origin.ID); err != nil {
			return err
		}
		var previous messageMaterialAdmission
		if err := tx.Table("message_material_admissions a").
			Select("a.*").
			Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").
			Where("a.message_id=?", messageID).
			Take(&previous).
			Error; err != nil {
			return err
		}
		previousMembership, err := messageVersion(tx, "message_session_membership_versions", "session_id", sessionID)
		if err != nil {
			return err
		}
		var original types.Message
		if err := tx.Where("id=? AND session_id=?", messageID, sessionID).Take(&original).Error; err != nil {
			return err
		}
		if original.Role == "assistant" {
			if _, err := store.verifyMessageMaterialInTx(tx, &original); err != nil {
				return err
			}
		}
		// Exact actual field write, preserving every other original field. A raw
		// partial update cannot invoke this path and cannot re-seal any material.
		update := tx.Model(&types.Message{}).Where("id=? AND session_id=?", messageID, sessionID).Update(field, value)
		if update.Error != nil {
			return update.Error
		}
		if update.RowsAffected != 1 {
			return ErrKnowledgeInputChanged
		}
		sources := boundary.Sources
		if original.SourceLineage.RequireComplete() == nil {
			sources, err = types.UnionSourceLineage(original.SourceLineage, sources)
			if err != nil {
				return err
			}
		}
		sourceJSON, err := json.Marshal(sources)
		if err != nil {
			return err
		}
		if original.Role == "assistant" {
			if err := tx.Model(&types.Message{}).
				Where("id=?", messageID).
				Update("source_lineage", types.JSON(sourceJSON)).
				Error; err != nil {
				return err
			}
		}
		metadataParents := make([]*types.MessageMaterialSnapshot, 0, len(boundary.Parents))
		for _, parent := range boundary.Parents {
			metadata := *parent
			metadata.Messages = nil
			metadataParents = append(metadataParents, &metadata)
		}
		parentJSON, err := json.Marshal(metadataParents)
		if err != nil {
			return err
		}
		actor, err := captureKnowledgeActorSelector(fresh, "")
		if err != nil {
			return err
		}
		actorJSON, err := json.Marshal(actor)
		if err != nil {
			return err
		}
		resultRevision, err := messageVersion(tx, "message_material_versions", "message_id", messageID)
		if err != nil {
			return err
		}
		fieldRevision, err := messageGeneratedFieldVersion(tx, messageID, field)
		if err != nil {
			return err
		}
		admission := messageGeneratedFieldAdmission{
			FieldRevision:       fieldRevision,
			PreviousAdmissionID: previous.ID,
			PreviousRevision:    previous.Revision,
			PreviousMembership:  previousMembership,
			ResultRevision:      resultRevision,
			ID:                  uuid.NewString(),
			MessageID:           messageID,
			GenerationID:        origin.ID,
			SessionID:           sessionID,
			TenantID:            session.TenantID,
			Field:               field,
			BodyDigest:          digest,
			SourceLineage:       string(sourceJSON),
			ParentMaterials:     string(parentJSON),
			Actor:               string(actorJSON),
		}
		if err := tx.Create(&admission).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "message_id"}, {
			Name: "field",
		}}, DoUpdates: clause.AssignmentColumns([]string{"admission_id"})}).
				Create(&messageGeneratedFieldReceipt{messageID, field, admission.ID}).
				Error; err != nil {
			return err
		}
		var actual types.Message
		if err := tx.Where("id=? AND session_id=?", messageID, sessionID).Take(&actual).Error; err != nil {
			return err
		}
		if original.Role == "assistant" {
			if err := store.sealMessageMaterialInTx(tx, &origin, &actual); err != nil {
				return err
			}
		}
		return nil
	})
}

// UpgradeMessageGeneratedFieldSchema returns the upgrade message generated field schema result.
// A real forward upgrade for installations that already completed old 151/70.
// The bundled fresh migration remains authoritative for clean installations.
func UpgradeMessageGeneratedFieldSchema(db *gorm.DB) error {
	if db.Migrator().HasTable("message_generated_field_admissions") &&
		db.Migrator().HasTable("message_generated_field_versions") &&
		db.Migrator().HasTable("message_generated_field_receipts") &&
		db.Migrator().HasTable("message_fork_parent_origins") &&
		db.Migrator().HasColumn(&messageGeneratedFieldAdmission{}, "field_revision") {
		return nil
	}
	if db.Migrator().HasTable("message_generated_field_admissions") {
		return types.ErrSourceLineageUnknown
	} // incomplete private upgrade needs operator reconciliation, never current-body admission
	ddl := messageGeneratedFieldsSQLiteUpgrade
	if db.Name() == "postgres" {
		ddl = messageGeneratedFieldsPostgresUpgrade
	} else if db.Name() != "sqlite" {
		return types.ErrSourceLineageUnknown
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(ddl).Error; err != nil {
			return err
		}
		return tx.Exec(
			("INSERT INTO message_generated_field_versions(message_id,field,revision) SE" +
				"LECT id,'images',1 FROM messages UNION ALL SELECT id,'attachments',1 FROM " +
				"messages UNION ALL SELECT id,'rendered_content',1 FROM messages UNION ALL " +
				"SELECT id,'context_checkpoint',1 FROM messages"),
		).Error
	})
}

const messageGeneratedFieldsSQLiteUpgrade = ("-- Actual generated fields have " +
	"independent immutable parents and material" +
	".\nCREATE TABLE message_generated_field_admissions(id TEXT PRIMARY KEY,mes" +
	"sage_id TEXT NOT NULL,generation_id TEXT NOT NULL,session_id TEXT NOT NULL" +
	",tenant_id BIGINT NOT NULL,field TEXT NOT NULL,body_digest TEXT NOT NULL,s" +
	"ource_lineage TEXT NOT NULL,parent_materials TEXT NOT NULL,actor TEXT NOT " +
	"NULL,previous_admission_id TEXT NOT NULL,previous_revision BIGINT NOT NULL" +
	",previous_membership BIGINT NOT NULL,result_revision BIGINT NOT NULL,field" +
	"_revision BIGINT NOT NULL);\nCREATE TABLE message_generated_field_receipts" +
	"(message_id TEXT NOT NULL,field TEXT NOT NULL,admission_id TEXT NOT NULL R" +
	"EFERENCES message_generated_field_admissions(id),PRIMARY KEY(message_id,fi" +
	"eld));\nCREATE TRIGGER message_generated_field_admissions_immutable_update" +
	" BEFORE UPDATE ON message_generated_field_admissions BEGIN SELECT RAISE(AB" +
	"ORT,'immutable original generated message field'); END;\nCREATE TRIGGER me" +
	"ssage_generated_field_admissions_immutable_delete BEFORE DELETE ON message" +
	"_generated_field_admissions BEGIN SELECT RAISE(ABORT,'immutable original g" +
	"enerated message field'); END;\n\nCREATE TABLE message_generated_field_ver" +
	"sions(message_id TEXT NOT NULL,field TEXT NOT NULL,revision BIGINT NOT NUL" +
	"L,PRIMARY KEY(message_id,field));\nCREATE TRIGGER message_generated_field_" +
	"insert AFTER INSERT ON messages BEGIN INSERT INTO message_generated_field_" +
	"versions VALUES(NEW.id,'images',1),(NEW.id,'attachments',1),(NEW.id,'rende" +
	"red_content',1),(NEW.id,'context_checkpoint',1); END;\nCREATE TRIGGER mess" +
	"age_generated_field_images AFTER UPDATE ON messages WHEN OLD.images IS NOT" +
	" NEW.images BEGIN INSERT INTO message_generated_field_versions VALUES(NEW." +
	"id,'images',1) ON CONFLICT(message_id,field) DO UPDATE SET revision=revisi" +
	"on+1; END;\nCREATE TRIGGER message_generated_field_attachments AFTER UPDAT" +
	"E ON messages WHEN OLD.attachments IS NOT NEW.attachments BEGIN INSERT INT" +
	"O message_generated_field_versions VALUES(NEW.id,'attachments',1) ON CONFL" +
	"ICT(message_id,field) DO UPDATE SET revision=revision+1; END;\nCREATE TRIG" +
	"GER message_generated_field_rendered_content AFTER UPDATE ON messages WHEN" +
	" OLD.rendered_content IS NOT NEW.rendered_content BEGIN INSERT INTO messag" +
	"e_generated_field_versions VALUES(NEW.id,'rendered_content',1) ON CONFLICT" +
	"(message_id,field) DO UPDATE SET revision=revision+1; END;\nCREATE TRIGGER" +
	" message_generated_field_context_checkpoint AFTER UPDATE ON messages WHEN " +
	"OLD.context_checkpoint IS NOT NEW.context_checkpoint BEGIN INSERT INTO mes" +
	"sage_generated_field_versions VALUES(NEW.id,'context_checkpoint',1) ON CON" +
	"FLICT(message_id,field) DO UPDATE SET revision=revision+1; END;\n\nCREATE " +
	"TABLE message_fork_parent_origins(generation_id TEXT PRIMARY KEY REFERENCE" +
	"S message_generation_origins(id),parent_materials TEXT NOT NULL);\nCREATE " +
	"TRIGGER message_fork_parent_origins_immutable_update BEFORE UPDATE ON mess" +
	"age_fork_parent_origins BEGIN SELECT RAISE(ABORT,'immutable original fork " +
	"parents'); END;\nCREATE TRIGGER message_fork_parent_origins_immutable_dele" +
	"te BEFORE DELETE ON message_fork_parent_origins BEGIN SELECT RAISE(ABORT,'" +
	"immutable original fork parents'); END;\n")

const messageGeneratedFieldsPostgresUpgrade = ("-- Actual generated fields have " +
	"independent immutable parents and material" +
	".\nCREATE TABLE message_generated_field_admissions(id TEXT PRIMARY KEY,mes" +
	"sage_id TEXT NOT NULL,generation_id TEXT NOT NULL,session_id TEXT NOT NULL" +
	",tenant_id BIGINT NOT NULL,field TEXT NOT NULL,body_digest TEXT NOT NULL,s" +
	"ource_lineage TEXT NOT NULL,parent_materials TEXT NOT NULL,actor TEXT NOT " +
	"NULL,previous_admission_id TEXT NOT NULL,previous_revision BIGINT NOT NULL" +
	",previous_membership BIGINT NOT NULL,result_revision BIGINT NOT NULL,field" +
	"_revision BIGINT NOT NULL);\nCREATE TABLE message_generated_field_receipts" +
	"(message_id TEXT NOT NULL,field TEXT NOT NULL,admission_id TEXT NOT NULL R" +
	"EFERENCES message_generated_field_admissions(id),PRIMARY KEY(message_id,fi" +
	"eld));\nCREATE TRIGGER message_generated_field_admissions_immutable BEFORE" +
	" UPDATE OR DELETE ON message_generated_field_admissions FOR EACH ROW EXECU" +
	"TE FUNCTION message_material_immutable();\n\nCREATE TABLE message_generate" +
	"d_field_versions(message_id TEXT NOT NULL,field TEXT NOT NULL,revision BIG" +
	"INT NOT NULL,PRIMARY KEY(message_id,field));\nCREATE FUNCTION message_gene" +
	"rated_field_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE f " +
	"TEXT;BEGIN FOREACH f IN ARRAY ARRAY['images','attachments','rendered_conte" +
	"nt','context_checkpoint'] LOOP IF TG_OP='INSERT' OR to_jsonb(OLD)->f IS DI" +
	"STINCT FROM to_jsonb(NEW)->f THEN INSERT INTO message_generated_field_vers" +
	"ions VALUES(NEW.id,f,1) ON CONFLICT(message_id,field) DO UPDATE SET revisi" +
	"on=message_generated_field_versions.revision+1; END IF;END LOOP;RETURN NEW" +
	";END $$;\nCREATE TRIGGER message_generated_field_checkpoint AFTER INSERT O" +
	"R UPDATE ON messages FOR EACH ROW EXECUTE FUNCTION message_generated_field" +
	"_checkpoint();\n\nCREATE TABLE message_fork_parent_origins(generation_id T" +
	"EXT PRIMARY KEY REFERENCES message_generation_origins(id),parent_materials" +
	" TEXT NOT NULL);\nCREATE TRIGGER message_fork_parent_origins_immutable BEF" +
	"ORE UPDATE OR DELETE ON message_fork_parent_origins FOR EACH ROW EXECUTE F" +
	"UNCTION message_material_immutable();\n")

// Generated checkpoint/render/image material retains every actual original
// parent proof. Own host-field advancement does not re-certify other parents.
func (s *KnowledgeInputProvenanceStore) verifyGeneratedMessageFieldsInTx(tx *gorm.DB, message *types.Message) error {
	if e := checkUnreceiptedMessageFieldsInTx(tx, message.ID); e != nil {
		return e
	}
	var fields []messageGeneratedFieldAdmission
	if err := tx.Table("message_generated_field_admissions a").
		Select("a.*").
		Joins(("JOIN message_generated_field_receipts r ON r.admission_id=a.id AND r.messa" +
			"ge_id=a.message_id AND r.field=a.field")).
		Where("a.message_id=? AND a.session_id=?", message.ID, message.SessionID).
		Find(&fields).
		Error; err != nil {
		return err
	}
	for _, field := range fields {
		revision, err := messageGeneratedFieldVersion(tx, message.ID, field.Field)
		if err != nil {
			return err
		}
		if revision != field.FieldRevision {
			return ErrKnowledgeInputChanged
		}
		var value any
		switch field.Field {
		case "context_checkpoint":
			value = message.ContextCheckpoint
		case "images":
			value = message.Images
		case "attachments":
			value = message.Attachments
		case "rendered_content":
			value = message.RenderedContent
		default:
			return types.ErrSourceLineageUnknown
		}
		digest, err := KnowledgeInputDigest(value)
		if err != nil {
			return err
		}
		if digest != field.BodyDigest {
			return ErrKnowledgeInputChanged
		}
		var lineage types.SourceLineage
		if err := json.Unmarshal([]byte(field.SourceLineage), &lineage); err != nil {
			return err
		}
		if err := lineage.RequireComplete(); err != nil {
			return err
		}
		if err := verifyKnowledgeInheritedSources(tx, &lineage); err != nil {
			return err
		}
		var parents []*types.MessageMaterialSnapshot
		if err := json.Unmarshal([]byte(field.ParentMaterials), &parents); err != nil {
			return err
		}
		for _, parent := range parents {
			_, session, ownerRevision, err := s.currentMessageOwnerInTx(tx, parent.Request.SessionID)
			if err != nil {
				return err
			}
			if session.UserID != parent.SessionOwnerID || ownerRevision != parent.SessionOwnerRevision {
				return ErrKnowledgeInputChanged
			}
			membership, err := messageVersion(
				tx,
				"message_session_membership_versions",
				"session_id",
				parent.Request.SessionID,
			)
			if err != nil {
				return err
			}
			if membership != parent.MembershipRevision {
				return ErrKnowledgeInputChanged
			}
			for _, proof := range parent.Proofs {
				if err := s.checkMessageOriginalBodyAvailability(tx, proof.GenerationID); err != nil {
					return err
				}
				if proof.MessageID == message.ID {
					if proof.AdmissionID != field.PreviousAdmissionID || proof.Revision != field.PreviousRevision {
						return ErrKnowledgeInputChanged
					}
					continue
				}
				var current messageMaterialAdmission
				if err := tx.Table("message_material_admissions a").
					Select("a.*").
					Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").
					Where(
						"a.message_id=? AND a.tenant_id=? AND a.session_id=?",
						proof.MessageID,
						proof.TenantID,
						proof.SessionID,
					).
					Take(&current).
					Error; err != nil {
					return err
				}
				revision, err := messageVersion(tx, "message_material_versions", "message_id", proof.MessageID)
				if err != nil {
					return err
				}
				if current.ID != proof.AdmissionID || current.BodyDigest != proof.BodyDigest ||
					current.Revision != proof.Revision ||
					revision != proof.Revision {
					return ErrKnowledgeInputChanged
				}
			}
		}
	}
	if message.Role == "user" {
		required := map[string]bool{"images": false, "attachments": false}
		if message.RenderedContent != "" {
			required["rendered_content"] = false
		}
		for _, field := range fields {
			if _, ok := required[field.Field]; ok {
				required[field.Field] = true
			}
		}
		for _, known := range required {
			if !known {
				return types.ErrSourceLineageUnknown
			}
		}
	}
	if message.ContextCheckpoint != nil {
		found := false
		for _, field := range fields {
			if field.Field == "context_checkpoint" {
				found = true
			}
		}
		if !found {
			return types.ErrSourceLineageUnknown
		}
	}
	return nil
}

// New actual human creation certifies only the exact submitted raw fields.
// A later generated caption/render/checkpoint requires its own model callback.
func (s *KnowledgeInputProvenanceStore) sealInitialUserFieldsInTx(
	tx *gorm.DB,
	origin *messageGenerationOrigin,
	message *types.Message,
) error {
	if message.Role != "user" {
		return nil
	}
	var previous messageMaterialAdmission
	if err := tx.Table("message_material_admissions a").
		Select("a.*").
		Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").
		Where("a.message_id=?", message.ID).
		Take(&previous).
		Error; err != nil {
		return err
	}
	membership, err := messageVersion(tx, "message_session_membership_versions", "session_id", message.SessionID)
	if err != nil {
		return err
	}
	empty, _ := types.NewCompleteSourceLineage()
	source, err := json.Marshal(empty)
	if err != nil {
		return err
	}
	for _, entry := range []struct {
		Name  string
		Value any
	}{{"images", message.Images}, {"attachments", message.Attachments}} {
		digest, err := KnowledgeInputDigest(entry.Value)
		if err != nil {
			return err
		}
		fieldRevision, err := messageGeneratedFieldVersion(tx, message.ID, entry.Name)
		if err != nil {
			return err
		}
		admission := messageGeneratedFieldAdmission{
			FieldRevision:       fieldRevision,
			ID:                  uuid.NewString(),
			MessageID:           message.ID,
			GenerationID:        origin.ID,
			SessionID:           message.SessionID,
			TenantID:            origin.TenantID,
			Field:               entry.Name,
			BodyDigest:          digest,
			SourceLineage:       string(source),
			ParentMaterials:     "[]",
			Actor:               origin.Actor,
			PreviousAdmissionID: previous.ID,
			PreviousRevision:    previous.Revision,
			PreviousMembership:  membership,
			ResultRevision:      previous.Revision,
		}
		if err := tx.Create(&admission).Error; err != nil {
			return err
		}
		if err := tx.Create(&messageGeneratedFieldReceipt{message.ID, entry.Name, admission.ID}).Error; err != nil {
			return err
		}
	}
	return nil
}

func messageGeneratedFieldVersion(tx *gorm.DB, message, field string) (uint64, error) {
	var row struct{ Revision uint64 }
	if err := tx.Table("message_generated_field_versions").
		Where("message_id=? AND field=?", message, field).
		Take(&row).
		Error; err != nil {
		return 0, err
	}
	if row.Revision == 0 {
		return 0, ErrKnowledgeInputChanged
	}
	return row.Revision, nil
}

func messageGeneratedFieldProofs(tx *gorm.DB, message string) ([]*types.MessageGeneratedFieldProof, error) {
	var rows []messageGeneratedFieldAdmission
	if err := tx.Table("message_generated_field_admissions a").
		Select("a.*").
		Joins(("JOIN message_generated_field_receipts r ON r.admission_id=a.id AND r.messa" +
			"ge_id=a.message_id AND r.field=a.field")).
		Where("a.message_id=?", message).
		Order("a.field").
		Find(&rows).
		Error; err != nil {
		return nil, err
	}
	proofs := []*types.MessageGeneratedFieldProof{}
	for _, row := range rows {
		var source types.SourceLineage
		if err := json.Unmarshal([]byte(row.SourceLineage), &source); err != nil {
			return nil, err
		}
		proofs = append(
			proofs,
			&types.MessageGeneratedFieldProof{
				AdmissionID:   row.ID,
				Field:         row.Field,
				BodyDigest:    row.BodyDigest,
				Revision:      row.FieldRevision,
				SourceLineage: &source,
			},
		)
	}
	return proofs, nil
}

// A missing generated-field receipt can describe only the untouched field of
// an actual new generation. Foreign empty -> nonempty -> empty changes cannot
// become original controls again through a current empty value.
func checkUnreceiptedMessageFieldsInTx(tx *gorm.DB, messageID string) error {
	var rows []struct {
		Field       string
		Revision    uint64
		AdmissionID *string
	}
	if e := tx.Table("message_generated_field_versions v").
		Select("v.field,v.revision,r.admission_id").
		Joins("LEFT JOIN message_generated_field_receipts r ON r.message_id=v.message_id AND r.field=v.field").
		Where("v.message_id=?", messageID).
		Find(&rows).
		Error; e != nil {
		return e
	}
	if len(rows) != 4 {
		return types.ErrSourceLineageUnknown
	}
	for _, row := range rows {
		if (row.AdmissionID == nil || *row.AdmissionID == "") && row.Revision != 1 {
			return ErrKnowledgeInputChanged
		}
	}
	return nil
}
