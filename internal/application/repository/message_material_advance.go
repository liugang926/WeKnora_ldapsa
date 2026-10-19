package repository

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type messageProducerAdvanceKey struct{}
type messageProducerAdvanceBoundary struct {
	Store                        *KnowledgeInputProvenanceStore
	MessageID, SessionID, Digest string
}
type messageProducerAdvance struct {
	AdmissionID                                             string `gorm:"primaryKey"`
	MessageID, SessionID, GenerationID, PreviousAdmissionID string
	PreviousRevision, PreviousMembership, ResultRevision    uint64
}

func (messageProducerAdvance) TableName() string { return "message_producer_advances" }
func (s *KnowledgeInputProvenanceStore) WithMessageProducerAdvance(ctx context.Context, message *types.Message) (context.Context, error) {
	digest, err := messageMaterialDigest(message)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, messageProducerAdvanceKey{}, messageProducerAdvanceBoundary{s, message.ID, message.SessionID, digest}), nil
}
func (s *KnowledgeInputProvenanceStore) sealMessageProducerAdvanceInTx(tx *gorm.DB, origin *messageGenerationOrigin, result *messageMaterialAdmission) error {
	marker, ok := tx.Statement.Context.Value(messageProducerAdvanceKey{}).(messageProducerAdvanceBoundary)
	if !ok {
		return nil
	}
	if marker.Store != s || marker.MessageID != result.MessageID || marker.SessionID != result.SessionID {
		return types.ErrSourceLineageUnknown
	}
	return tx.Create(&messageProducerAdvance{result.ID, result.MessageID, result.SessionID, result.GenerationID, origin.PreviousAdmissionID, origin.PreviousRevision, origin.PreviousMembership, result.Revision}).Error
}

func UpgradeMessageProducerAdvanceSchema(db *gorm.DB) error {
	if db.Migrator().HasTable("message_producer_advances") {
		return nil
	}
	ddl := messageProducerAdvanceSQLiteDDL
	if db.Dialector.Name() == "postgres" {
		ddl = messageProducerAdvancePostgresDDL
	} else if db.Dialector.Name() != "sqlite" {
		return types.ErrSourceLineageUnknown
	}
	return db.Transaction(func(tx *gorm.DB) error { return tx.Exec(ddl).Error })
}

const messageProducerAdvanceSQLiteDDL = `
CREATE TABLE message_producer_advances(admission_id TEXT PRIMARY KEY REFERENCES message_material_admissions(id),message_id TEXT NOT NULL,session_id TEXT NOT NULL,generation_id TEXT NOT NULL,previous_admission_id TEXT NOT NULL,previous_revision BIGINT NOT NULL,previous_membership BIGINT NOT NULL,result_revision BIGINT NOT NULL);
CREATE TRIGGER message_producer_advances_immutable_update BEFORE UPDATE ON message_producer_advances BEGIN SELECT RAISE(ABORT,'immutable actual message producer advance'); END;
CREATE TRIGGER message_producer_advances_immutable_delete BEFORE DELETE ON message_producer_advances BEGIN SELECT RAISE(ABORT,'immutable actual message producer advance'); END;
`
const messageProducerAdvancePostgresDDL = `
CREATE TABLE message_producer_advances(admission_id TEXT PRIMARY KEY REFERENCES message_material_admissions(id),message_id TEXT NOT NULL,session_id TEXT NOT NULL,generation_id TEXT NOT NULL,previous_admission_id TEXT NOT NULL,previous_revision BIGINT NOT NULL,previous_membership BIGINT NOT NULL,result_revision BIGINT NOT NULL);
CREATE TRIGGER message_producer_advances_immutable BEFORE UPDATE OR DELETE ON message_producer_advances FOR EACH ROW EXECUTE FUNCTION message_material_immutable();
`
