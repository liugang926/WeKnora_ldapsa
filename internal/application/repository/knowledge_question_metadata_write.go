package repository

import (
	"context"
	"reflect"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type questionMetadataOutput struct {
	ChunkID, AdmissionID, ProducerInputID, PreviousMetadataDigest, PreviousBodyDigest, ResultMetadataDigest, ResultBodyDigest string
	PreviousRevision, ResultRevision                                                                                          uint64
}

func (questionMetadataOutput) TableName() string { return "knowledge_question_metadata_outputs" }
func questionMetadataIndependentBody(c *types.Chunk) (string, error) {
	copy := *c
	copy.Metadata = nil
	return knowledgeChunkDigest(&copy)
}

// A span may retain an old input only through the exact immutable result of
// that admission's question producer. An external metadata/body mutation has
// no matching transition, including an ABA restored to the same JSON bytes.
func verifyQuestionMetadataOwnResult(
	tx *gorm.DB,
	a *knowledgeInputAdmission,
	before, after *types.Chunk,
	beforeRevision, afterRevision uint64,
) error {
	if !tx.Migrator().HasTable("knowledge_question_metadata_outputs") {
		return ErrKnowledgeInputChanged
	}
	oldBody, err := questionMetadataIndependentBody(before)
	if err != nil {
		return err
	}
	newBody, err := questionMetadataIndependentBody(after)
	if err != nil || oldBody != newBody {
		return ErrKnowledgeInputChanged
	}
	currentDigest, err := KnowledgeInputDigest(before.Metadata)
	if err != nil {
		return err
	}
	targetDigest, err := KnowledgeInputDigest(after.Metadata)
	if err != nil {
		return err
	}
	revision := beforeRevision
	for step := 0; step < 128 && revision != afterRevision; step++ {
		var result questionMetadataOutput
		if err := tx.Where("chunk_id = ? AND admission_id = ? AND previous_revision = ? AND previous_metadata_digest = ?", before.ID, a.AdmissionID, revision, currentDigest).
			Order("result_revision ASC").
			Take(&result).
			Error; err != nil {
			return err
		}
		if result.ResultRevision <= revision || result.PreviousBodyDigest != oldBody ||
			result.ResultBodyDigest != newBody {
			return ErrKnowledgeInputChanged
		}
		var input knowledgeProducerInput
		if err := tx.Where("id = ? AND admission_id = ? AND purpose='question'", result.ProducerInputID, a.AdmissionID).
			Take(&input).
			Error; err != nil {
			return err
		}
		revision = result.ResultRevision
		currentDigest = result.ResultMetadataDigest
	}
	if revision != afterRevision || currentDigest != targetDigest {
		return ErrKnowledgeInputChanged
	}
	return nil
}

// Question generation commits only its actual metadata. Generic UpdateChunks
// updates text/flags and must not clear or silently omit another producer's
// metadata. Every model result in the batch shares one original input check.
func (r *chunkRepository) UpdateKnowledgeGeneratedQuestionMetadata(ctx context.Context, chunks []*types.Chunk) error {
	if len(chunks) == 0 {
		return nil
	}
	producer, ok := ctx.Value(knowledgeProducerInputContextKey{}).(knowledgeProducerInputContext)
	if !ok || producer.proof == nil || producer.proof.Purpose != "question" {
		return types.ErrSourceLineageUnknown
	}
	if err := PrecheckKnowledgeBuildWrite(ctx); err != nil {
		return err
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, c := range chunks {
			if c == nil {
				return ErrKnowledgeInputChanged
			}
			if _, err := validateKnowledgeInputWrite(
				ctx,
				tx,
				c.TenantID,
				c.KnowledgeBaseID,
				c.KnowledgeID,
			); err != nil {
				return err
			}
		}
		for _, c := range chunks {
			if err := ValidateNextcloudBuildWrite(ctx, tx, c.TenantID, c.KnowledgeBaseID, c.KnowledgeID); err != nil {
				return err
			}
			meta, err := c.DocumentMetadata()
			if err != nil || meta == nil || meta.GeneratedQuestionsRevision != c.ContentRevision {
				return ErrKnowledgeInputChanged
			}
			var previous types.Chunk
			if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_id = ?", c.ID, c.TenantID, c.KnowledgeID).
				Take(&previous).
				Error; err != nil {
				return err
			}
			previousRevision, err := inputRowRevision(tx, "chunk", c.ID)
			if err != nil {
				return err
			}
			previousDigest, err := KnowledgeInputDigest(previous.Metadata)
			if err != nil {
				return err
			}
			previousBody, err := questionMetadataIndependentBody(&previous)
			if err != nil {
				return err
			}
			resultBody, err := questionMetadataIndependentBody(c)
			if err != nil || previousBody != resultBody {
				return ErrKnowledgeInputChanged
			}
			result := tx.Model(&types.Chunk{}).
				Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ? AND content_revision = ?", c.ID, c.TenantID, c.KnowledgeBaseID, c.KnowledgeID, c.ContentRevision).
				Update("metadata", c.Metadata)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrKnowledgeInputChanged
			}
			if !reflect.DeepEqual(previous.Metadata, c.Metadata) &&
				tx.Migrator().HasTable("knowledge_question_metadata_outputs") {
				resultRevision, err := inputRowRevision(tx, "chunk", c.ID)
				if err != nil {
					return err
				}
				resultDigest, err := KnowledgeInputDigest(c.Metadata)
				if err != nil {
					return err
				}
				original := KnowledgeBuildInputProofFromContext(ctx)
				if original == nil {
					return ErrKnowledgeInputChanged
				}
				if err := tx.Create(
					&questionMetadataOutput{
						ChunkID:                c.ID,
						AdmissionID:            original.AdmissionID,
						ProducerInputID:        producer.proof.ID,
						PreviousRevision:       previousRevision,
						PreviousMetadataDigest: previousDigest,
						PreviousBodyDigest:     previousBody,
						ResultRevision:         resultRevision,
						ResultMetadataDigest:   resultDigest,
						ResultBodyDigest:       resultBody,
					},
				).Error; err != nil {
					return err
				}
			}
		}
		return sealKnowledgeInputChunks(ctx, tx, chunks)
	})
}
