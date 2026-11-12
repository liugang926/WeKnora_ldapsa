package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/common"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

// PlanDingTalkDocumentRecovery is read-only. The CLI never restores an actor or
// starts the application container; authority is checked by the actual worker.
func PlanDingTalkDocumentRecovery(ctx context.Context,
	db *gorm.DB,
	tenant uint64,
	kb,
	source,
	id,
	action string) (*repository.DingTalkRecoveryPlan,
	error,
) {
	return repository.NewKnowledgeInputProvenanceStore(
		db).PlanDingTalkRecovery(ctx,
		tenant,
		kb,
		source,
		id,
		action,
		recoveryIndexContent)
}

func recoveryIndexContent(k *types.Knowledge, c *types.Chunk) string {
	return common.CleanInvalidUTF8(buildKnowledgeIndexContent(k, c.EmbeddingContent()))
}

type recoveryTriggerError struct{ err error }

func (e recoveryTriggerError) Error() string { return e.err.Error() }

// DingTalkDocumentRecoveryHandler uses the configured service without admitting new originals.
func DingTalkDocumentRecoveryHandler(knowledge interfaces.KnowledgeService) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		s, ok := knowledge.(*knowledgeService)
		if !ok {
			return fmt.Errorf("recovery service unavailable: %w", asynq.SkipRetry)
		}
		if err := s.processDingTalkDocumentRecovery(ctx, t); err != nil {
			var trigger recoveryTriggerError
			if errors.As(err, &trigger) {
				return err
			}
			return fmt.Errorf("DingTalk recovery rejected: %v: %w", err, asynq.SkipRetry)
		}
		return nil
	}
}

func (s *knowledgeService) processDingTalkDocumentRecovery(ctx context.Context, t *asynq.Task) error {
	if driver := os.Getenv("RETRIEVE_DRIVER"); driver != "" && driver != "postgres" {
		return errors.New("recovery requires PostgreSQL retrieval")
	}
	var p repository.DingTalkRecoveryPlan
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return err
	}
	if err := p.Validate(); err != nil {
		return err
	}
	store, ok := s.knowledgeInputs.(*repository.KnowledgeInputProvenanceStore)
	if !ok {
		return errors.New("original input store unavailable")
	}
	proof, err := store.LoadKnowledgeOriginalInput(ctx, p.TenantID, p.KnowledgeBaseID, p.KnowledgeID)
	if err != nil {
		return err
	}
	if proof.AdmissionID != p.AdmissionID ||
		proof.BuildGeneration != p.BuildGeneration ||
		proof.InputDigest != p.InputDigest ||
		proof.ConfigDigest != p.ConfigDigest {

		return repository.ErrKnowledgeInputChanged
	}
	ctx, err = store.RestoreKnowledgeBuildActor(ctx, proof)
	if err != nil {
		return err
	}
	ctx = repository.WithKnowledgeBuildInputProof(ctx, store, proof)
	if err = store.RecheckKnowledgeBuildInputs(ctx, proof); err != nil {
		return err
	}
	if p.Action == "resume-wiki" {
		applied, e := store.DingTalkRecoveryWasApplied(ctx, &p)
		if e != nil {
			return e
		}
		if applied {
			if e = enqueueWikiIngestTrigger(ctx, s.task, p.TenantID, p.KnowledgeBaseID); e != nil {
				return recoveryTriggerError{e}
			}
			return nil
		}
	}
	// Queue uncertainty is not an idle result. This maintenance task itself is
	// intentionally not a document-build task and is excluded by the inspector.
	if s.taskInspector == nil {
		return errors.New("queue inspection unavailable")
	}
	queued, err := s.taskInspector.HasQueuedTasksForKnowledge(ctx, p.KnowledgeID)
	if err != nil {
		return err
	}
	if queued {
		return errors.New("document still has processing work; generate a new plan later")
	}
	k, kb, err := loadKnowledgeWrite(ctx, s.repo, s.kbService, p.KnowledgeID)
	if err != nil {
		return err
	}
	if _, err = s.prepareDingTalkReparseInput(ctx, k, kb, nil); err != nil {
		return err
	}
	fresh,
		err := store.PlanDingTalkRecovery(ctx,
		p.TenantID,
		p.KnowledgeBaseID,
		p.DataSourceID,
		p.KnowledgeID,
		p.Action,
		recoveryIndexContent)
	if err != nil {
		return err
	}
	if fresh.Digest != p.Digest {
		return errors.New("recovery plan changed; generate a fresh plan")
	}
	switch p.Action {
	case "resume-wiki":
		if !kb.IsWikiEnabled() {
			return errors.New("wiki is not enabled")
		}
		op, err := newWikiIngestPendingOp(withAttempt(ctx, p.Attempt), p.TenantID, p.KnowledgeBaseID, p.KnowledgeID)
		if err != nil {
			return err
		}
		if err = store.ApplyDingTalkWikiRecovery(ctx, &p, op, recoveryIndexContent); err != nil {
			return err
		}
		if err = enqueueWikiIngestTrigger(ctx, s.task, p.TenantID, p.KnowledgeBaseID); err != nil {
			return recoveryTriggerError{err}
		}
		return nil
	case "reparse":
		// This is the existing U2 path, including its normal original-input guards.
		// Do not wrap it in a long SQL transaction: it performs external cleanup and
		// enqueue. Operators execute one exact plan at a time, without other reparses.
		tenant, e := s.tenantRepo.GetTenantByID(ctx, p.TenantID)
		if e != nil {
			return e
		}
		ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
		_, err = s.ReparseKnowledge(ctx, p.KnowledgeID, nil)
		return err
	default:
		return errors.New("unsupported recovery action")
	}
}
