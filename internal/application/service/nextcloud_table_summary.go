package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"hash"
	"io"
	"strconv"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
)

func tableProducerControls(p DataTableSummaryPayload) any {
	return struct {
		Kind, SummaryPrompt, ColumnPrompt                        string
		Tenant                                                   uint64
		KB, Knowledge, SummaryModel, EmbeddingModel, ArtifactSHA string
		Attempt, SampleRows                                      int
	}{
		types.TypeDataTableSummary, tableDescriptionPromptTemplate, columnDescriptionsPromptTemplate,
		p.TenantID, p.KnowledgeBaseID, p.KnowledgeID, p.SummaryModel, p.EmbeddingModel, p.ArtifactSHA256, p.Attempt, 10,
	}
}

// Table work starts only after the original source artifact was admitted by
// DocumentProcess. The original upload path never emits a weak machine job.
func (s *knowledgeService) enqueueNextcloudTableSummary(
	ctx context.Context,
	kb *types.KnowledgeBase,
	k *types.Knowledge,
) error {
	if k.Channel != types.ConnectorTypeNextcloud || !isDataTableFileType(k.FileType) {
		return nil
	}
	store, ok := s.knowledgeInputs.(*repository.KnowledgeInputProvenanceStore)
	if !ok || store == nil || s.task == nil {
		return types.ErrSourceLineageUnknown
	}
	original := repository.KnowledgeBuildInputProofFromContext(ctx)
	if original == nil {
		return types.ErrSourceLineageUnknown
	}
	actual, err := store.CaptureKnowledgeEnrichmentInputs(ctx, types.KnowledgeBuildInputCaptureRequest{
		TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, Expected: original,
	})
	if err != nil {
		return err
	}
	if actual.KnowledgeBase.SummaryModelID != kb.SummaryModelID ||
		actual.KnowledgeBase.EmbeddingModelID != kb.EmbeddingModelID {
		return repository.ErrKnowledgeInputChanged
	}
	ctx = repository.WithKnowledgeBuildInputProof(ctx, store, actual.Proof)
	p := DataTableSummaryPayload{
		TenantID:        k.TenantID,
		KnowledgeBaseID: k.KnowledgeBaseID,
		KnowledgeID:     k.ID,
		SummaryModel:    kb.SummaryModelID,
		EmbeddingModel:  kb.EmbeddingModelID,
		Attempt:         attemptFromCtx(ctx),
		OriginalInput:   actual.Proof,
	}
	p.ArtifactSHA256, err = store.NextcloudOriginalTableArtifactDigest(ctx, actual.Proof)
	if err != nil {
		return err
	}
	p.Initiator = types.TaskInitiatorFromContext(ctx)
	langfuse.InjectTracing(ctx, &p)
	p.ProducerInput, err = store.AdmitKnowledgeProducerInput(ctx, actual.Proof, "summary", tableProducerControls(p))
	if err != nil {
		return err
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	// Bind the queue execution to this original admission, artifact and build attempt.
	id := "nc-table:" + actual.Proof.AdmissionID + ":" + p.ArtifactSHA256 + ":" + strconv.Itoa(p.Attempt) + ":" +
		p.ProducerInput.ID
	task, err := newKnowledgeQueueTask(ctx, store, types.TypeDataTableSummary, raw,
		asynq.Queue(types.QueueSummary), asynq.MaxRetry(3), asynq.TaskID(id))
	if err != nil {
		return err
	}
	_, err = s.task.Enqueue(task)
	if err == asynq.ErrTaskIDConflict {
		return nil
	}
	return err
}

// Keep each real provider call inside the admitted model and source controls.
// The native vector transaction repeats this check at its commit boundary.
type verifiedTableEmbedder struct{ embedding.Embedder }

func (e verifiedTableEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if err := requireActualKnowledgeBuildInput(ctx); err != nil {
		return nil, err
	}
	result, err := e.Embedder.Embed(ctx, text)
	if err != nil {
		return nil, err
	}
	if err := requireActualKnowledgeBuildInput(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (e verifiedTableEmbedder) BatchEmbed(ctx context.Context, texts []string) ([][]float32, error) {
	if err := requireActualKnowledgeBuildInput(ctx); err != nil {
		return nil, err
	}
	result, err := e.Embedder.BatchEmbed(ctx, texts)
	if err != nil {
		return nil, err
	}
	if err := requireActualKnowledgeBuildInput(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (e verifiedTableEmbedder) BatchEmbedWithPool(
	ctx context.Context, _ embedding.Embedder, texts []string,
) ([][]float32, error) {
	// A table emits exactly two derived chunks. Keep that bounded provider call
	// on BatchEmbed so nested pool wrappers cannot replace this checkpoint.
	if len(texts) > 2 {
		return nil, types.ErrSourceLineageUnknown
	}
	return e.BatchEmbed(ctx, texts)
}

// ConfigureDataTableInputProvenance wires the same original-input authority
// used by document, summary and question workers into the table consumer.
func ConfigureDataTableInputProvenance(handler interfaces.TaskHandler, reader interfaces.KnowledgeBuildInputReader) {
	if table, ok := handler.(*DataTableSummaryService); ok {
		table.knowledgeInputs = reader
	}
}

type verifiedTableFileService struct {
	interfaces.FileService
	ctx          context.Context
	path, digest string
}

func (s verifiedTableFileService) GetFile(ctx context.Context, path string) (io.ReadCloser, error) {
	if path != s.path || s.digest == "" || s.ctx.Err() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	reader, err := s.FileService.GetFile(ctx, path)
	if err != nil {
		return nil, err
	}
	return &verifiedTableArtifactReader{ReadCloser: reader, ctx: s.ctx, expected: s.digest, digest: sha256.New()}, nil
}

type verifiedTableArtifactReader struct {
	io.ReadCloser
	ctx      context.Context
	expected string
	digest   hash.Hash
	checked  bool
}

func (r *verifiedTableArtifactReader) Read(dst []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.ReadCloser.Read(dst)
	if n > 0 {
		_, _ = r.digest.Write(dst[:n])
	}
	if err == io.EOF && !r.checked {
		r.checked = true
		if hex.EncodeToString(r.digest.Sum(nil)) != r.expected {
			return n, repository.ErrKnowledgeInputChanged
		}
	}
	return n, err
}
