package types

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/yanyiwu/gojieba"
)

// ErrEvaluationReviewInvalid means the judgment labels do not fit the case.
var ErrEvaluationReviewInvalid = errors.New("invalid evaluation case review")

// ErrEvaluationReviewNotReady means only completed runs may be reviewed.
var ErrEvaluationReviewNotReady = errors.New("evaluation run is not complete")

// ErrEvaluationCaseNotFound means the question is absent from the tenant's run.
var ErrEvaluationCaseNotFound = errors.New("evaluation case not found")

// ErrEvaluationReviewConflict means a concurrent writer changed the run.
var ErrEvaluationReviewConflict = errors.New("evaluation review changed concurrently")

// ErrEvaluationChatCostInvalid means the tariff metadata is incomplete or unsafe.
var ErrEvaluationChatCostInvalid = errors.New("invalid evaluation chat tariff")

// ErrEvaluationChatCostNotReady means only completed runs may be priced.
var ErrEvaluationChatCostNotReady = errors.New("evaluation chat cost is not ready")

// ErrEvaluationChatCostConflict means a concurrent writer changed the run.
var ErrEvaluationChatCostConflict = errors.New("evaluation chat cost changed concurrently")

// ErrEvaluationExplicitModelsRequired prevents a named or operator-provided
// dataset from being sent to whichever model happens to appear first.
var ErrEvaluationExplicitModelsRequired = errors.New("explicit embedding, rerank and chat models required for evaluation dataset")

// Jieba is a global instance of Chinese text segmentation tool
var Jieba *gojieba.Jieba = newJieba()

func newJieba() *gojieba.Jieba {
	dictDir := os.Getenv("JIEBA_DICT_DIR")
	if dictDir == "" {
		return gojieba.NewJieba()
	}

	return gojieba.NewJieba(
		filepath.Join(dictDir, "jieba.dict.utf8"),
		filepath.Join(dictDir, "hmm_model.utf8"),
		filepath.Join(dictDir, "user.dict.utf8"),
		filepath.Join(dictDir, "idf.utf8"),
		filepath.Join(dictDir, "stop_words.utf8"),
	)
}

// EvaluationStatue represents the status of an evaluation task
type EvaluationStatue int

const (
	EvaluationStatuePending EvaluationStatue = iota // Task is waiting to start
	EvaluationStatueRunning                         // Task is in progress
	EvaluationStatueSuccess                         // Task completed successfully
	EvaluationStatueFailed                          // Task failed
)

// EvaluationTask contains information about an evaluation task
type EvaluationTask struct {
	ID                       string `json:"id"`                       // Unique task ID
	TenantID                 uint64 `json:"tenant_id"`                // Tenant/Organization ID
	DatasetID                string `json:"dataset_id"`               // Dataset ID for evaluation
	DatasetSHA256            string `json:"dataset_sha256,omitempty"` // Exact QA fixture snapshot
	ReferenceKnowledgeBaseID string `json:"reference_knowledge_base_id,omitempty"`
	EmbeddingModelID         string `json:"embedding_model_id,omitempty"`
	ChatModelID              string `json:"chat_model_id,omitempty"`
	IndexingChatModelID      string `json:"indexing_chat_model_id,omitempty"` // Temporary KB summarization model
	RerankModelID            string `json:"rerank_model_id,omitempty"`
	BuildRevision            string `json:"build_revision,omitempty"`
	Concurrency              int    `json:"concurrency,omitempty"` // Worker cap used for this run

	StartTime time.Time        `json:"start_time"`           // Task start time
	UpdatedAt time.Time        `json:"updated_at,omitempty"` // Last durable progress update
	Status    EvaluationStatue `json:"status"`               // Current task status
	ErrMsg    string           `json:"err_msg,omitempty"`    // Error message if failed

	Total    int `json:"total,omitempty"`    // Total items to evaluate
	Finished int `json:"finished,omitempty"` // Completed items count
}

// EvaluationDetail contains detailed evaluation information
type EvaluationDetail struct {
	Task            *EvaluationTask         `json:"task"`                        // Evaluation task info
	Params          *ChatManage             `json:"params"`                      // Evaluation parameters
	Metric          *MetricResult           `json:"metric,omitempty"`            // Evaluation metrics
	Cases           []*EvaluationCaseResult `json:"cases,omitempty"`             // Per-question review evidence
	ChatCost        *EvaluationChatCost     `json:"chat_cost,omitempty"`         // Operator tariff snapshot
	ChatCostHistory []EvaluationChatCost    `json:"chat_cost_history,omitempty"` // Prior tariff estimates for audit
}

// EvaluationChatCostInput is a versioned operator-supplied chat API tariff.
// Auxiliary chat calls, retries outside the recorded final response, embedding,
// rerank, infrastructure, cache-specific discounts and taxes are excluded:
// this is not an invoice or total RAG cost.
type EvaluationChatCostInput struct {
	Currency         string  `json:"currency"`
	TariffVersion    string  `json:"tariff_version"`
	InputPerMillion  float64 `json:"input_per_million"`
	OutputPerMillion float64 `json:"output_per_million"`
}

// EvaluationChatCost snapshots the tariff and measured usage at review time.
// EstimatedAmount is absent if any chat response lacked provider token usage.
type EvaluationChatCost struct {
	EvaluationChatCostInput
	PromptTokens           int64     `json:"prompt_tokens"`
	CompletionTokens       int64     `json:"completion_tokens"`
	ChatResponses          int       `json:"chat_responses"`
	UsageReportedResponses int       `json:"usage_reported_responses"`
	EstimatedAmount        *float64  `json:"estimated_amount,omitempty"`
	SetBy                  string    `json:"set_by"`
	SetAt                  time.Time `json:"set_at"`
}

// EvaluationCaseResult preserves enough provenance for a human to judge
// citation accuracy and evidence faithfulness. Passage text stays in the
// separately controlled fixture, not in this row or application logs.
type EvaluationCaseResult struct {
	QuestionID          int                    `json:"question_id"`
	Question            string                 `json:"question"`
	ReferenceAnswer     string                 `json:"reference_answer"`
	GeneratedAnswer     string                 `json:"generated_answer"`
	RelevantPassageIDs  []int                  `json:"relevant_passage_ids"`
	RetrievedPassageIDs []int                  `json:"retrieved_passage_ids"`
	RerankedPassageIDs  []int                  `json:"reranked_passage_ids"`
	LatencyMs           int64                  `json:"latency_ms"`
	PromptTokens        int64                  `json:"prompt_tokens"`
	CompletionTokens    int64                  `json:"completion_tokens"`
	Review              *EvaluationCaseReview  `json:"review,omitempty"`
	ReviewHistory       []EvaluationCaseReview `json:"review_history,omitempty"`
}

// EvaluationCaseReview is a human judgment, not an LLM-derived metric. The
// reviewer must compare the answer and citations against the controlled
// evidence fixture; the application deliberately does not store that text.
type EvaluationCaseReview struct {
	Faithfulness     string    `json:"faithfulness"`
	CitationAccuracy string    `json:"citation_accuracy"`
	Abstention       string    `json:"abstention"`
	ReviewedBy       string    `json:"reviewed_by"`
	ReviewedAt       time.Time `json:"reviewed_at"`
}

// EvaluationCaseReviewInput contains only judgment labels. Free-form evidence
// text is excluded so reviewers do not accidentally copy controlled content
// into evaluation metadata or logs.
type EvaluationCaseReviewInput struct {
	Faithfulness     string `json:"faithfulness"`
	CitationAccuracy string `json:"citation_accuracy"`
	Abstention       string `json:"abstention"`
}

// String returns JSON representation of EvaluationTask
func (e *EvaluationTask) String() string {
	b, _ := json.Marshal(e)
	return string(b)
}

// MetricInput contains input data for metric calculation
type MetricInput struct {
	RetrievalGT  [][]int // Ground truth for retrieval
	RetrievalIDs []int   // Retrieved IDs

	GeneratedTexts string // Generated text for evaluation
	GeneratedGT    string // Ground truth text for comparison
}

// MetricResult contains evaluation metrics
type MetricResult struct {
	MetricVersion       int               `json:"metric_version,omitempty"`       // Legacy rows omit this
	RetrievalEvaluated  int               `json:"retrieval_evaluated,omitempty"`  // Questions with relevance evidence
	GenerationEvaluated int               `json:"generation_evaluated,omitempty"` // Questions with a reference answer
	RetrievalMetrics    RetrievalMetrics  `json:"retrieval_metrics"`              // Retrieval performance metrics
	GenerationMetrics   GenerationMetrics `json:"generation_metrics"`             // Text generation quality metrics
	ExecutionMetrics    ExecutionMetrics  `json:"execution_metrics"`
}

// ExecutionMetrics records measured latency and usage. Currency costs require
// a versioned operator tariff and complete provider-reported chat usage.
type ExecutionMetrics struct {
	LatencyP50Ms           int64 `json:"latency_p50_ms"`
	LatencyP95Ms           int64 `json:"latency_p95_ms"`
	PromptTokens           int64 `json:"prompt_tokens"`
	CompletionTokens       int64 `json:"completion_tokens"`
	UsageAccountingVersion int   `json:"usage_accounting_version,omitempty"`
	ChatResponses          int   `json:"chat_responses,omitempty"`
	UsageReportedResponses int   `json:"usage_reported_responses,omitempty"`
}

// RetrievalMetrics contains metrics for retrieval evaluation
type RetrievalMetrics struct {
	Precision float64 `json:"precision"` // Precision score
	Recall    float64 `json:"recall"`    // Recall score

	NDCG3  float64 `json:"ndcg3"`  // Normalized Discounted Cumulative Gain at 3
	NDCG10 float64 `json:"ndcg10"` // Normalized Discounted Cumulative Gain at 10
	MRR    float64 `json:"mrr"`    // Mean Reciprocal Rank
	MAP    float64 `json:"map"`    // Mean Average Precision
}

// GenerationMetrics contains metrics for text generation evaluation
type GenerationMetrics struct {
	BLEU1 float64 `json:"bleu1"` // BLEU-1 score
	BLEU2 float64 `json:"bleu2"` // BLEU-2 score
	BLEU4 float64 `json:"bleu4"` // BLEU-4 score

	ROUGE1 float64 `json:"rouge1"` // ROUGE-1 score
	ROUGE2 float64 `json:"rouge2"` // ROUGE-2 score
	ROUGEL float64 `json:"rougel"` // ROUGE-L score
}

// EvalState represents different stages of evaluation process
type EvalState int

const (
	StateBegin             EvalState = iota // Evaluation started
	StateAfterQaPairs                       // After loading QA pairs
	StateAfterDataset                       // After processing dataset
	StateAfterEmbedding                     // After generating embeddings
	StateAfterVectorSearch                  // After vector search
	StateAfterRerank                        // After reranking
	StateAfterComplete                      // After completion
	StateEnd                                // Evaluation ended
)
