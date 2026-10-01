# 评估能力（Evaluation）

评估使用带标准答案的问答数据集，比较分块、模型和检索配置的效果。系统创建评估知识库并导入语料，逐题执行检索与生成，输出 Precision、Recall、NDCG、MRR、MAP、BLEU 和 ROUGE 等指标。

::: tip 界面与 API
空间管理员可在「设置 → RAG Evaluation」发起评估并比较同一数据集的历史运行。也可通过 `POST /api/v1/evaluation` 发起、`GET /api/v1/evaluation?task_id=...` 轮询结果；不带 `task_id` 的 GET 返回当前空间最近 20 次运行。创建和读取均需要 Admin 权限，因为逐题结果包含脱敏题目与生成答案。数据集由部署管理员以只读文件挂载，页面不会上传语料。
:::

比较配置时应固定数据集，每次调整一个变量，并对照同一组指标分析结果。

## 运行一次评估

1. 准备符合下方格式、已脱敏且获准用于所选模型的 Parquet 数据集，或使用内置 `default` 样本。自定义数据集挂载于 `EVALUATION_DATASET_DIR/<dataset_id>/`；`dataset_id` 只允许字母、数字、下划线和短横线。默认每次最多 100 题，可由部署管理员设置 `EVALUATION_MAX_QUESTIONS`（1–10000）；这只是单次调用量上限，不是货币费用预算。
2. 明确选择 Embedding、对话模型和重排模型，通过 `POST /api/v1/evaluation` 创建任务。可选参考知识库用于复制配置，评估会使用单独的知识库；指定参考知识库时，`embedding_id` 必须与其现有 Embedding 一致。
3. 记录返回的任务 ID，通过 `GET /api/v1/evaluation?task_id=...` 查询状态和进度。
4. 任务成功后比较检索与生成指标；失败时先检查任务错误，再调整配置并重新运行。

创建与查询任务均需要 Admin 权限；API Key 还需评估能力或 full-access。

## 数据集格式

数据集服务（`internal/application/service/dataset.go`）为 `default` 从 `EVALUATION_DEFAULT_DATASET_DIR`（未设置时 `./dataset/samples/`）加载；自定义 ID 从环境变量 `EVALUATION_DATASET_DIR` 指向的目录（未设置时为 `./dataset/benchmarks/`）下加载同名子目录中的 5 个 **Parquet** 文件：

| 文件 | Schema | 含义 |
| --- | --- | --- |
| `queries.parquet` | `id: int64, text: string` | 问题集合 |
| `corpus.parquet` | `id: int64, text: string` | 语料段落（评估时灌入知识库） |
| `answers.parquet` | `id: int64, text: string` | 参考答案 |
| `qrels.parquet` | `qid: int64, pid: int64` | 问题 → 相关段落的 ground truth 关联（检索指标依据） |
| `qas.parquet` | `qid: int64, aid: int64` | 问题 → 答案映射（生成指标依据） |

对应的 Go 结构体：

```go
type TextInfo struct {
    ID   int64  `parquet:"id"`
    Text string `parquet:"text"`
}
type RelsInfo struct {
    QID int64 `parquet:"qid"`
    PID int64 `parquet:"pid"`
}
type QaInfo struct {
    QID int64 `parquet:"qid"`
    AID int64 `parquet:"aid"`
}
```

加载后拼装为逐样本的 `QAPair`（`internal/types/dataset.go`）：

```go
type QAPair struct {
    QID      int      // 问题 ID
    Question string   // 问题文本
    PIDs     []int    // 相关段落 ID（ground truth）
    Passages []string // 段落文本
    AID      int      // 答案 ID
    Answer   string   // 参考答案文本
}
```

自定义数据集需按上述 Schema 生成同名 Parquet 文件，由部署人员以只读目录挂载。完整语料都会被索引，包括没有关联 qrel 的干扰段落；稀疏 passage ID 会映射为稳定的连续 ID，避免大 ID 导致内存耗尽。空 qrel 可表示无答案问题，但当前聚合 Recall 对该类问题记 0，不能据此评价拒答质量。服务只记录题数与段落数，不把问题原文写入评估日志。

## 结果查询

`GET /api/v1/evaluation?task_id=<返回的任务 ID>`，返回 `EvaluationDetail`：

```json
{
  "success": true,
  "data": {
    "task": {
      "id": "evaluation-1-default",
      "dataset_id": "default",
      "status": 2,
      "total": 100,
      "finished": 100
    },
    "params": { "...": "ChatManage 评估参数快照" },
    "metric": {
      "retrieval_metrics": {
        "precision": 0.85, "recall": 0.92,
        "ndcg3": 0.88, "ndcg10": 0.86,
        "mrr": 0.95, "map": 0.87
      },
      "generation_metrics": {
        "bleu1": 0.72, "bleu2": 0.65, "bleu4": 0.58,
        "rouge1": 0.78, "rouge2": 0.71, "rougel": 0.75
      }
    }
  }
}
```

任务运行期间可轮询该接口获取 `finished / total` 进度；`status = 3` 时 `err_msg` 携带失败原因。

> **注意**：运行记录、参数、进度和指标现在写入 `evaluation_runs` 表，服务重启后可查询历史结果。正在运行的任务不具备跨进程续跑能力；若运行实例重启，应将旧任务视为中断并重新发起。每次运行记录数据集 SHA-256、模型 ID 与 `WEKNORA_BUILD_COMMIT`（若部署设置），便于判断两次结果是否可比。页面只对相同数据集指纹的成功运行做并排比较。

> **真实基线门槛**：模拟 Embedding、缺失 ReRank 或仅有对话模型时，不应称为真实 RAG 效果验证。评估需使用获准处理该数据集的正式模型；页面会拒绝显式的模拟/低维 Embedding。权限类问题还须按不同 AD 身份分别运行端到端授权测试；此离线评估会创建临时知识库，不能替代真实权限测试。

## 指标清单

指标注册表见 `internal/application/service/metric_hook.go`，共 12 项，分两组。文本先经 `metric/common.go` 分词：中文用 Jieba 分词、英文按空白切分、按 `。` / `.` 切句。

### 检索指标（Retrieval Metrics）

| 指标 | 字段 | 实现文件 | 含义 |
| --- | --- | --- | --- |
| Precision | `precision` | `metric/precision.go` | 检索准确率：命中的相关文档数 / 检索返回总数，按 GT 集合求均值 |
| Recall | `recall` | `metric/recall.go` | 检索召回率：命中的相关文档数 / 相关文档总数 |
| NDCG@3 | `ndcg3` | `metric/ndcg.go` | 归一化折损累计增益（取前 3 位），奖励把相关文档排在前面 |
| NDCG@10 | `ndcg10` | `metric/ndcg.go` | 同上，取前 10 位 |
| MRR | `mrr` | `metric/mrr.go` | 首个相关文档倒数排名的平均：`sum(1/rank) / N` |
| MAP | `map` | `metric/map.go` | 平均精度均值：对每个命中位置累计 `Precision@k` 再归一化 |

NDCG 核心计算（`metric/ndcg.go`）：

```go
// DCG = sum((2^rel_i - 1) / log2(i+2))，rel 为 0/1
dcg += (math.Pow(2, float64(relevance)) - 1) / math.Log2(float64(i+2))
// NDCG = DCG / IDCG（理想排序的 DCG）
```

MRR 核心计算（`metric/mrr.go`）：

```go
for i, predID := range ids {
    if _, ok := gtSet[predID]; ok {
        sumRR += 1.0 / float64(i+1) // 第一个命中位置的倒数
        break
    }
}
```

### 生成指标（Generation Metrics）

| 指标 | 字段 | 实现文件 | 含义 |
| --- | --- | --- | --- |
| BLEU-1 | `bleu1` | `metric/bleu.go` | 1-gram 精度（权重 `[1.0, 0, 0, 0]`） |
| BLEU-2 | `bleu2` | `metric/bleu.go` | 1/2-gram 各 50%（权重 `[0.5, 0.5, 0, 0]`） |
| BLEU-4 | `bleu4` | `metric/bleu.go` | 1~4-gram 均权（`[0.25, 0.25, 0.25, 0.25]`），含 brevity penalty |
| ROUGE-1 | `rouge1` | `metric/rouge.go` | 一元词重叠 F1 |
| ROUGE-2 | `rouge2` | `metric/rouge.go` | 二元词组重叠 F1 |
| ROUGE-L | `rougel` | `metric/rouge.go` | 最长公共子序列（LCS）F1 |

BLEU 核心（`metric/bleu.go`）：修正 n-gram 精度的加权几何平均乘以简短惩罚 `bp * exp(sum(w_i * log(p_i)))`。ROUGE 取 F1：`F1 = 2PR / (P + R + 1e-8)`（`metric/rouge_score.go`）。

## 接口与执行参考

### API

`internal/router/router.go`：

```go
evaluationRoutes := g.apiKeyGroup(r.Group("/evaluation"), apiKeyRunEvaluations(apiKeyFullAccess()))
{
    evaluationRoutes.POST("", g.Admin(), handler.Evaluation)
    evaluationRoutes.GET("", g.Viewer(), handler.GetEvaluationResult)
}
```

| 方法 | 路径 | 权限 | 说明 |
| --- | --- | --- | --- |
| POST | `/api/v1/evaluation` | Admin（API Key 需 `RunEvaluations` 能力） | 创建评估任务，立即返回任务信息 |
| GET | `/api/v1/evaluation?task_id=...` | Viewer | 查询任务状态、进度与指标结果 |

#### 创建评估任务

请求参数（`internal/handler/evaluation.go`）：

```go
type EvaluationRequest struct {
    DatasetID       string `json:"dataset_id"`        // 数据集 ID，默认 "default"
    KnowledgeBaseID string `json:"knowledge_base_id"` // 参考知识库（复用其配置）
    ChatModelID     string `json:"chat_id"`           // 聊天模型
    RerankModelID   string `json:"rerank_id"`         // 重排模型
}
```

| 参数 | 必填 | 默认行为 |
| --- | --- | --- |
| `dataset_id` | 否 | 缺省使用内置 `default` 数据集（`dataset/samples/`） |
| `knowledge_base_id` | 否 | 未提供则新建评估专用知识库；提供则复制其配置创建评估 KB |
| `chat_id` | 否 | 缺省自动选择默认 Chat 模型 |
| `rerank_id` | 否 | 缺省自动选择默认 Rerank 模型 |

任务 ID 是带时间戳和随机后缀的唯一值，不应自行拼接。任务对象（`internal/types/evaluation.go`）：

```go
type EvaluationTask struct {
    ID        string           `json:"id"`
    TenantID  uint64           `json:"tenant_id"`
    DatasetID string           `json:"dataset_id"`
    StartTime time.Time        `json:"start_time"`
    Status    EvaluationStatue `json:"status"`
    ErrMsg    string           `json:"err_msg,omitempty"`
    Total     int              `json:"total,omitempty"`    // 样本总数
    Finished  int              `json:"finished,omitempty"` // 已完成数
}
```

请求可增加 `embedding_id` 指定评估知识库的 Embedding。结果附有 `dataset_sha256`、`embedding_model_id`、`chat_model_id`、`rerank_model_id`、`build_revision`，以及 P50/P95 延迟和对话 token 用量。金额成本需要配置并版本化模型计价后另行计算，不能把 token 数当作费用。

BLEU/ROUGE 衡量文本重叠，**不代表**答案忠于检索证据，也不代表引用准确。建议基于脱敏问题、检索上下文和答案，使用 [Ragas](https://arxiv.org/abs/2309.15217) 的证据忠实度思路，并用 [ARES](https://arxiv.org/abs/2311.09476) 所强调的少量人工标注校准自动评审。当前界面明确将这两项标为尚未自动评分，不会伪造合格结论。

任务状态枚举（注意源码中拼写为 `EvaluationStatue`）：

```go
const (
    EvaluationStatuePending EvaluationStatue = iota // 0 待启动
    EvaluationStatueRunning                          // 1 运行中
    EvaluationStatueSuccess                          // 2 成功
    EvaluationStatueFailed                           // 3 失败
)
```

### 评估流程

`internal/application/service/evaluation.go` 中，POST 接口**同步完成准备、异步执行评估**：

1. **知识库准备**：新建（或按参考 KB 配置克隆）评估专用知识库，优先使用显式指定的 Embedding；
2. **参数装配**：从系统配置装配 `ChatManage` 评估参数——`VectorThreshold`、`KeywordThreshold`、`EmbeddingTopK`、`RerankTopK`、`RerankThreshold`、`MaxRounds`、`SummaryConfig`（MaxTokens / TopK / TopP / RepeatPenalty / Prompt / ContextTemplate 等）、`FallbackResponse`、改写提示词等；
3. **任务注册**：以唯一任务 ID 写入数据库，状态 `Pending`，立即返回响应；
4. **后台执行**（goroutine）：将数据集 corpus 灌入评估 KB → 并行评估每个 QA 对 → 汇聚指标 → 清理资源。

并发度取 `max(GOMAXPROCS - 1, 1)`（errgroup 限流）：

```go
var g errgroup.Group
metricHook := NewHookMetric(len(dataset))
g.SetLimit(max(runtime.GOMAXPROCS(0)-1, 1))
for i, qaPair := range dataset {
    g.Go(func() error {
        // 1. 克隆 ChatManage 配置
        // 2. 走 KnowledgeQAByEvent 完整管道（检索 + 重排 + 生成）
        // 3. 记录 MetricInput（检索到的 passage ID、生成文本、GT）
        // 4. 加锁更新 finished 进度
    })
}
g.Wait()
```

每个样本产出一个 `MetricInput`（`internal/types/evaluation.go`）：

```go
type MetricInput struct {
    RetrievalGT    [][]int // 检索 ground truth（相关 passage ID 列表）
    RetrievalIDs   []int   // 实际检索返回的 passage ID
    GeneratedTexts string  // 模型生成文本
    GeneratedGT    string  // 参考答案
}
```

`metric_hook.go` 对每个样本遍历所有已注册指标计算器求分，最终 `Avg()` 对全部样本逐指标取均值，写入 `MetricResult`。

::: warning RetrievalIDs 的口径
`RetrievalIDs` 必须是**数据集里的 passage ID**，不能直接用检索结果的 `ChunkIndex`——后者只是分块在知识库里的序号，与 passage ID 没有对应关系，直接使用会让所有检索指标恒为 0。`recordFinish` 因此把每条检索结果的正文与该样本的 ground truth passage 做双向包含匹配，反查出对应的 pid 并去重。重排结果为空时回退用原始检索结果，避免整条样本记成「什么都没召回」。

语料灌入也必须**同步等待索引完成**（`CreateKnowledgeFromPassageSync`）：异步入库时评估查询会跑在索引建好之前，同样表现为指标恒为 0。另外 passage 列表按 `maxPID + 1` 分配长度，pid 是 0-based 且包含末位。
:::

#### 评估流程图

```mermaid
flowchart TD
    A["POST /api/v1/evaluation<br/>(dataset_id, knowledge_base_id, chat_id, rerank_id)"] --> B["创建评估专用知识库<br/>(新建或克隆参考 KB 配置)"]
    B --> C["装配 ChatManage 评估参数<br/>(阈值 / TopK / Summary 配置)"]
    C --> D["持久化唯一任务 ID<br/>状态 Pending"]
    D --> E["立即返回任务信息"]
    D --> F["goroutine 后台执行, 状态 Running"]
    F --> G["加载 Parquet 数据集<br/>queries / corpus / qrels / answers / qas"]
    G --> H["corpus 灌入评估知识库"]
    H --> I["errgroup 并行处理 QA 对<br/>并发 = max(CPU-1, 1)"]
    I --> J["每个问题跑 KnowledgeQAByEvent<br/>检索 + 重排 + 生成"]
    J --> K["记录 MetricInput<br/>(RetrievalIDs vs GT, 生成文本 vs 参考答案)"]
    K --> L["MetricList.Avg 汇聚 12 项指标均值"]
    L --> M["写回 EvaluationDetail, 状态 Success / Failed<br/>清理评估知识库"]
    M --> N["GET /api/v1/evaluation?task_id=...<br/>轮询进度与指标"]
```

## 实现参考

以下路径均相对仓库根目录：

| 层 | 文件 |
| --- | --- |
| HTTP Handler | `internal/handler/evaluation.go` |
| 评估服务 | `internal/application/service/evaluation.go` |
| 指标注册与汇聚 | `internal/application/service/metric_hook.go` |
| 指标实现 | `internal/application/service/metric/`（`precision.go`、`recall.go`、`ndcg.go`、`mrr.go`、`map.go`、`bleu.go`、`rouge.go`、`rouge_score.go`、`common.go`） |
| 数据集加载 | `internal/application/service/dataset.go`、`internal/handler/dataset.go` |
| 类型定义 | `internal/types/evaluation.go`、`internal/types/dataset.go` |
| 内置样例数据集 | `dataset/samples/`（Parquet 文件） |
| 路由注册 | `internal/router/router.go` 的 `RegisterEvaluationRoutes` |
