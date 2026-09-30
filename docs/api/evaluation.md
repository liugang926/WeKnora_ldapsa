# 评估功能 API

[返回目录](./README.md)

| 方法 | 路径           | 描述                  |
| ---- | -------------- | --------------------- |
| GET  | `/evaluation/` | 获取评估任务结果       |
| POST | `/evaluation/` | 创建评估任务          |
| PUT  | `/evaluation/:taskId/cases/:questionId/review` | 保存逐题人工复核（管理员 JWT） |
| PUT  | `/evaluation/:taskId/chat-cost` | 保存对话 API 价格快照与费用估算（管理员 JWT） |

> 注：服务端路由带尾斜杠（Gin 会自动从 `/evaluation` 重定向到 `/evaluation/`），下方示例为方便阅读用了 `/evaluation`。

## GET `/evaluation` - 获取评估任务结果

**参数说明（查询参数）**:

| 字段     | 类型   | 必填 | 说明                                                |
| -------- | ------ | ---- | --------------------------------------------------- |
| task_id  | string | 是   | 从 `POST /evaluation` 返回的任务 ID                  |

**请求**:

```bash
curl --location 'http://localhost:8080/api/v1/evaluation?task_id=c34563ad-b09f-4858-b72e-e92beb80becb' \
--header 'X-API-Key: sk-xxxxx' \
--header 'Content-Type: application/json'
```

**响应**:

```json
{
    "data": {
        "task": {
            "id": "c34563ad-b09f-4858-b72e-e92beb80becb",
            "tenant_id": 1,
            "dataset_id": "default",
            "start_time": "2025-08-12T14:54:26.221804768+08:00",
            "status": 2,
            "total": 1,
            "finished": 1
        },
        "params": {
            "session_id": "",
            "knowledge_base_id": "2ef57434-8c8d-4442-b967-2f7fc578a2fc",
            "vector_threshold": 0.5,
            "keyword_threshold": 0.3,
            "embedding_top_k": 10,
            "vector_database": "",
            "rerank_model_id": "b30171a1-787b-426e-a293-735cd5ac16c0",
            "rerank_top_k": 5,
            "rerank_threshold": 0.7,
            "chat_model_id": "8aea788c-bb30-4898-809e-e40c14ffb48c",
            "summary_config": {
                "max_tokens": 0,
                "repeat_penalty": 1,
                "top_k": 0,
                "top_p": 0,
                "frequency_penalty": 0,
                "presence_penalty": 0,
                "prompt": "这是用户和助手之间的对话。",
                "context_template": "你是一个专业的智能信息检索助手",
                "no_match_prefix": "<think>\n</think>\nNO_MATCH",
                "temperature": 0.3,
                "seed": 0,
                "max_completion_tokens": 2048
            },
            "fallback_strategy": "",
            "fallback_response": "抱歉，我无法回答这个问题。"
        },
        "metric": {
            "retrieval_metrics": {
                "precision": 0,
                "recall": 0,
                "ndcg3": 0,
                "ndcg10": 0,
                "mrr": 0,
                "map": 0
            },
            "generation_metrics": {
                "bleu1": 0.037656734016532384,
                "bleu2": 0.04067392145167686,
                "bleu4": 0.048963321289052536,
                "rouge1": 0,
                "rouge2": 0,
                "rougel": 0
            }
        }
    },
    "success": true
}
```

## POST `/evaluation` - 创建评估任务

**参数说明（请求体）**:

| 字段              | 类型   | 必填 | 说明                                            |
| ----------------- | ------ | ---- | ----------------------------------------------- |
| dataset_id        | string | 是   | 只读挂载的评估数据集 ID；仓库内 `synthetic-*` 仅为虚构回归夹具 |
| knowledge_base_id | string | 否   | 评估使用的知识库 ID；省略时创建并清理临时知识库 |
| chat_id           | string | 是   | 评估使用的对话模型 ID                            |
| rerank_id         | string | 是   | 评估使用的重排序模型 ID                          |
| embedding_id      | string | 建议 | 临时知识库的 Embedding 模型 ID                   |

**请求**:

```bash
curl --location 'http://localhost:8080/api/v1/evaluation' \
--header 'X-API-Key: sk-xxxxx' \
--header 'Content-Type: application/json' \
--data '{
    "dataset_id": "default",
    "knowledge_base_id": "kb-00000001",
    "chat_id": "8aea788c-bb30-4898-809e-e40c14ffb48c",
    "rerank_id": "b30171a1-787b-426e-a293-735cd5ac16c0"
}'
```

**响应**:

```json
{
    "data": {
        "task": {
            "id": "c34563ad-b09f-4858-b72e-e92beb80becb",
            "tenant_id": 1,
            "dataset_id": "default",
            "start_time": "2025-08-12T14:54:26.221804768+08:00",
            "status": 1
        },
        "params": {
            "session_id": "",
            "knowledge_base_id": "2ef57434-8c8d-4442-b967-2f7fc578a2fc",
            "vector_threshold": 0.5,
            "keyword_threshold": 0.3,
            "embedding_top_k": 10,
            "vector_database": "",
            "rerank_model_id": "b30171a1-787b-426e-a293-735cd5ac16c0",
            "rerank_top_k": 5,
            "rerank_threshold": 0.7,
            "chat_model_id": "8aea788c-bb30-4898-809e-e40c14ffb48c",
            "summary_config": {
                "max_tokens": 0,
                "repeat_penalty": 1,
                "top_k": 0,
                "top_p": 0,
                "frequency_penalty": 0,
                "presence_penalty": 0,
                "prompt": "这是用户和助手之间的对话。",
                "context_template": "你是一个专业的智能信息检索助手，xxx",
                "no_match_prefix": "<think>\n</think>\nNO_MATCH",
                "temperature": 0.3,
                "seed": 0,
                "max_completion_tokens": 2048
            },
            "fallback_strategy": "",
            "fallback_response": "抱歉，我无法回答这个问题。"
        }
    },
    "success": true
}
```

## PUT `/evaluation/:taskId/chat-cost` - 记录对话 API 费用估算

仅空间管理员的登录 JWT 可提交；API Key 不可代替具名操作员。任务必须已完成且属于当前空间。请求体示例：

```json
{
  "currency": "CNY",
  "tariff_version": "provider-2026-09-30",
  "input_per_million": 1.0,
  "output_per_million": 2.0
}
```

费率为每百万输入/输出 token 的币种金额，必须为有限、非负且不大于 1,000,000 的数值；币种为 3 个大写字母，价格表版本为 1–64 个安全字符。服务端保存操作员 ID、UTC 时间、费率、token 数、用量回报覆盖数及历次修订。只有新版本运行中**至少有一条最终答复响应，且每条记录的最终答复响应都回报可分解的输入/输出 token**，才返回 `chat_cost.estimated_amount`；旧运行、零调用或缺失用量时该字段不存在，不得把它理解为零费用。两个价格字段都必须显式提交，零价仅表示操作员确认该项价格为零。

该金额仅为**已记录的最终答复对话调用**费用估算；查询理解等辅助 LLM 调用、Embedding、ReRank、本机算力、缓存价格、税费及其他基础设施费用均未统计，不能当成账单或 RAG 总成本。版本比较仅在两次运行完成、题集指纹/指标口径/并发相同且币种相同、双方估算都完整时显示此项；价格表版本不同会提示费用差额含价格变化。
