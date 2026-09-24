# 企业场景合成题集 v2

这是一批**完全虚构**的中文企业场景测试数据，不是从企业资料脱敏所得，也没有真实员工、AD 组、域名或凭据。`fixture.json` 的 SHA-256 为 `84f32acefa66b9d2bd9abe399825a5224eec188d31801d949502c6a3ae7d2812`。可用于本地功能和检索基线测试，不能充当“数据负责人已批准的企业题集”或生产验收结论。

| 类别 | 题数 | 预期 |
| --- | ---: | --- |
| 中文单证据 | 12 | 找到正确段落并引用 |
| 跨文档 | 8 | 找到全部证据段落 |
| 无答案 | 10 | 生成阶段应明确拒答，不臆测数值 |
| 权限允许 | 20 | 五类虚构部门的直接/嵌套组均可见其授权文档 |
| 权限拒绝 | 20 | 普通员工或其他部门成员不得取得受限文档 |

语料共 42 段：公共文档 12 段，运维、财务、人事、采购、安全各 6 段。每题均有 `id`、`category`、`question`、`answer` 和 `evidence`；拒绝题另有 `must_not_access`，权限题有 `persona`。`access_model` 描述虚构组的父子关系、直接组身份与作用域授权；`operations_nested` 等身份通过子组继承父组权限。生成器会解析组关系、拒绝循环并检查证据可见性。

本目录的五个 Parquet 文件由 `fixture.json` 生成，供 WeKnora 评估接口以数据集 ID `synthetic-enterprise-zh-v2` 载入。**Parquet 只包含语料、问题、答案、证据映射，不包含 persona/组元数据**；应用内普通评估不会替换登录身份，也不能单独证明权限。`benchmark.py` 通过测试夹具事先过滤可见文档，仅检验检索链路；真实 AD、API Key、分享、智能体等入口仍须另做端到端鉴权测试。

在仓库根目录运行：

```sh
/path/to/rag-venv/bin/python scripts/local-rag-models/generate_fixture.py dataset/benchmarks/synthetic-enterprise-zh-v2/fixture.json
/path/to/rag-venv/bin/python -m unittest discover -s scripts/local-rag-models -p 'test_*.py'
RAG_MODEL_TOKEN="$(tr -d '\n' < /private/path/rag-model-token)" \
  /path/to/rag-venv/bin/python scripts/local-rag-models/benchmark.py dataset/benchmarks/synthetic-enterprise-zh-v2/fixture.json
```

2026-09-24 在本机 M2 Pro/MPS 使用 `BAAI/bge-small-zh-v1.5`（512 维）和 `BAAI/bge-reranker-base` 的检索烟测：40 道有答案题的 Embedding Recall@5 为 1.000，ReRank Recall@1 为 0.762、Recall@3/5 为 1.000，NDCG@5 为 0.992；70 题检索链路 P50 为 64.2 ms、P95 为 181.4 ms，夹具预过滤后受限段落命中数为 0。这些数值不包含生成答案、拒答质量、引用忠实度、服务并发、索引更新或真实权限验证。该合成题集偏小且文档表述与问题接近，指标可能明显高估真实业务效果。
