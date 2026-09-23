# 本地真实 Embedding / ReRank 验证

本方案在 Apple Silicon 主机上运行 [BAAI/bge-small-zh-v1.5](https://huggingface.co/BAAI/bge-small-zh-v1.5)（512 维）和 [BAAI/bge-reranker-base](https://huggingface.co/BAAI/bge-reranker-base) 原版权重。PyTorch 优先用 MPS，若不可用则回退 CPU。它们不是 3 维模拟 Embedding，也不会把文本发到外部推理 API；首次安装与下载权重需要联网。模型许可证、下载内容及企业数据使用仍由部署者复核。

## 主机启动

```sh
python3.12 -m venv /path/to/rag-venv
/path/to/rag-venv/bin/pip install -r scripts/local-rag-models/requirements.txt
/path/to/rag-venv/bin/pip install -r scripts/local-rag-models/requirements-fixture.txt
umask 077
openssl rand -hex 32 -out /private/path/rag-model-token
RAG_MODEL_TOKEN_FILE=/private/path/rag-model-token \
  RAG_MODEL_PYTHON=/path/to/rag-venv/bin/python \
  bash scripts/local-rag-models/start.sh
```

首次运行会下载模型；下载完成后 `start.sh` 默认 `HF_HUB_OFFLINE=1`，所以首启下载时先设 `HF_HUB_OFFLINE=0`。服务默认只监听 `127.0.0.1:19090`；Docker Desktop 的 `host.docker.internal` 在本机验证可连接这个地址。只对 `host.docker.internal` 加 SSRF 白名单。Bearer token 文件须为 `0600`，不要放入 Git 或日志，也不要把服务暴露到公网。容器端应以只读部署配置声明两个全局内置模型，API Key 通过环境变量注入，不写入 YAML。

模型配置的关键字段：`source: remote`、`provider: generic`、`base_url: http://host.docker.internal:19090/v1`，名称分别为 `BAAI/bge-small-zh-v1.5` 和 `BAAI/bge-reranker-base`；Embedding 维度为 `512`。本地测试部署的样例在 `../weknora-ldap-local/builtin_rag_models.yaml`（部署目录不在 Git 仓库中）。既有 3 维知识库不能原地切换到 512 维，应新建知识库或重新索引。已有生产资料不会自动送入本服务。

## 完全虚构的题集

`dataset/benchmarks/synthetic-zh-v1/fixture.json` 只有虚构公司制度，没有企业原文、真实账号或 AD 对象。运行以下命令生成 WeKnora 的 5 个 Parquet 文件并验证协议和检索：

```sh
/path/to/rag-venv/bin/python scripts/local-rag-models/generate_fixture.py dataset/benchmarks/synthetic-zh-v1/fixture.json
RAG_MODEL_TOKEN="$(tr -d '\n' < /private/path/rag-model-token)" \
  /path/to/rag-venv/bin/python scripts/local-rag-models/benchmark.py dataset/benchmarks/synthetic-zh-v1/fixture.json
/path/to/rag-venv/bin/python -m unittest discover -s scripts/local-rag-models -p 'test_*.py'
```

题集覆盖中文单证据、跨文档、无答案，以及虚构直接/嵌套组身份允许与拒绝。`benchmark.py` 仅测 Embedding+ReRank 检索；它在测试夹具中预先按虚构 persona 过滤文档，因此 `forbidden_retrievals=0` **不是 WeKnora 权限或真实 AD 验收**。无答案题也必须另测生成阶段是否拒答。企业脱敏题集需要数据责任人单独批准，并按空间/模型明确允许外发范围；未获授权前不得替换这个合成题集进行真实内容评估。
