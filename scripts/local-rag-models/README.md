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

## 候选应用镜像隔离烟测

模型服务启动后，可用 `smoke_candidate.py` 在**全新的一次性 Docker Compose 项目**里验证指定本地应用镜像。脚本先核对可选的源码/补丁镜像标签，再生成随机测试凭据并仅在环回地址发布 API；它使用虚构采购条款注册测试用户、创建知识库、完成解析和 512 维入库，通过应用模型调试接口验证 Embedding/ReRank，并关闭关键词召回执行纯向量检索。结束时只删除自己生成的容器、网络、临时数据库和令牌副本；不会触碰共享 `18080` 或其他 Compose 项目。令牌文件须为 `0600`，脚本不会输出明文令牌、密码或测试文本。

```sh
APP_IMAGE=weknora-ldap-app:your-candidate-tag
APP_REVISION=your-40-character-source-commit
python3 scripts/local-rag-models/smoke_candidate.py \
  --image "$APP_IMAGE" \
  --token-file /private/path/rag-model-token \
  --expected-revision "$APP_REVISION"
```

把前两行替换为实际本机镜像标签及其构建源码提交；若镜像合入 Nextcloud 补丁，再加 `--expected-patch-sha` 指定镜像标签上的精确补丁 SHA-256。不要把历史示例提交号当成当前部署版本。

该命令是**本机可选验收**，不在 GitHub CI 中调用本机模型服务。它不生成答案，也不测试引用、无答案拒答、真实 AD 权限或企业语料；这些仍需独立验收。服务地址固定为 Docker 主机的 `host.docker.internal:19090`，只应在已批准的本地测试机器上运行。

若要同时验收评估结果持久化，可在同一候选镜像上运行 `smoke_evaluation_candidate.py`，参数与上例相同，只替换脚本名。默认使用仓库自带的 16 题虚构 Parquet 题集；增加 `--dataset-id synthetic-enterprise-zh-v2` 可跑 70 题虚构集，脚本不接受其他数据集 ID。它在独立环回 Compose 栈中使用正式 BGE Embedding/ReRank 和仅监听本机的确定性对话桩，检查题目全部完成、指标口径 v2、逐题记录、数据集指纹，并提交两条**仅供协议测试**的人工复核标签及虚构价格表，确认重启后任务详情、历史、人工标签和最终答复费用估算仍可读取。固定拒答不发生对话模型调用，缺失模型用量的真实调用则不得估成零元。脚本会验证镜像提交与补丁标签，并在结束时删除自己创建的容器和卷；它不会使用共享 `18080` 环境，也不会将题集发送到外部对话模型。确定性对话桩、虚构价格及自动写入的测试标签**不能**用于引用准确、忠实度、无答案拒答或真实费用验收。

## 完全虚构的题集

`dataset/benchmarks/synthetic-zh-v1/fixture.json` 是 16 题小样例；新增的 [`synthetic-enterprise-zh-v2`](../../dataset/benchmarks/synthetic-enterprise-zh-v2/README.md) 有 42 段、70 题，并用虚构组层级覆盖五类部门的直接/嵌套组。两者都只有虚构公司制度，没有企业原文、真实账号或 AD 对象。运行以下命令生成 WeKnora 的 5 个 Parquet 文件并验证协议和检索（将数据集路径替换为需要的版本）：

```sh
/path/to/rag-venv/bin/python scripts/local-rag-models/generate_fixture.py dataset/benchmarks/synthetic-zh-v1/fixture.json
RAG_MODEL_TOKEN="$(tr -d '\n' < /private/path/rag-model-token)" \
  /path/to/rag-venv/bin/python scripts/local-rag-models/benchmark.py dataset/benchmarks/synthetic-zh-v1/fixture.json
/path/to/rag-venv/bin/python -m unittest discover -s scripts/local-rag-models -p 'test_*.py'
```

题集覆盖中文单证据、跨文档、无答案，以及虚构直接/嵌套组身份允许与拒绝。`benchmark.py` 仅测 Embedding+ReRank 检索；它在测试夹具中预先按虚构 persona 过滤文档，因此 `forbidden_retrievals=0` **不是 WeKnora 权限或真实 AD 验收**。无答案题也必须另测生成阶段是否拒答。企业脱敏题集需要数据责任人单独批准，并按空间/模型明确允许外发范围；未获授权前不得替换这个合成题集进行真实内容评估。
