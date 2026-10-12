# Nextcloud 流式原件封存修复 — 2026-10-12

实际 `DataSourceService.ProcessSync` 复验发现：`ObserveNextcloudPublicationEpoch` 取得当前发布代次后，`Emit` 将其直接写入 connector 已封存的 `FetchedItem.Metadata`。原件证明覆盖完整 fetched item，因此后续 `WithNextcloudOriginalFetchedArtifact` 正确拒绝了这个被同步层修改的对象，正常文档无法入库。

修复保留 connector 原件及其摘要不变，将观察到的发布代次放入仅 service 包可构造、绑定 data source ID 的上下文，在构造独立 knowledge metadata 时才加入。当前来源/原件证明、代次、构建租约及容量门禁继续执行。不能通过忽略 proof 字段或重新封存任意修改来放宽校验。

`TestKnowledgeMachineActualPipelineSQLite` 从真实 HTTP connector 下载原文，调用实际同步、原件保存、解析/分块、后处理、摘要和自动标签任务，并验证本人读取与撤权。该测试现在安装与当前代码匹配的处理台账及恢复迁移，接入真实 processing ledger；原夹具停留在旧 schema，缺少这些门禁时不能作为当前版本的正向验收。新增断言确认 observation epoch 保存到 knowledge metadata。

本轮也由实际解析任务日志发现，LLM debug 保护未覆盖 `logger.Infof/Debugf` 的普通正文预览。受保护上下文的 package 日志函数现在只发固定诊断、不格式化参数或继承 metadata；直接取得 logrus logger 的调用关闭其内容输出。普通未标记上下文保留原日志行为。测试使用实际 output sink 和会在格式化时 panic 的 Stringer/error，覆盖普通、格式化、结构化及 detached context；直接 logrus 调用同样不能输出正文。

验证结果由 Nextcloud 仓 `docs/evidence/auto-space-prd-20261012/machine-pipeline-*.log` 与对应 receipt 记录。该 pipeline 的 HTTP 来源、模型和检索引擎为受控夹具；解析与业务任务真实执行，但不把它记为真实 Nextcloud/LDAPS/外部模型/向量检索的完整联合验收。

扩大回归选择时，两个旧 mock 测试仍失败：`TestNextcloudOrdinarySyncSkipsFailedSameETagBeforeCreate` 和 `TestNextcloudStreamHandlerStagesVersionBeforeAcceptingItem`。它们使用没有 connector 原件证明和容量准入的手工 FetchedItem，已存在于原 `baseline-regression-audit.json` 的失败清单；本轮没有放宽生产门禁来使旧 mock 通过，也没有把扩大选择报告为全绿。
