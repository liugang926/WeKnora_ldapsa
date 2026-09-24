<template>
  <div class="evaluation-settings">
    <div class="evaluation-heading">
      <div>
        <h2>{{ copy.title }}</h2>
        <p>{{ copy.subtitle }}</p>
      </div>
      <button type="button" class="secondary" :disabled="loading" @click="refresh">{{ copy.refresh }}</button>
    </div>

    <div class="evaluation-card">
      <h3>{{ copy.newRun }}</h3>
      <div class="evaluation-form">
        <label>{{ copy.dataset }}<input v-model.trim="datasetID" placeholder="synthetic-zh-v1" /></label>
        <label>{{ copy.embedding }}
          <select v-model="embeddingID">
            <option value="">{{ copy.choose }}</option>
            <option v-for="model in embeddings" :key="model.id" :value="model.id">{{ model.display_name || model.name }}</option>
          </select>
        </label>
        <label>{{ copy.rerank }}
          <select v-model="rerankID">
            <option value="">{{ copy.choose }}</option>
            <option v-for="model in rerankers" :key="model.id" :value="model.id">{{ model.display_name || model.name }}</option>
          </select>
        </label>
        <label>{{ copy.chat }}
          <select v-model="chatID">
            <option value="">{{ copy.choose }}</option>
            <option v-for="model in chatModels" :key="model.id" :value="model.id">{{ model.display_name || model.name }}</option>
          </select>
        </label>
      </div>
      <p v-if="selectedEmbeddingIsMock" class="evaluation-warning">{{ copy.mockWarning }}</p>
      <p class="evaluation-note">{{ copy.dataWarning }}</p>
      <button type="button" class="primary" :disabled="running || !canRun" @click="startRun">
        {{ running ? copy.starting : copy.start }}
      </button>
    </div>

    <p v-if="error" class="evaluation-error" role="alert">{{ error }}</p>
    <div class="evaluation-card">
      <h3>{{ copy.history }}</h3>
      <p v-if="!runs.length && !loading" class="evaluation-empty">{{ copy.empty }}</p>
      <div v-else class="evaluation-table-wrap">
        <table class="evaluation-table">
          <thead><tr>
            <th>{{ copy.time }}</th><th>{{ copy.status }}</th><th>{{ copy.dataset }}</th>
            <th>Recall</th><th>NDCG@10</th><th>ROUGE-L</th><th>P95</th><th>{{ copy.tokens }}</th><th>{{ copy.review }}</th>
          </tr></thead>
          <tbody>
            <tr v-for="run in runs" :key="run.task.id">
              <td>{{ formatDate(run.task.start_time) }}</td>
              <td>{{ statusText(run.task.status) }} <span v-if="run.task.status === 1">({{ run.task.finished || 0 }}/{{ run.task.total || '?' }})</span></td>
              <td><span :title="run.task.dataset_sha256">{{ run.task.dataset_id }}</span></td>
              <td>{{ metric(run.metric?.retrieval_evaluated === 0 ? undefined : run.metric?.retrieval_metrics?.recall) }} <small v-if="run.metric?.retrieval_evaluated != null">(n={{ run.metric.retrieval_evaluated }})</small></td>
              <td>{{ metric(run.metric?.retrieval_metrics?.ndcg10) }}</td>
              <td>{{ metric(run.metric?.generation_evaluated === 0 ? undefined : run.metric?.generation_metrics?.rougel) }} <small v-if="run.metric?.generation_evaluated != null">(n={{ run.metric.generation_evaluated }})</small></td>
              <td>{{ run.metric?.execution_metrics?.latency_p95_ms ?? '—' }} ms</td>
              <td>{{ tokenCount(run) }}</td>
              <td><button v-if="run.cases?.some(Boolean)" type="button" class="secondary" @click="selectedRunID = run.task.id">{{ copy.open }}</button></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div v-if="selectedRun && selectedCases.length" class="evaluation-card">
      <h3>{{ copy.review }} · {{ selectedRun.task.dataset_id }}</h3>
      <p class="evaluation-note">{{ copy.caseNote }}</p>
      <details v-for="entry in selectedCases" :key="entry.question_id" class="evaluation-case">
        <summary>#{{ entry.question_id }} · {{ entry.question }}</summary>
        <p><strong>{{ copy.expected }}</strong> {{ entry.reference_answer || copy.noAnswer }}</p>
        <p><strong>{{ copy.generated }}</strong></p><pre>{{ entry.generated_answer }}</pre>
        <p><strong>{{ copy.relevant }}</strong> {{ entry.relevant_passage_ids.join(', ') || '—' }}</p>
        <p><strong>{{ copy.retrieved }}</strong> {{ entry.retrieved_passage_ids.join(', ') || '—' }}</p>
        <p><strong>{{ copy.reranked }}</strong> {{ entry.reranked_passage_ids.join(', ') || '—' }}</p>
      </details>
    </div>

    <div v-if="completedRuns.length >= 2" class="evaluation-card">
      <h3>{{ copy.compare }}</h3>
      <div class="evaluation-form comparison-selectors">
        <label>A<select v-model="leftID"><option value="">{{ copy.choose }}</option><option v-for="run in completedRuns" :key="run.task.id" :value="run.task.id">{{ run.task.dataset_id }} · {{ formatDate(run.task.start_time) }}</option></select></label>
        <label>B<select v-model="rightID"><option value="">{{ copy.choose }}</option><option v-for="run in completedRuns" :key="run.task.id" :value="run.task.id">{{ run.task.dataset_id }} · {{ formatDate(run.task.start_time) }}</option></select></label>
      </div>
      <p v-if="left && right && !comparable" class="evaluation-warning">{{ copy.notComparable }}</p>
      <div v-if="comparable" class="comparison-grid">
        <div v-for="row in comparison" :key="row.label"><strong>{{ row.label }}</strong><span>{{ row.left }} → {{ row.right }}</span><small>{{ row.delta }}</small></div>
      </div>
      <p class="evaluation-note">{{ copy.faithfulness }}</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { get, post } from '@/utils/request'
import { listModels, type ModelConfig } from '@/api/model'

interface CaseResult { question_id: number; question: string; reference_answer: string; generated_answer: string; relevant_passage_ids: number[]; retrieved_passage_ids: number[]; reranked_passage_ids: number[] }
interface Run {
  task: { id: string; dataset_id: string; dataset_sha256?: string; concurrency?: number; start_time: string; status: number; total?: number; finished?: number; err_msg?: string }
  metric?: { metric_version?: number; retrieval_evaluated?: number; generation_evaluated?: number; retrieval_metrics?: { recall?: number; ndcg10?: number }; generation_metrics?: { rougel?: number }; execution_metrics?: { latency_p95_ms?: number; prompt_tokens?: number; completion_tokens?: number } }
  cases?: Array<CaseResult | null>
}

const { locale } = useI18n()
const copy = computed(() => String(locale.value).startsWith('zh') ? {
  title: 'RAG 评估', subtitle: '持久化运行记录，并在相同数据集上比较检索配置。', refresh: '刷新',
  newRun: '新建评估', dataset: '数据集 ID', embedding: 'Embedding 模型', rerank: 'ReRank 模型', chat: '对话模型', choose: '请选择',
  mockWarning: '当前 Embedding 是模拟模型或维度过低，不能作为真实检索基线。',
  dataWarning: '仅上传已脱敏、获准用于所选模型的数据；此页面不会自动把受限 AD 文档送往外部模型。',
  start: '开始评估', starting: '提交中…', history: '历史运行', empty: '暂无评估记录。', time: '开始时间', status: '状态', tokens: 'Tokens',
  compare: '版本对比', notComparable: '仅可比较相同数据集指纹、指标口径和并发设置，且均已成功的两次运行。',
  faithfulness: '证据忠实度与引用准确性尚未自动评分；BLEU/ROUGE 不能替代这两项人工或可信评审。',
  review: '逐题复核', open: '查看', caseNote: '负数表示未能唯一映射到语料的检索结果。请依据原始脱敏题集逐条核验引用、拒答和证据忠实度。',
  expected: '参考答案：', generated: '生成答案：', relevant: '相关证据 ID：', retrieved: '召回 ID：', reranked: '重排 ID：', noAnswer: '应拒答',
} : {
  title: 'RAG evaluation', subtitle: 'Persisted runs and comparisons on the same dataset.', refresh: 'Refresh',
  newRun: 'New run', dataset: 'Dataset ID', embedding: 'Embedding model', rerank: 'ReRank model', chat: 'Chat model', choose: 'Select',
  mockWarning: 'This embedding is a mock or has too few dimensions for a real retrieval baseline.',
  dataWarning: 'Use only de-identified data approved for the selected models. Restricted AD documents are not exported automatically.',
  start: 'Start evaluation', starting: 'Submitting…', history: 'Run history', empty: 'No evaluation runs yet.', time: 'Started', status: 'Status', tokens: 'Tokens',
  compare: 'Compare versions', notComparable: 'Both runs must succeed and use the same dataset fingerprint, metric version, and concurrency.',
  faithfulness: 'Evidence faithfulness and citation accuracy are not yet auto-scored; BLEU/ROUGE cannot replace human or trusted judging.',
  review: 'Case review', open: 'Inspect', caseNote: 'Negative IDs are retrieval hits that could not be mapped uniquely to the corpus. Check citations, abstention and faithfulness against the approved fixture.',
  expected: 'Reference:', generated: 'Generated:', relevant: 'Relevant IDs:', retrieved: 'Retrieved IDs:', reranked: 'Reranked IDs:', noAnswer: 'Should abstain',
})

const datasetID = ref('synthetic-zh-v1')
const embeddingID = ref('')
const rerankID = ref('')
const chatID = ref('')
const models = ref<ModelConfig[]>([])
const runs = ref<Run[]>([])
const loading = ref(false)
const running = ref(false)
const error = ref('')
const leftID = ref('')
const rightID = ref('')
const selectedRunID = ref('')
const embeddings = computed(() => models.value.filter(m => m.type === 'Embedding' && m.status === 'active'))
const rerankers = computed(() => models.value.filter(m => m.type === 'Rerank' && m.status === 'active'))
const chatModels = computed(() => models.value.filter(m => m.type === 'KnowledgeQA' && m.status === 'active'))
const selectedEmbedding = computed(() => embeddings.value.find(m => m.id === embeddingID.value))
const selectedEmbeddingIsMock = computed(() => !!selectedEmbedding.value && (
  /mock|test/i.test(selectedEmbedding.value.name) || (selectedEmbedding.value.parameters.embedding_parameters?.dimension ?? 0) <= 3
))
const canRun = computed(() => !!datasetID.value && !!embeddingID.value && !!rerankID.value && !!chatID.value && !selectedEmbeddingIsMock.value)
const completedRuns = computed(() => runs.value.filter(r => r.task.status === 2))
const left = computed(() => runs.value.find(r => r.task.id === leftID.value))
const right = computed(() => runs.value.find(r => r.task.id === rightID.value))
const selectedRun = computed(() => runs.value.find(r => r.task.id === selectedRunID.value))
const selectedCases = computed(() => selectedRun.value?.cases?.filter((entry): entry is CaseResult => !!entry) || [])
const comparable = computed(() => !!left.value && !!right.value && left.value.task.id !== right.value.task.id &&
  !!left.value.task.dataset_sha256 && left.value.task.dataset_sha256 === right.value.task.dataset_sha256 &&
  (left.value.metric?.metric_version ?? 1) === (right.value.metric?.metric_version ?? 1) &&
  (left.value.task.concurrency ?? 0) === (right.value.task.concurrency ?? 0) &&
  left.value.task.status === 2 && right.value.task.status === 2)
const metric = (value?: number) => value == null ? '—' : value.toFixed(3)
const formatDate = (value: string) => new Date(value).toLocaleString()
const statusText = (status: number) => ({ 0: 'Pending', 1: 'Running', 2: 'Success', 3: 'Failed' }[status] || 'Unknown')
const tokenCount = (run: Run) => (run.metric?.execution_metrics?.prompt_tokens ?? 0) + (run.metric?.execution_metrics?.completion_tokens ?? 0)
const comparison = computed(() => {
  if (!left.value || !right.value || !comparable.value) return []
  const pairs: Array<[string, number | undefined, number | undefined]> = [
    ['Recall', left.value.metric?.retrieval_metrics?.recall, right.value.metric?.retrieval_metrics?.recall],
    ['NDCG@10', left.value.metric?.retrieval_metrics?.ndcg10, right.value.metric?.retrieval_metrics?.ndcg10],
    ['ROUGE-L', left.value.metric?.generation_metrics?.rougel, right.value.metric?.generation_metrics?.rougel],
    ['P95 ms', left.value.metric?.execution_metrics?.latency_p95_ms, right.value.metric?.execution_metrics?.latency_p95_ms],
  ]
  return pairs.map(([label, a, b]) => ({ label, left: metric(a), right: metric(b), delta: a == null || b == null ? '—' : `${b - a >= 0 ? '+' : ''}${(b - a).toFixed(3)}` }))
})

async function refresh() {
  loading.value = true
  try {
    const response = await get<{ success: boolean; data: Run[] }>('/api/v1/evaluation')
    runs.value = response.data || []
    error.value = ''
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : String(cause)
  } finally { loading.value = false }
}
async function startRun() {
  if (!canRun.value) return
  running.value = true
  try {
    await post('/api/v1/evaluation', { dataset_id: datasetID.value, embedding_id: embeddingID.value, rerank_id: rerankID.value, chat_id: chatID.value })
    await refresh()
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : String(cause)
  } finally { running.value = false }
}
let poll: ReturnType<typeof setInterval> | undefined
onMounted(async () => {
  try { models.value = await listModels() } catch (cause) { error.value = cause instanceof Error ? cause.message : String(cause) }
  await refresh()
  poll = setInterval(() => { if (runs.value.some(r => r.task.status === 0 || r.task.status === 1)) void refresh() }, 5000)
})
onUnmounted(() => { if (poll) clearInterval(poll) })
</script>

<style scoped>
.evaluation-settings { display: grid; gap: 18px; padding: 8px 4px 28px; color: var(--td-text-color-primary); }
.evaluation-heading { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
.evaluation-heading h2 { margin: 0 0 6px; font-size: var(--app-text-4xl); }
.evaluation-heading p, .evaluation-note, .evaluation-empty { color: var(--td-text-color-secondary); }
.evaluation-heading p { margin: 0; }
.evaluation-card { border: 1px solid var(--td-border-level-1-color); border-radius: var(--app-radius-xl); padding: 18px; background: var(--td-bg-color-container); }
.evaluation-card h3 { margin: 0 0 16px; font-size: var(--app-text-xl); }
.evaluation-form { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
.evaluation-form label { display: grid; gap: 6px; font-size: var(--app-text-md); }
.evaluation-form input, .evaluation-form select { min-width: 0; padding: 8px 10px; border: 1px solid var(--td-border-level-2-color); border-radius: var(--app-radius-sm); background: var(--td-bg-color-container); color: inherit; }
.evaluation-card button, .evaluation-heading button { padding: 8px 14px; border-radius: var(--app-radius-sm); cursor: pointer; }
.evaluation-card button:disabled, .evaluation-heading button:disabled { opacity: .5; cursor: not-allowed; }
.primary { margin-top: 10px; border: 0; background: var(--td-brand-color); color: white; }
.secondary { border: 1px solid var(--td-border-level-2-color); background: var(--td-bg-color-container); color: inherit; }
.evaluation-warning, .evaluation-error { color: var(--td-error-color); }
.evaluation-note { font-size: var(--app-text-sm); line-height: 1.5; }
.evaluation-table-wrap { overflow-x: auto; }
.evaluation-table { width: 100%; border-collapse: collapse; white-space: nowrap; font-size: var(--app-text-md); }
.evaluation-table th, .evaluation-table td { padding: 9px 10px; text-align: left; border-bottom: 1px solid var(--td-border-level-1-color); }
.comparison-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
.comparison-grid > div { display: grid; gap: 4px; border: 1px solid var(--td-border-level-1-color); border-radius: var(--app-radius-md); padding: 12px; }
.comparison-grid small { color: var(--td-text-color-secondary); }
.evaluation-case { border-top: 1px solid var(--td-border-level-1-color); padding: 10px 0; }
.evaluation-case summary { cursor: pointer; }
.evaluation-case pre { white-space: pre-wrap; overflow-wrap: anywhere; background: var(--td-bg-color-secondarycontainer); padding: 10px; border-radius: var(--app-radius-sm); }
@media (max-width: 720px) { .evaluation-form, .comparison-grid { grid-template-columns: 1fr; } .evaluation-heading { flex-wrap: wrap; } }
</style>
