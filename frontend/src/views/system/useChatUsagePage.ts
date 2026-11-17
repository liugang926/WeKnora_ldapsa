import { ref, shallowRef, watchEffect } from 'vue'

// A query changes when the administrator selects a person, searches, pages or
// retries. Never publish an earlier query's results/errors for its successor.
export function useChatUsagePage<T>(request: (signal: AbortSignal) => Promise<T> | undefined) {
  const data = shallowRef<T>()
  const loading = ref(false)
  const failed = ref(false)
  const revision = ref(0)
  watchEffect((onCleanup) => {
    void revision.value
    const controller = new AbortController()
    onCleanup(() => controller.abort())
    const result = request(controller.signal)
    data.value = undefined
    failed.value = false
    loading.value = !!result
    if (!result) return
    result.then((page) => {
      if (!controller.signal.aborted) data.value = page
    }).catch(() => {
      if (!controller.signal.aborted) failed.value = true
    }).finally(() => {
      if (!controller.signal.aborted) loading.value = false
    })
  })
  return { data, loading, failed, reload: () => { revision.value++ } }
}
