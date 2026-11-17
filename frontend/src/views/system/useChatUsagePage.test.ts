import assert from 'node:assert/strict'
import test from 'node:test'
import { effectScope, nextTick, ref } from 'vue'
import { useChatUsagePage } from './useChatUsagePage'

test('switching people clears old results and ignores late responses and errors', async () => {
  const scope = effectScope()
  const person = ref('A')
  const requests = new Map<string, ReturnType<typeof deferred>>()
  function deferred(signal: AbortSignal) {
    let resolve!: (value: string) => void, reject!: () => void
    const promise = new Promise<string>((ok, fail) => { resolve = ok; reject = fail })
    return { signal, promise, resolve, reject }
  }
  try {
    const result = scope.run(() => useChatUsagePage(signal => {
      const request = deferred(signal)
      requests.set(person.value, request)
      return request.promise
    }))!
    const settle = () => new Promise<void>(resolve => setTimeout(resolve, 0))
    requests.get('A')!.resolve('A sessions')
    await settle()
    assert.equal(result.data.value, 'A sessions')
    person.value = 'B'
    await nextTick()
    assert.equal(result.data.value, undefined)
    assert.equal(result.loading.value, true)
    person.value = 'C'
    await nextTick()
    assert.ok(requests.get('B')!.signal.aborted)
    requests.get('C')!.resolve('C sessions')
    await settle()
    requests.get('B')!.reject()
    await settle()
    assert.equal(result.data.value, 'C sessions')
    assert.equal(result.failed.value, false)
  } finally {
    scope.stop()
  }
})
