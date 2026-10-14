import { onScopeDispose, ref, watch } from 'vue'
import { getAuthConfig } from '@/api/auth'

/** Public capability metadata keeps disabled-directory controls out of resource editors. */
export function useDirectoryFeature(visible: () => boolean) {
  const directoryEnabled = ref(false)
  let generation = 0
  watch(visible, async (open) => {
    const request = ++generation
    directoryEnabled.value = false
    if (!open) return
    const response = await getAuthConfig()
    if (request === generation) directoryEnabled.value = response.success && response.ldap_enabled === true
  }, { immediate: true })
  onScopeDispose(() => { generation++ })
  return { directoryEnabled }
}
