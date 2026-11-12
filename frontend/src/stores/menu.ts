import { reactive, ref, computed, watch } from 'vue'
import { defineStore } from 'pinia'
import i18n from '@/i18n'
import { useAuthStore } from '@/stores/auth'
import { useDeploymentCapabilitiesStore } from '@/stores/deploymentCapabilities'
import { filterVisibleMenuItems } from '@/stores/menuVisibility'
import type { DeploymentCapabilityKey } from '@/config/deploymentCapabilities'
import type { QuestionOrigin } from '@/utils/questionOrigin'

type MenuChild = Record<string, any>

interface MenuItem {
  title: string
  titleKey?: string
  icon: string
  path: string
  childrenPath?: string
  children?: MenuChild[]
  requiredCapability?: DeploymentCapabilityKey
}

const createMenuChildren = () => reactive<MenuChild[]>([])

export const useMenuStore = defineStore('menuStore', () => {
  const menuArr = reactive<MenuItem[]>([
    {
      title: '',
      titleKey: 'menu.newChat',
      icon: 'prefixIcon',
      path: 'creatChat',
      childrenPath: 'chat',
      children: createMenuChildren()
    },
    { title: '', titleKey: 'menu.knowledgeBase', icon: 'zhishiku', path: 'knowledge-bases' },
    // Artifacts only exist where skills run in a sandbox.
    { title: '', titleKey: 'menu.artifacts', icon: 'artifact', path: 'artifacts', requiredCapability: 'settings.sandbox' },
    { title: '', titleKey: 'menu.agents', icon: 'agent', path: 'agents', requiredCapability: 'agents' },
    { title: '', titleKey: 'menu.organizations', icon: 'organization', path: 'organizations', requiredCapability: 'organizations' },
    { title: '', titleKey: 'menu.settings', icon: 'setting', path: 'settings' },
    { title: '', titleKey: 'menu.logout', icon: 'logout', path: 'logout' }
  ])

  const isFirstSession = ref(false)
  const firstQuery = ref('')
  const firstMentionedItems = ref<any[]>([])
  const firstModelId = ref('')
  const firstImageFiles = ref<any[]>([])
  const firstAttachmentFiles = ref<any[]>([])
  const firstQuestionOrigin = ref<QuestionOrigin | null>(null)
  const prefillQuery = ref('')

  const applyMenuTranslations = () => {
    menuArr.forEach(item => {
      if (item.titleKey) {
        item.title = i18n.global.t(item.titleKey)
      }
    })
  }

  applyMenuTranslations()

  watch(
    () => i18n.global.locale.value,
    () => {
      applyMenuTranslations()
    }
  )

  // 菜单可见性过滤（lite 模式 / 普通员工收敛 / 部署能力）的判定抽成了
  // 纯函数 filterVisibleMenuItems（stores/menuVisibility.ts），这里只负责
  // 把 store 上下文喂进去。规则细节与测试见 menuVisibility.test.ts。
  //
  // 共享空间 (organizations) 仅对当前空间的 admin / owner 暴露入口。
  // viewer / contributor 即便在共享空间里拥有资源，也无需自行管理共享关系，
  // 入口在侧栏只会徒增噪音；后端 RBAC 才是权限的最终来源（见 middleware/rbac.go）。
  //
  // 普通员工（!hasRole('admin') && !isSystemAdmin）只保留 AI 对话与退出
  // 登录（W-UI-01）；organizations 因此自然被员工规则覆盖。
  const visibleMenuArr = computed(() => {
    const authStore = useAuthStore()
    const deploymentCapabilities = useDeploymentCapabilitiesStore()
    return filterVisibleMenuItems(menuArr, {
      isLiteMode: authStore.isLiteMode,
      isTenantAdmin: authStore.hasRole('admin'),
      isSystemAdmin: authStore.isSystemAdmin,
      isCapabilitySupported: capability =>
        deploymentCapabilities.isSupported(capability as DeploymentCapabilityKey | undefined),
    })
  })

  const chatMenuIndex = menuArr.findIndex(item => item.path === 'creatChat')

  const clearMenuArr = () => {
    const chatMenu = menuArr[chatMenuIndex]
    if (chatMenu && chatMenu.children) {
      chatMenu.children = createMenuChildren()
    }
  }

  const updatemenuArr = (obj: any) => {
    const chatMenu = menuArr[chatMenuIndex]
    if (!chatMenu.children) {
      chatMenu.children = createMenuChildren()
    }
    const exists = chatMenu.children.some((item: MenuChild) => item.id === obj.id)
    if (!exists) {
      chatMenu.children.push(obj)
    }
  }

  const updataMenuChildren = (item: MenuChild) => {
    const chatMenu = menuArr[chatMenuIndex]
    if (!chatMenu.children) {
      chatMenu.children = createMenuChildren()
    }
    chatMenu.children.unshift(item)
  }

  const updatasessionTitle = (sessionId: string, title: string) => {
    const chatMenu = menuArr[chatMenuIndex]
    chatMenu.children?.forEach((item: MenuChild) => {
      if (item.id === sessionId) {
        item.title = title
        item.isNoTitle = false
      }
    })
  }

  const changeIsFirstSession = (payload: boolean) => {
    isFirstSession.value = payload
  }

  const changeFirstQuery = (payload: string, mentionedItems: any[] = [], modelId: string = '', imageFiles: any[] = [], attachmentFiles: any[] = [], questionOrigin: QuestionOrigin | null = null) => {
    firstQuery.value = payload
    firstMentionedItems.value = mentionedItems
    firstModelId.value = modelId
    firstImageFiles.value = imageFiles
    firstAttachmentFiles.value = attachmentFiles
    firstQuestionOrigin.value = questionOrigin
  }

  const setPrefillQuery = (q: string) => {
    prefillQuery.value = q
  }

  const consumePrefillQuery = () => {
    const q = prefillQuery.value
    prefillQuery.value = ''
    return q
  }

  return {
    menuArr,
    visibleMenuArr,
    isFirstSession,
    firstQuery,
    firstMentionedItems,
    firstModelId,
    firstImageFiles,
    firstAttachmentFiles,
    firstQuestionOrigin,
    prefillQuery,
    clearMenuArr,
    updatemenuArr,
    updataMenuChildren,
    updatasessionTitle,
    changeIsFirstSession,
    changeFirstQuery,
    setPrefillQuery,
    consumePrefillQuery
  }
})
