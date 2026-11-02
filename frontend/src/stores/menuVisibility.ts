// 普通员工导航收敛（W-UI-01）的纯函数核心：从 menu store 里抽出来，
// 便于在 node 单测里直接验证过滤规则，不必搭 Pinia + i18n 环境。
//
// 只做前端隐藏层——服务端拦截由后端 RBAC 负责；这里隐藏的入口在
// 路由守卫（router/index.ts requiresTenantAdmin）有对应的兜底重定向。

export interface MenuVisibilityItem {
  path: string
  requiredCapability?: string
}

export interface MenuVisibilityContext {
  isLiteMode: boolean
  /** 当前空间角色是否 >= admin（authStore.hasRole('admin')）。 */
  isTenantAdmin: boolean
  /** 平台级系统管理员（authStore.isSystemAdmin）。 */
  isSystemAdmin: boolean
  /** 部署能力是否支持该菜单项的 requiredCapability。 */
  isCapabilitySupported: (capability?: string) => boolean
}

/**
 * 非管理员（`!hasRole('admin') && !isSystemAdmin`）仅保留 AI 对话
 * （creatChat，含其历史会话 children）与 logout；knowledgeBase /
 * artifacts / agents / organizations / settings 全部隐藏。管理员不变。
 */
export const EMPLOYEE_HIDDEN_MENU_PATHS = new Set([
  'knowledge-bases',
  'artifacts',
  'agents',
  'organizations',
  'settings',
])

/** Lite 模式本来就隐藏的入口（与员工收敛正交，两者叠加生效）。 */
export const LITE_HIDDEN_MENU_PATHS = new Set(['logout', 'organizations'])

/** 是否按管理员口径放行全部菜单。 */
export function isMenuAdminUser(isTenantAdmin: boolean, isSystemAdmin: boolean): boolean {
  return isTenantAdmin || isSystemAdmin
}

export function filterVisibleMenuItems<T extends MenuVisibilityItem>(
  items: T[],
  context: MenuVisibilityContext,
): T[] {
  const isAdmin = isMenuAdminUser(context.isTenantAdmin, context.isSystemAdmin)
  return items.filter(item => {
    if (context.isLiteMode && LITE_HIDDEN_MENU_PATHS.has(item.path)) {
      return false
    }
    if (!isAdmin && EMPLOYEE_HIDDEN_MENU_PATHS.has(item.path)) {
      return false
    }
    if (!context.isCapabilitySupported(item.requiredCapability)) {
      return false
    }
    return true
  })
}
