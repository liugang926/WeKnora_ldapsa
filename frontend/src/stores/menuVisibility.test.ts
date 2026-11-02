import assert from 'node:assert/strict'
import test from 'node:test'

import {
  filterVisibleMenuItems,
  isMenuAdminUser,
  EMPLOYEE_HIDDEN_MENU_PATHS,
} from './menuVisibility.ts'

// 与 stores/menu.ts 的 menuArr path 一一对应的桩数据。
const ALL_MENU_PATHS = [
  'creatChat',
  'knowledge-bases',
  'artifacts',
  'agents',
  'organizations',
  'settings',
  'logout',
]

const baseContext = {
  isLiteMode: false,
  isTenantAdmin: true,
  isSystemAdmin: false,
  isCapabilitySupported: () => true,
}

test('admins keep the full menu', () => {
  const visible = filterVisibleMenuItems(
    ALL_MENU_PATHS.map(path => ({ path })),
    baseContext,
  )
  assert.deepEqual(visible.map(item => item.path), ALL_MENU_PATHS)
})

test('ordinary employees only keep AI chat and logout (W-UI-01)', () => {
  const visible = filterVisibleMenuItems(
    ALL_MENU_PATHS.map(path => ({ path })),
    { ...baseContext, isTenantAdmin: false },
  )
  assert.deepEqual(visible.map(item => item.path), ['creatChat', 'logout'])
})

test('every hidden entry is covered by the employee rule', () => {
  // settings/organizations 等管理入口必须全部命中收敛集合。
  assert.deepEqual(
    [...EMPLOYEE_HIDDEN_MENU_PATHS].sort(),
    ['agents', 'artifacts', 'knowledge-bases', 'organizations', 'settings'],
  )
})

test('system admins keep the full menu even without a tenant role', () => {
  assert.equal(isMenuAdminUser(false, true), true)
  const visible = filterVisibleMenuItems(
    ALL_MENU_PATHS.map(path => ({ path })),
    { ...baseContext, isTenantAdmin: false, isSystemAdmin: true },
  )
  assert.deepEqual(visible.map(item => item.path), ALL_MENU_PATHS)
})

test('owner and admin roles both count as admin users', () => {
  // hasRole('admin') 在 store 里按角色等级比较，这里只验证口径函数。
  assert.equal(isMenuAdminUser(true, false), true)
  assert.equal(isMenuAdminUser(false, false), false)
})

test('user-menu settings shortcuts use the same employee gate (W-UI-01)', () => {
  // UserMenu.vue 的 canAccessSettingsShortcuts 直接复用 isMenuAdminUser；
  // 逐角色核对：viewer/contributor 关闭，admin/owner/systemAdmin 开启。
  const roleLevels: Record<string, number> = { viewer: 10, contributor: 20, admin: 30, owner: 40 }
  const minAdmin = roleLevels.admin
  const cases: Array<[string, boolean, boolean]> = [
    ['viewer', false, false],
    ['contributor', false, false],
    ['admin', true, false],
    ['owner', true, false],
    ['system-admin', false, true],
  ]
  for (const [role, isTenantAdmin, isSystemAdmin] of cases) {
    const expected = role === 'system-admin' || (roleLevels[role] ?? 0) >= minAdmin
    assert.equal(
      isMenuAdminUser(isTenantAdmin, isSystemAdmin),
      expected,
      `${role} should ${expected ? 'see' : 'not see'} settings shortcuts`,
    )
  }
})

test('lite mode and employee restriction compose', () => {
  const visible = filterVisibleMenuItems(
    ALL_MENU_PATHS.map(path => ({ path })),
    { ...baseContext, isLiteMode: true, isTenantAdmin: false },
  )
  // lite 隐藏 logout/organizations，员工又只保留 creatChat/logout → 只剩对话。
  assert.deepEqual(visible.map(item => item.path), ['creatChat'])
})

test('capability filtering still applies to the remaining entries', () => {
  const visible = filterVisibleMenuItems(
    [
      { path: 'creatChat' },
      { path: 'artifacts', requiredCapability: 'settings.sandbox' },
    ],
    { ...baseContext, isCapabilitySupported: capability => capability !== 'settings.sandbox' },
  )
  assert.deepEqual(visible.map(item => item.path), ['creatChat'])
})
