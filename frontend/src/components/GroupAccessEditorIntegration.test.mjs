import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const kb = readFileSync(new URL('../views/knowledge/KnowledgeBaseEditorModal.vue', import.meta.url), 'utf8')
const agent = readFileSync(new URL('../views/agent/AgentEditorModal.vue', import.meta.url), 'utf8')

test('knowledge base editor mounts resource group access', () => {
  assert.match(kb, /resource-type="knowledge_base"/)
  assert.match(kb, /key: 'access'/)
})

test('agent editor mounts resource group access', () => {
  assert.match(agent, /resource-type="agent"/)
  assert.match(agent, /key: 'access'/)
})
