import assert from 'node:assert/strict'
import test from 'node:test'
import { comparableChatCost, estimatedChatCost, validChatCostInput, type ChatCost } from './evaluationCost'

const cost: ChatCost = {
  currency: 'CNY', tariff_version: 'v2026-09-30', input_per_million: 1, output_per_million: 2,
  prompt_tokens: 1500, completion_tokens: 200, chat_responses: 2, usage_reported_responses: 2,
  estimated_amount: 0.0019, set_by: 'operator', set_at: '2026-09-30T00:00:00Z',
}

test('chat tariff requires a safe version, currency and finite nonnegative rates', () => {
  assert.equal(validChatCostInput(cost), true)
  assert.equal(validChatCostInput({ ...cost, currency: 'cny' }), false)
  assert.equal(validChatCostInput({ ...cost, tariff_version: 'unsafe version' }), false)
  assert.equal(validChatCostInput({ ...cost, input_per_million: Number.NaN }), false)
  assert.equal(validChatCostInput({ ...cost, output_per_million: -1 }), false)
})

test('cost comparison requires complete, same-currency estimates', () => {
  assert.equal(estimatedChatCost({ chat_cost: cost }), 0.0019)
  assert.equal(estimatedChatCost({ chat_cost: { ...cost, estimated_amount: undefined } }), undefined)
  assert.equal(comparableChatCost({ chat_cost: cost }, { chat_cost: { ...cost, estimated_amount: 0 } }), true)
  assert.equal(comparableChatCost({ chat_cost: cost }, { chat_cost: { ...cost, currency: 'USD' } }), false)
  assert.equal(comparableChatCost({ chat_cost: cost }, { chat_cost: { ...cost, estimated_amount: undefined } }), false)
})
