export interface ChatCostInput {
  currency: string
  tariff_version: string
  input_per_million: number
  output_per_million: number
}

export interface ChatCost extends ChatCostInput {
  prompt_tokens: number
  completion_tokens: number
  chat_responses: number
  usage_reported_responses: number
  estimated_amount?: number
  set_by: string
  set_at: string
}

export interface CostRun { chat_cost?: ChatCost }

export function validChatCostInput(input: ChatCostInput): boolean {
  return /^[A-Z]{3}$/.test(input.currency) &&
    /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(input.tariff_version) &&
    [input.input_per_million, input.output_per_million].every(rate =>
      Number.isFinite(rate) && rate >= 0 && rate <= 1_000_000)
}

export function estimatedChatCost(run: CostRun): number | undefined {
  const amount = run.chat_cost?.estimated_amount
  return amount != null && Number.isFinite(amount) && amount >= 0 ? amount : undefined
}

export function comparableChatCost(left: CostRun, right: CostRun): boolean {
  return estimatedChatCost(left) != null && estimatedChatCost(right) != null &&
    !!left.chat_cost?.currency && left.chat_cost.currency === right.chat_cost?.currency
}
