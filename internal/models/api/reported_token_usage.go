package api

import "github.com/Tencent/WeKnora/internal/types"

// ReportedTokenUsage preserves the wire distinction between an explicit zero
// and an absent counter. Positive counts already carry their own evidence.
func ReportedTokenUsage(input, output, total *int) types.TokenUsage {
	u := types.TokenUsage{
		PromptTokens: valueOrZero(input), CompletionTokens: valueOrZero(output), TotalTokens: valueOrZero(total),
	}
	if u.TotalTokens == 0 {
		u.TotalTokens = u.PromptTokens + u.CompletionTokens
	}
	u.ProviderReported = u.PromptTokens == 0 && u.CompletionTokens == 0 && u.TotalTokens == 0 &&
		(total != nil || input != nil && output != nil)
	return u
}
