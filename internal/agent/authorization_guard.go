package agent

import "context"

// SetRoundGuard installs a request-local authorization check that runs before
// every ReAct round, including the first. A revocation that lands while an
// agent is running therefore stops the next background execution stage before
// it can make another model or tool call.
func (e *AgentEngine) SetRoundGuard(guard func(context.Context) error) {
	e.roundGuard = guard
}
