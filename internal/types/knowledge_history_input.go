package types

// CommittedHistoryKnowledgeInput contains actual committed parent material.
// Callers supply server-resolved IDs, never an answer/question/proof string.
type CommittedHistoryKnowledgeInput struct {
	Knowledge *Knowledge                `json:"-"`
	Passages  []string                  `json:"-"`
	Proof     *KnowledgeBuildInputProof `json:"-"`
	Assistant *Message                  `json:"-"`
	User      *Message                  `json:"-"`
}

// CommittedHistoryKnowledgeInputRequest selects committed history using server-resolved parent IDs.
type CommittedHistoryKnowledgeInputRequest struct {
	TenantID           uint64 `json:"-"`
	KnowledgeBaseID    string `json:"-"`
	SessionID          string `json:"-"`
	AssistantMessageID string `json:"-"`
}
