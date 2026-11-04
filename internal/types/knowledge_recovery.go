package types

import "context"

// KnowledgeRecoveryEligibility is a read-only action projection, never a grant.
type KnowledgeRecoveryEligibility struct {
	Allowed    bool   `json:"allowed"`
	Action     string `json:"action"`
	Reason     string `json:"reason"`
	Snapshot   string `json:"snapshot,omitempty"`
	Attempt    int    `json:"attempt"`
	Generation uint64 `json:"generation"`
}
type knowledgeRecoverySnapshotKey struct{}

func WithKnowledgeRecoverySnapshot(ctx context.Context, snapshot string) context.Context {
	return context.WithValue(ctx, knowledgeRecoverySnapshotKey{}, snapshot)
}
func KnowledgeRecoverySnapshot(ctx context.Context) string {
	value, _ := ctx.Value(knowledgeRecoverySnapshotKey{}).(string)
	return value
}
