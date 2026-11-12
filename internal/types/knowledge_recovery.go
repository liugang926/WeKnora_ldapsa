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

// WithKnowledgeRecoverySnapshot stores the expected recovery snapshot in a context.
func WithKnowledgeRecoverySnapshot(ctx context.Context, snapshot string) context.Context {
	return context.WithValue(ctx, knowledgeRecoverySnapshotKey{}, snapshot)
}

// KnowledgeRecoverySnapshot returns the expected recovery snapshot, if present.
func KnowledgeRecoverySnapshot(ctx context.Context) string {
	value, _ := ctx.Value(knowledgeRecoverySnapshotKey{}).(string)
	return value
}
