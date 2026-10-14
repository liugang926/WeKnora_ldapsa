package types

// HistoryKnowledgeParentPlan names one current immutable history-child
// admission and the exact committed message parents it already recorded. It
// is control metadata for acquiring actual guards, never a body receipt.
type HistoryKnowledgeParentPlan struct {
	KnowledgeID     string
	AdmissionID     string
	InputDigest     string
	BuildGeneration uint64
	MessagePlan     *MessageMaterialReadPlan
}
