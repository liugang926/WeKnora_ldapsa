package types

// QAPair represents a complete QA example with question, related passages and answer
type QAPair struct {
	QID      int      // Question ID
	Question string   // Question text
	PIDs     []int    // Related passage IDs
	Passages []string // Passage texts
	AID      int      // Answer ID
	Answer   string   // Answer text
}

// EvaluationDataset keeps the complete, densely indexed corpus. A passage
// without a positive qrel is still a retrieval distractor and must be indexed.
type EvaluationDataset struct {
	QAPairs []*QAPair
	Corpus  []string
}
