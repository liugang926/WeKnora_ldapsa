package repository

// ContentLeaseStore uses the same durable database as the original input
// receipts. Output handlers must hold its read fences before loading bodies.
func (s *KnowledgeInputProvenanceStore) ContentLeaseStore() *NextcloudContentLeaseStore {
	if s == nil || s.db == nil {
		return nil
	}
	return NewNextcloudContentLeaseStore(s.db)
}
