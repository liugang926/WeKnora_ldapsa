package repository

import (
	"context"
	"slices"

	"github.com/Tencent/WeKnora/internal/types"
)

// IsAutomationAnswer distinguishes legacy scheduler outputs from ordinary
// follow-up messages in the same conversation.
func (s *KnowledgeInputProvenanceStore) IsAutomationAnswer(ctx context.Context, message *types.Message) (bool, error) {
	db := s.db.WithContext(ctx)
	var count int64
	err := db.Table("automation_runs").
		Where("id = ? AND session_id = ? AND assistant_message_id = ?",
			message.RequestID, message.SessionID, message.ID).
		Count(&count).Error
	if err != nil {
		// Schemas predating automations have no scheduler-produced messages.
		tables, schemaErr := db.Migrator().GetTables()
		if schemaErr == nil && !slices.Contains(tables, "automation_runs") {
			return false, nil
		}
	}
	return count != 0, err
}
