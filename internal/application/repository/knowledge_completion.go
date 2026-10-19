package repository

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

func ConfigureKnowledgeCompletionObserver(repo interfaces.KnowledgeRepository, observer func(context.Context, string) error) {
	if r, ok := repo.(*knowledgeRepository); ok {
		r.completionObserver = observer
	}
}

// The handoff runs before completion commits. Failure rolls back both the last
// subtask decrement and completion, so redelivery can try the same handoff.
func ConfigureKnowledgeCompletionHandoff(repo interfaces.KnowledgeRepository, handoff func(context.Context, *gorm.DB, string) error) {
	if r, ok := repo.(*knowledgeRepository); ok {
		r.completionHandoff = handoff
	}
}
