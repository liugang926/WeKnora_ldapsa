package repository

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

func ConfigureKnowledgeCompletionObserver(repo interfaces.KnowledgeRepository, observer func(context.Context, string) error) {
	if r, ok := repo.(*knowledgeRepository); ok {
		r.completionObserver = observer
	}
}
