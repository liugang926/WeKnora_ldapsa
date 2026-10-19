package service

import (
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

func ConfigureMessageMaterialProvenance(
	messages interfaces.MessageService,
	store *repository.KnowledgeInputProvenanceStore,
) {
	if actual, ok := messages.(*messageService); ok {
		actual.materialStore = store
		repository.ConfigureMessageMaterialWriters(actual.messageRepo, store)
		if knowledge, ok := actual.knowService.(*knowledgeService); ok {
			knowledge.historyMaterialMessages = actual
		}
	}
}
