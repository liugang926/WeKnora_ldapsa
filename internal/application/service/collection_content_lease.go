package service

import "github.com/Tencent/WeKnora/internal/application/repository"

func (a *FAQOutputAuthorizer) ContentLeaseStore() *repository.NextcloudContentLeaseStore {
	if a == nil {
		return nil
	}
	return a.store.ContentLeaseStore()
}

func (a *TagOutputAuthorizer) ContentLeaseStore() *repository.NextcloudContentLeaseStore {
	if a == nil {
		return nil
	}
	store, ok := a.reader.(*repository.KnowledgeInputProvenanceStore)
	if !ok {
		return nil
	}
	return store.ContentLeaseStore()
}
