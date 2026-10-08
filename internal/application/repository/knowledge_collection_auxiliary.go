package repository

import (
	"reflect"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// These helpers deliberately use their caller's RR/SQLite transaction. Each
// producer keeps its own original bounded lineage; the collection authorizer
// checks current caller/source permission for every proof separately.
func captureKnowledgeCollectionAuxiliary(tx *gorm.DB, scopes []types.KnowledgeSearchScope, candidates []*types.Knowledge) (*types.KnowledgeCollectionAuxiliary, error) {
	aux := &types.KnowledgeCollectionAuxiliary{Proofs: []*types.KnowledgeCollectionAuxiliaryProof{}, KnowledgeBaseNames: map[string]string{}, Tags: map[string][]*types.KnowledgeTag{}, Relations: []*types.KnowledgeTagRelation{}}
	allowed := map[string]uint64{}
	kbIDs := []string{}
	for _, scope := range scopes {
		if previous, ok := allowed[scope.KBID]; ok {
			if previous != scope.TenantID {
				return nil, ErrKnowledgeInputChanged
			}
			continue
		}
		allowed[scope.KBID] = scope.TenantID
		kbIDs = append(kbIDs, scope.KBID)
	}
	knowledgeIDs := []string{}
	for _, k := range candidates {
		if k == nil || allowed[k.KnowledgeBaseID] != k.TenantID {
			return nil, ErrKnowledgeInputChanged
		}
		knowledgeIDs = append(knowledgeIDs, k.ID)
	}
	allRelations := []*types.KnowledgeTagRelation{}
	for first := 0; first < len(knowledgeIDs); first += 500 {
		end := min(first+500, len(knowledgeIDs))
		var rows []*types.KnowledgeTagRelation
		if err := tx.Where("knowledge_id IN ?", knowledgeIDs[first:end]).Order("knowledge_id ASC,tag_id ASC").Find(&rows).Error; err != nil {
			return nil, err
		}
		allRelations = append(allRelations, rows...)
	}
	byKnowledge := map[string][]*types.KnowledgeTagRelation{}
	tagIDs := []string{}
	unique := map[string]bool{}
	for _, relation := range allRelations {
		byKnowledge[relation.KnowledgeID] = append(byKnowledge[relation.KnowledgeID], relation)
		if !unique[relation.TagID] {
			unique[relation.TagID] = true
			tagIDs = append(tagIDs, relation.TagID)
		}
	}
	tagsByID := map[string]*types.KnowledgeTag{}
	for first := 0; first < len(tagIDs); first += 500 {
		end := min(first+500, len(tagIDs))
		var rows []*types.KnowledgeTag
		if err := tx.Where("id IN ?", tagIDs[first:end]).Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, tag := range rows {
			tagsByID[tag.ID] = tag
		}
	}
	cachedTx, err := cacheKnowledgeDisplayInputs(tx, append(append(kbIDs, knowledgeIDs...), tagIDs...))
	if err != nil {
		return nil, err
	}
	tx = cachedTx
	var kbs []*types.KnowledgeBase
	for first := 0; first < len(kbIDs); first += 500 {
		end := min(first+500, len(kbIDs))
		var rows []*types.KnowledgeBase
		if err := tx.Where("id IN ?", kbIDs[first:end]).Find(&rows).Error; err != nil {
			return nil, err
		}
		kbs = append(kbs, rows...)
	}
	if len(kbs) != len(allowed) {
		return nil, ErrKnowledgeInputChanged
	}
	for _, kb := range kbs {
		if allowed[kb.ID] != kb.TenantID {
			return nil, ErrKnowledgeInputChanged
		}
		proof, err := readKnowledgeDisplayOrigin(tx, "kb-name", kb.ID, kb.TenantID, kb.ID, kb.Name)
		if err != nil {
			return nil, err
		}
		aux.KnowledgeBaseNames[kb.ID] = kb.Name
		aux.Proofs = append(aux.Proofs, proof)
	}
	seenTags := map[string]bool{}
	for _, k := range candidates {
		if k == nil || allowed[k.KnowledgeBaseID] != k.TenantID {
			return nil, ErrKnowledgeInputChanged
		}
		relations := byKnowledge[k.ID]
		ids := make([]string, 0, len(relations))
		for _, relation := range relations {
			ids = append(ids, relation.TagID)
		}
		revision, err := knowledgeDisplayRevision(tx, "relations", k.ID)
		if err != nil {
			return nil, err
		}
		var membership *types.KnowledgeCollectionAuxiliaryProof
		if len(relations) > 0 {
			membership, err = readKnowledgeDisplayOrigin(tx, "relations", k.ID, k.TenantID, k.KnowledgeBaseID, ids)
			if err != nil {
				return nil, err
			}
		} else {
			// An absent relation contains no source-bearing display value. Its
			// monotonic version still binds membership, including add/delete ABA.
			lineage, _ := types.NewCompleteSourceLineage()
			digest, err := KnowledgeInputDigest(knowledgeDisplayBody("relations", k.ID, k.TenantID, k.KnowledgeBaseID, ids))
			if err != nil {
				return nil, err
			}
			membership = &types.KnowledgeCollectionAuxiliaryProof{Kind: "relations", ID: k.ID, TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, Revision: revision, Digest: digest, SourceLineage: lineage}
		}
		aux.Proofs = append(aux.Proofs, membership)
		aux.Tags[k.ID] = []*types.KnowledgeTag{}
		for _, relation := range relations {
			stored := tagsByID[relation.TagID]
			if stored == nil || stored.TenantID != k.TenantID || stored.KnowledgeBaseID != k.KnowledgeBaseID {
				return nil, ErrKnowledgeInputChanged
			}
			tag := *stored
			aux.Tags[k.ID] = append(aux.Tags[k.ID], &tag)
			aux.Relations = append(aux.Relations, relation)
			if seenTags[tag.ID] {
				continue
			}
			seenTags[tag.ID] = true
			for _, field := range []string{"name", "color", "sort_order"} {
				proof, err := readKnowledgeDisplayOrigin(tx, "tag-"+field, tag.ID, tag.TenantID, tag.KnowledgeBaseID, tagDisplayFieldValue(&tag, field))
				if err != nil {
					return nil, err
				}
				aux.Proofs = append(aux.Proofs, proof)
			}
			// The structural row envelope has no additional text producer. Its
			// name/color/order origins are the separate proofs above, each checked
			// independently so unrelated producers never hit a collection union cap.
			lineage, _ := types.NewCompleteSourceLineage()
			envelopeRevision, err := knowledgeDisplayRevision(tx, "tag-envelope", tag.ID)
			if err != nil {
				return nil, err
			}
			digest, err := KnowledgeInputDigest(&tag)
			if err != nil {
				return nil, err
			}
			aux.Proofs = append(aux.Proofs, &types.KnowledgeCollectionAuxiliaryProof{Kind: "tag-envelope", ID: tag.ID, TenantID: tag.TenantID, KnowledgeBaseID: tag.KnowledgeBaseID, Revision: envelopeRevision, Digest: digest, SourceLineage: lineage})
		}
	}
	sort.Slice(aux.Proofs, func(i, j int) bool {
		a, b := aux.Proofs[i], aux.Proofs[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.ID < b.ID
	})
	sort.Slice(aux.Relations, func(i, j int) bool {
		a, b := aux.Relations[i], aux.Relations[j]
		if a.KnowledgeID != b.KnowledgeID {
			return a.KnowledgeID < b.KnowledgeID
		}
		return a.TagID < b.TagID
	})
	return aux, nil
}

func recheckKnowledgeCollectionAuxiliary(tx *gorm.DB, scopes []types.KnowledgeSearchScope, candidates []*types.Knowledge, expected *types.KnowledgeCollectionAuxiliary) error {
	if expected == nil {
		return types.ErrSourceLineageUnknown
	}
	actual, err := captureKnowledgeCollectionAuxiliary(tx, scopes, candidates)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		return ErrKnowledgeInputChanged
	}
	return nil
}
