package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

// collectionTransaction supplies a single coherent database view for every
// candidate, original producer receipt, control checkpoint and selection.
func (s *KnowledgeInputProvenanceStore) collectionTransaction(ctx context.Context, fn func(*gorm.DB) error) error {
	if ctx == nil || s == nil || s.db == nil {
		return types.ErrSourceLineageUnknown
	}
	options := &sql.TxOptions{ReadOnly: true}
	switch s.db.Dialector.Name() {
	case "postgres":
		options.Isolation = sql.LevelRepeatableRead
	case "sqlite":
	default:
		return ErrKnowledgeInputChanged
	}
	if _, nested := s.db.Statement.ConnPool.(gorm.TxCommitter); nested {
		return ErrKnowledgeInputChanged
	}
	return s.db.WithContext(ctx).Transaction(fn, options)
}

func collectionScopes(request types.KnowledgeCollectionReadRequest) ([]types.KnowledgeSearchScope, error) {
	out := append([]types.KnowledgeSearchScope{}, request.Scopes...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].TenantID != out[j].TenantID {
			return out[i].TenantID < out[j].TenantID
		}
		return out[i].KBID < out[j].KBID
	})
	for i, scope := range out {
		if scope.TenantID == 0 || scope.KBID == "" || i > 0 && scope == out[i-1] {
			return nil, types.ErrSourceLineageUnknown
		}
	}
	return out, nil
}

func collectionRevision(tx *gorm.DB, scope types.KnowledgeSearchScope) (uint64, error) {
	var row struct{ Revision uint64 }
	if err := tx.Table("knowledge_collection_scope_versions").Where("tenant_id = ? AND knowledge_base_id = ?", scope.TenantID, scope.KBID).Take(&row).Error; err != nil {
		return 0, err
	}
	if row.Revision == 0 {
		return 0, ErrKnowledgeInputChanged
	}
	return row.Revision, nil
}

func collectionProof(tx *gorm.DB, k *types.Knowledge, a *knowledgeInputAdmission, lineage *types.SourceLineage) (*types.KnowledgeBuildInputProof, error) {
	digest, err := knowledgeMaterialDigest(k)
	if err != nil {
		return nil, err
	}
	revision, err := inputRowRevision(tx, "knowledge", k.ID)
	if err != nil {
		return nil, err
	}
	body, err := KnowledgeInputDigest([]string{a.AdmissionID, a.RootDigest, a.ConfigDigest, digest, fmt.Sprint(revision)})
	if err != nil {
		return nil, err
	}
	return &types.KnowledgeBuildInputProof{Version: 1, TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, AdmissionID: a.AdmissionID, BuildGeneration: a.BuildGeneration, InputDigest: a.InputDigest, ConfigDigest: a.ConfigDigest, BodyDigest: body, ChunkIDs: []string{}, SourceLineage: lineage}, nil
}

// No caller-provided predicate is evaluated here. All candidate bodies start
// from their persisted original admission, including candidates that will not
// be displayed and documents excluded by the subsequent operational filters.
func (s *KnowledgeInputProvenanceStore) CaptureKnowledgeCollectionInputs(ctx context.Context, request types.KnowledgeCollectionReadRequest) (*types.KnowledgeCollectionInputSnapshot, error) {
	scopes, err := collectionScopes(request)
	if err != nil {
		return nil, err
	}
	result := &types.KnowledgeCollectionInputSnapshot{Scopes: []types.KnowledgeCollectionScopeCheckpoint{}, Candidates: []*types.KnowledgeCollectionCandidate{}}
	err = s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		if err := s.checkFAQImportReadSetPlanInTx(tx); err != nil {
			return err
		}
		var candidates []*types.Knowledge
		for _, scope := range scopes {
			var kb types.KnowledgeBase
			if err := tx.Where("id = ? AND tenant_id = ?", scope.KBID, scope.TenantID).Take(&kb).Error; err != nil {
				return err
			}
			configLineage, err := readKnowledgeConfigurationOrigin(tx, &kb)
			if err != nil {
				return err
			}
			configRevision, err := knowledgeConfigurationControlRevision(tx, kb.ID)
			if err != nil {
				return err
			}
			configDigest, err := knowledgeConfigurationOriginDigest(&kb)
			if err != nil {
				return err
			}
			revision, err := collectionRevision(tx, scope)
			if err != nil {
				return err
			}
			result.Scopes = append(result.Scopes, types.KnowledgeCollectionScopeCheckpoint{Scope: scope, Revision: revision, ConfigurationRevision: configRevision, ConfigurationDigest: configDigest, ConfigurationLineage: configLineage})
			// Batches bound SQL result hydration without imposing a permanent KB
			// size ceiling. They all share this transaction's original snapshot.
			last := ""
			for {
				var rows []*types.Knowledge
				q := tx.Where("tenant_id = ? AND knowledge_base_id = ?", scope.TenantID, scope.KBID)
				if last != "" {
					q = q.Where("id > ?", last)
				}
				if err := q.Order("id ASC").Limit(256).Find(&rows).Error; err != nil {
					return err
				}
				for _, k := range rows {
					a, _, lineage, err := s.admissionInTx(tx, k, nil)
					if err != nil {
						return err
					}
					proof, err := collectionProof(tx, k, a, lineage)
					if err != nil {
						return err
					}
					result.Candidates = append(result.Candidates, &types.KnowledgeCollectionCandidate{Knowledge: k, Proof: proof})
					candidates = append(candidates, k)
				}
				if len(rows) < 256 {
					break
				}
				last = rows[len(rows)-1].ID
			}
		}
		var err error
		result.Auxiliary, err = captureKnowledgeCollectionAuxiliary(tx, scopes, candidates)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *KnowledgeInputProvenanceStore) recheckCollectionInTx(tx *gorm.DB, snapshot *types.KnowledgeCollectionInputSnapshot) error {
	if snapshot == nil {
		return types.ErrSourceLineageUnknown
	}
	var scopes []types.KnowledgeSearchScope
	var candidates []*types.Knowledge
	index := 0
	for _, checkpoint := range snapshot.Scopes {
		scope := checkpoint.Scope
		scopes = append(scopes, scope)
		revision, err := collectionRevision(tx, scope)
		if err != nil {
			return err
		}
		if revision != checkpoint.Revision {
			return ErrKnowledgeInputChanged
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", scope.KBID, scope.TenantID).Take(&kb).Error; err != nil {
			return err
		}
		lineage, err := readKnowledgeConfigurationOrigin(tx, &kb)
		if err != nil {
			return err
		}
		configRevision, err := knowledgeConfigurationControlRevision(tx, kb.ID)
		if err != nil {
			return err
		}
		configDigest, err := knowledgeConfigurationOriginDigest(&kb)
		if err != nil {
			return err
		}
		if configRevision != checkpoint.ConfigurationRevision || configDigest != checkpoint.ConfigurationDigest || !reflect.DeepEqual(lineage, checkpoint.ConfigurationLineage) {
			return ErrKnowledgeInputChanged
		}
		last := ""
		for {
			var rows []*types.Knowledge
			q := tx.Where("tenant_id = ? AND knowledge_base_id = ?", scope.TenantID, scope.KBID)
			if last != "" {
				q = q.Where("id > ?", last)
			}
			if err := q.Order("id ASC").Limit(256).Find(&rows).Error; err != nil {
				return err
			}
			for _, current := range rows {
				if index >= len(snapshot.Candidates) {
					return ErrKnowledgeInputChanged
				}
				entry := snapshot.Candidates[index]
				index++
				if entry == nil || entry.Knowledge == nil || entry.Proof == nil || entry.Proof.Projection != "" || len(entry.Proof.ChunkIDs) != 0 {
					return types.ErrSourceLineageUnknown
				}
				old, err := json.Marshal(entry.Knowledge)
				if err != nil {
					return err
				}
				body, err := json.Marshal(current)
				if err != nil {
					return err
				}
				if string(old) != string(body) {
					return ErrKnowledgeInputChanged
				}
				a, _, lineage, err := s.admissionInTx(tx, current, entry.Proof)
				if err != nil {
					return err
				}
				proof, err := collectionProof(tx, current, a, lineage)
				if err != nil {
					return err
				}
				if !reflect.DeepEqual(proof, entry.Proof) {
					return ErrKnowledgeInputChanged
				}
				candidates = append(candidates, current)
			}
			if len(rows) < 256 {
				break
			}
			last = rows[len(rows)-1].ID
		}
	}
	if index != len(snapshot.Candidates) {
		return ErrKnowledgeInputChanged
	}
	if _, err := collectionScopes(types.KnowledgeCollectionReadRequest{Scopes: scopes}); err != nil {
		return err
	}
	return recheckKnowledgeCollectionAuxiliary(tx, scopes, candidates, snapshot.Auxiliary)
}

func (s *KnowledgeInputProvenanceStore) selectCollectionInTx(ctx context.Context, tx *gorm.DB, snapshot *types.KnowledgeCollectionInputSnapshot, request types.KnowledgeCollectionSelectionRequest) (*types.KnowledgeCollectionSelection, error) {
	result := &types.KnowledgeCollectionSelection{Request: request, Knowledge: []*types.Knowledge{}}
	repo := &knowledgeRepository{db: tx}
	var err error
	switch request.Mode {
	case "list":
		if len(snapshot.Scopes) != 1 {
			return nil, types.ErrSourceLineageUnknown
		}
		scope := snapshot.Scopes[0].Scope
		result.Knowledge, result.Total, err = repo.ListPagedKnowledgeByKnowledgeBaseID(ctx, scope.TenantID, scope.KBID, &request.Page, request.Filter)
		result.Page, result.PageSize = request.Page.GetPage(), request.Page.Limit()
	case "folders":
		if len(snapshot.Scopes) != 1 {
			return nil, types.ErrSourceLineageUnknown
		}
		scope := snapshot.Scopes[0].Scope
		var counts []*types.KnowledgeFolderCount
		counts, err = repo.ListKnowledgeFolderCounts(ctx, scope.TenantID, scope.KBID)
		result.Folders = types.BuildKnowledgeFolderTree(counts)
	case "search":
		var scopes []types.KnowledgeSearchScope
		for _, checkpoint := range snapshot.Scopes {
			scopes = append(scopes, checkpoint.Scope)
		}
		result.Knowledge, result.HasMore, result.Total, err = repo.SearchKnowledgeInScopes(ctx, scopes, request.Keyword, request.Offset, request.Limit, request.FileTypes)
	default:
		return nil, types.ErrSourceLineageUnknown
	}
	if err != nil {
		return nil, err
	}
	for _, k := range result.Knowledge {
		// Display producers are separately bound to their original receipts.
		k.KnowledgeBaseName = ""
		// These asynchronous diagnostic producers do not have original material
		// receipts. Their strings must not ride a document's admission.
		k.ErrorMessage = ""
		if k.Type == types.KnowledgeTypeFAQ && len(k.LastFAQImportResult) > 0 && string(k.LastFAQImportResult) != "null" {
			proof, readErr := s.captureFAQImportOutputInTx(tx, types.FAQImportOutputReadRequest{TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, Kind: "result"})
			if readErr != nil {
				return nil, readErr
			}
			result.FAQImportResults = append(result.FAQImportResults, proof)
		} else {
			k.LastFAQImportResult = nil
		}
		if snapshot.Auxiliary != nil {
			if request.Mode == "search" {
				k.KnowledgeBaseName = snapshot.Auxiliary.KnowledgeBaseNames[k.KnowledgeBaseID]
			}
			if request.Mode == "list" {
				k.Tags = snapshot.Auxiliary.Tags[k.ID]
			}
		}
	}
	return result, nil
}

// Select is called only after the request's original candidates have passed
// live authorization. Filtering, total, has_more and folder aggregation run in
// the same local snapshot as the complete membership/material recheck.
func (s *KnowledgeInputProvenanceStore) SelectKnowledgeCollectionInputs(ctx context.Context, snapshot *types.KnowledgeCollectionInputSnapshot, request types.KnowledgeCollectionSelectionRequest) (*types.KnowledgeCollectionSelection, error) {
	var result *types.KnowledgeCollectionSelection
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		if err := s.recheckCollectionInTx(tx, snapshot); err != nil {
			return err
		}
		var err error
		result, err = s.selectCollectionInTx(ctx, tx, snapshot, request)
		return err
	})
	return result, err
}

func (s *KnowledgeInputProvenanceStore) RecheckKnowledgeCollectionInputs(ctx context.Context, snapshot *types.KnowledgeCollectionInputSnapshot, selection *types.KnowledgeCollectionSelection) error {
	return s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		if err := s.recheckCollectionInTx(tx, snapshot); err != nil {
			return err
		}
		if selection == nil {
			return nil
		}
		current, err := s.selectCollectionInTx(ctx, tx, snapshot, selection.Request)
		if err != nil {
			return err
		}
		expected, err := json.Marshal(selection)
		if err != nil {
			return err
		}
		actual, err := json.Marshal(current)
		if err != nil {
			return err
		}
		if string(expected) != string(actual) {
			return ErrKnowledgeInputChanged
		}
		return nil
	})
}

var _ interfaces.KnowledgeCollectionInputReader = (*KnowledgeInputProvenanceStore)(nil)
