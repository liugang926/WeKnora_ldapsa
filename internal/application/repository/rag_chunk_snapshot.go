package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

// RAGChunkInputSnapshot represents ragchunk input snapshot repository data.
// RAGChunkReadRequest identifies server-resolved rows, never supplied text.
// Lease is the original exact read lease held before content hydration.
// ExpectedLineage is required when augmenting an already captured result.
type (
	RAGChunkReadRequest   = types.RAGChunkReadRequest
	RAGChunkInputSnapshot = types.RAGChunkInputSnapshot
)

// OrdinaryRAGInputProofLookup is a trusted server dependency. Implementations
// must read persisted producer provenance in tx and bind it to these exact
// content/metadata rows (including their revision or digest). Request fields,
// channel, absence of a source and current history-KB settings are not proof.
// Missing legacy or orphan derivative provenance remains unknown.
type OrdinaryRAGInputProofLookup interface {
	ReadOrdinaryRAGInputLineage(context.Context, *gorm.DB, *types.Knowledge, *types.Chunk) (*types.SourceLineage, error)
}

// ControlledRAGInputProofLookup represents controlled raginput proof lookup repository data.
type ControlledRAGInputProofLookup interface {
	ReadControlledRAGInputLineage(
		context.Context,
		*gorm.DB,
		*types.Knowledge,
		*types.Chunk,
		*types.SourceLineage,
	) (*types.SourceLineage, error)
}

// RAGChunkSnapshotLookup represents ragchunk snapshot lookup repository data.
type RAGChunkSnapshotLookup = interfaces.RAGChunkSnapshotReader

// RAGChunkSnapshotReader represents ragchunk snapshot reader repository data.
type RAGChunkSnapshotReader struct {
	db       *gorm.DB
	ordinary OrdinaryRAGInputProofLookup
}

// NewRAGChunkSnapshotReader constructs ragchunk snapshot reader.
func NewRAGChunkSnapshotReader(db *gorm.DB, ordinary OrdinaryRAGInputProofLookup) *RAGChunkSnapshotReader {
	return &RAGChunkSnapshotReader{db: db, ordinary: ordinary}
}

// HydrateRAGChunkSnapshots reloads the actual material and original source
// identity in one snapshot. An index result supplies IDs/scores only; its text
// can never receive the current publication's identity through this API.
// The caller still authorizes current grants/publication and verifies leases
// after hydration and before every model/output boundary.
func (r *RAGChunkSnapshotReader) HydrateRAGChunkSnapshots(
	ctx context.Context,
	requests []RAGChunkReadRequest,
) ([]RAGChunkInputSnapshot, error) {
	if ctx == nil || r == nil || r.db == nil {
		return nil, ErrNextcloudLineageSnapshotStore
	}
	if len(requests) > types.SourceLineageMaxSources {
		return nil, types.ErrSourceLineageLimit
	}
	if len(requests) == 0 {
		return []RAGChunkInputSnapshot{}, nil
	}
	options := &sql.TxOptions{ReadOnly: true}
	switch r.db.Name() {
	case "postgres":
		options.Isolation = sql.LevelRepeatableRead
	case "sqlite":
	default:
		return nil, ErrNextcloudLineageSnapshotStore
	}
	// GORM nested Transaction uses a savepoint and ignores TxOptions. A caller's
	// weaker transaction cannot silently remove this reader's snapshot contract.
	if _, nested := r.db.Statement.ConnPool.(gorm.TxCommitter); nested {
		return nil, ErrNextcloudLineageSnapshotStore
	}
	var out []RAGChunkInputSnapshot
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		resolver := &knowledgeRepository{db: tx}
		seen := map[string]bool{}
		for _, request := range requests {
			if request.TenantID == 0 || request.TenantID > math.MaxInt64 || request.KnowledgeBaseID == "" ||
				request.KnowledgeID == "" ||
				(len(request.ChunkIDs) == 0 && !request.MetadataOnly) ||
				(len(request.ChunkIDs) != 0 && request.MetadataOnly) {
				return ErrNextcloudLineageSnapshotDenied
			}
			var knowledge types.Knowledge
			if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", request.KnowledgeID, request.TenantID, request.KnowledgeBaseID).
				Take(&knowledge).
				Error; err != nil {
				return lineageSnapshotReadError(err)
			}
			if !request.MetadataOnly &&
				(knowledge.ParseStatus != types.ParseStatusCompleted || knowledge.EnableStatus != "enabled") {
				return ErrNextcloudLineageSnapshotDenied
			}
			var kb types.KnowledgeBase
			if err := tx.Select("id", "tenant_id", "deleted_at").
				Where("id = ? AND tenant_id = ?", request.KnowledgeBaseID, request.TenantID).
				Take(&kb).
				Error; err != nil {
				return lineageSnapshotReadError(err)
			}
			controlled, err := ragKnowledgeIsControlled(tx, &knowledge)
			if err != nil {
				return err
			}
			var sourceLineage *types.SourceLineage
			if controlled {
				current, identity, err := resolver.resolveNextcloudSourceLineageInTx(
					tx,
					request.TenantID,
					request.KnowledgeBaseID,
					request.KnowledgeID,
				)
				if err != nil {
					return err
				}
				if err := verifyRAGExactLease(tx, request, identity); err != nil {
					return err
				}
				knowledge = *current
				sourceLineage, err = types.NewCompleteSourceLineage(identity)
				if err != nil {
					return err
				}
				if request.ExpectedLineage != nil {
					if request.ExpectedLineage.RequireComplete() != nil {
						return types.ErrSourceLineageUnknown
					}
					originalFound := false
					for _, original := range request.ExpectedLineage.Sources {
						if original.TenantID == identity.TenantID &&
							original.KnowledgeBaseID == identity.KnowledgeBaseID &&
							original.KnowledgeID == identity.KnowledgeID {
							if original != identity {
								return ErrNextcloudLineageSnapshotDenied
							}
							originalFound = true
						}
					}
					if !originalFound {
						return ErrNextcloudLineageSnapshotDenied
					}
				}
			}
			chunkIDs := request.ChunkIDs
			if request.MetadataOnly {
				chunkIDs = []string{""}
			}
			for _, id := range chunkIDs {
				var chunk *types.Chunk
				if !request.MetadataOnly {
					if id == "" || seen[id] {
						return ErrNextcloudLineageSnapshotDenied
					}
					seen[id] = true
					if len(seen) > 4096 {
						return types.ErrSourceLineageLimit
					}
					chunk = &types.Chunk{}
					if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", id, request.TenantID, request.KnowledgeBaseID, request.KnowledgeID).
						Take(chunk).
						Error; err != nil {
						return lineageSnapshotReadError(err)
					}
					if !chunk.IsEnabled || chunk.IndexStatus == "processing" || chunk.IndexStatus == "failed" {
						return ErrNextcloudLineageSnapshotDenied
					}
				}
				lineage := sourceLineage
				materialVerified := false
				if controlled && r.ordinary != nil {
					proof, ok := r.ordinary.(ControlledRAGInputProofLookup)
					if !ok {
						lineage = types.UnknownSourceLineage()
					} else {
						var err error
						lineage, err = proof.ReadControlledRAGInputLineage(ctx, tx, &knowledge, chunk, sourceLineage)
						if err != nil {
							return err
						}
						if lineage == nil {
							lineage = types.UnknownSourceLineage()
						}
						if err := lineage.Validate(); err != nil {
							return err
						}
						materialVerified = chunk != nil && lineage.RequireComplete() == nil
					}
				}
				if !controlled {
					lineage = types.UnknownSourceLineage()
					if r.ordinary != nil {
						var err error
						lineage, err = r.ordinary.ReadOrdinaryRAGInputLineage(ctx, tx, &knowledge, chunk)
						if err != nil {
							return err
						}
						if lineage == nil {
							lineage = types.UnknownSourceLineage()
						}
						if err := lineage.Validate(); err != nil {
							return err
						}
						materialVerified = chunk != nil && lineage.RequireComplete() == nil
					}
				}
				if request.ExpectedLineage != nil {
					var err error
					lineage, err = types.UnionSourceLineage(request.ExpectedLineage, lineage)
					if err != nil {
						return err
					}
				}
				copyLineage, err := types.UnionSourceLineage(lineage)
				if err != nil {
					return err
				}
				copyKnowledge := knowledge
				out = append(
					out,
					RAGChunkInputSnapshot{
						Knowledge:                &copyKnowledge,
						Chunk:                    chunk,
						SourceLineage:            copyLineage,
						ProducerMaterialVerified: materialVerified && copyLineage.RequireComplete() == nil,
					},
				)
			}
		}
		return nil
	}, options)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ragKnowledgeIsControlled(tx *gorm.DB, knowledge *types.Knowledge) (bool, error) {
	var metadata map[string]json.RawMessage
	if len(knowledge.Metadata) != 0 {
		if err := json.Unmarshal(knowledge.Metadata, &metadata); err != nil {
			return false, ErrNextcloudLineageSnapshotDenied
		}
	}
	if knowledge.Channel == types.ConnectorTypeNextcloud || metadata["nextcloud_instance_id"] != nil ||
		metadata["nextcloud_binding_id"] != nil ||
		metadata["nextcloud_file_id"] != nil {
		return true, nil
	}
	// An erased channel/metadata marker must not recertify a retained source row
	// as ordinary. This is exact-document positive evidence, not a never proof.
	var versions, revisions int64
	if err := tx.Table("nextcloud_source_versions").
		Where("candidate_knowledge_id = ?", knowledge.ID).
		Count(&versions).
		Error; err != nil {
		return false, lineageSnapshotReadError(err)
	}
	if err := tx.Table("nextcloud_source_revisions").
		Where("candidate_knowledge_id = ?", knowledge.ID).
		Count(&revisions).
		Error; err != nil {
		return false, lineageSnapshotReadError(err)
	}
	return versions > 0 || revisions > 0, nil
}

func verifyRAGExactLease(tx *gorm.DB, request RAGChunkReadRequest, identity types.SourceIdentity) error {
	lease := request.Lease
	if lease == nil || lease.ID == "" || lease.OwnerID == "" || lease.Kind != string(NextcloudContentReadLease) ||
		lease.Epoch <= 0 ||
		lease.TenantID != request.TenantID ||
		lease.KnowledgeBaseID != request.KnowledgeBaseID ||
		lease.KnowledgeID != request.KnowledgeID ||
		uint64(lease.Epoch) != identity.Revision {
		return ErrNextcloudContentLeaseDenied
	}
	var row nextcloudContentLeaseRow
	if err := tx.Where("lease_id = ?", lease.ID).Take(&row).Error; err != nil {
		return lineageSnapshotReadError(err)
	}
	nowMS, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return fmt.Errorf("%w: lease clock", ErrNextcloudLineageSnapshotStore)
	}
	if row.TenantID != lease.TenantID || row.KnowledgeBaseID != lease.KnowledgeBaseID ||
		row.KnowledgeID != lease.KnowledgeID ||
		row.Kind != string(lease.Kind) ||
		row.OwnerID != lease.OwnerID ||
		row.Epoch != lease.Epoch ||
		row.ReleasedAtMS != nil ||
		row.ExpiresAtMS <= nowMS {
		return ErrNextcloudContentLeaseDenied
	}
	return nil
}

var _ RAGChunkSnapshotLookup = (*RAGChunkSnapshotReader)(nil)
