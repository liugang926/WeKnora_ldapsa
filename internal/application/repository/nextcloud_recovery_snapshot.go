package repository

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// NextcloudRecoverySnapshot represents nextcloud recovery snapshot repository data.
// Snapshot is a fresh owner observation, not a saved receipt or an actor grant.
// The coordinator must obtain it again immediately before its final gate.
type NextcloudRecoverySnapshot struct {
	SchemaVersion int                               `json:"schema_version"`
	Receipt       NextcloudRecoveryReceipt          `json:"receipt"`
	Sources       []NextcloudRecoverySourceSnapshot `json:"sources"`
	StateSHA256   string                            `json:"state_sha256"`
	ControlState  map[string]any                    `json:"control_state,omitempty"`
}

// NextcloudRecoverySourceSnapshot represents nextcloud recovery source snapshot repository data.
type NextcloudRecoverySourceSnapshot struct {
	Pair               NextcloudRecoveryPair                   `json:"pair"`
	PairSHA256         string                                  `json:"pair_sha256"`
	ScopeState         string                                  `json:"scope_state"`
	SourceStatus       string                                  `json:"source_status"`
	KBEpoch            int64                                   `json:"kb_epoch"`
	RetiredGenerations json.RawMessage                         `json:"retired_generations"`
	Reconciliation     *NextcloudRecoveryReconciliationReceipt `json:"reconciliation"`
	Published          []NextcloudRecoveryPublishedSource      `json:"published"`
}

// NextcloudRecoveryPublishedSource represents nextcloud recovery published source repository data.
type NextcloudRecoveryPublishedSource struct {
	KnowledgeID           string   `json:"knowledge_id"`
	FileID                string   `json:"file_id"`
	ETag                  string   `json:"etag"`
	SourceContentRevision string   `json:"source_content_revision,omitempty"`
	IndexedChunkIDs       []string `json:"indexed_chunk_ids"`
}

// Snapshot returns the snapshot result.
func (s *NextcloudPublicationRecoveryStore) Snapshot(
	ctx context.Context,
	planSHA string,
	inv *NextcloudRecoveryInventory,
	pin string,
) (*NextcloudRecoverySnapshot, error) {
	if inv == nil || !recoveryHash.MatchString(planSHA) || !recoveryHash.MatchString(pin) {
		return nil, ErrNextcloudRecoveryDenied
	}
	result := &NextcloudRecoverySnapshot{
		SchemaVersion: 1,
		Sources:       make([]NextcloudRecoverySourceSnapshot, 0, len(inv.Pairs)),
	}
	e := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := tx.Where("plan_sha256=? AND inventory_sha256=?", planSHA, pin).Take(&result.Receipt).Error; e != nil {
			return ErrNextcloudRecoveryDenied
		}
		if result.Receipt.InstanceID != inv.Anchor.InstanceID || result.Receipt.StreamID != inv.Anchor.StreamID ||
			result.Receipt.CheckpointRecordSHA256 != inv.Anchor.RecordSHA256 ||
			result.Receipt.PairCount != len(inv.Pairs) {
			return ErrNextcloudRecoveryDenied
		}
		var scopes []nextcloudRecoveryScope
		if e := tx.Where("plan_sha256=?", planSHA).Order("operation_id").Find(&scopes).Error; e != nil {
			return e
		}
		var pairs []NextcloudSourcePairing
		if e := recoveryPairCurrentQuery(tx, inv.Anchor.InstanceID).Order("operation_id").Find(&pairs).Error; e != nil {
			return e
		}
		if len(scopes) != len(inv.Pairs) || len(pairs) != len(inv.Pairs) {
			return ErrNextcloudRecoveryDenied
		}

		if tx.Migrator().HasTable(&nextcloudControlRecoveryGate{}) {
			var gate nextcloudControlRecoveryGate
			if err := tx.Where("instance_id=? AND plan_sha256=?", inv.Anchor.InstanceID, planSHA).
				Take(&gate).
				Error; err != nil {
				return ErrNextcloudRecoveryDenied
			}
			controls, err := nextcloudControlMetadataInventory(tx)
			if err != nil {
				return err
			}
			result.ControlState = map[string]any{
				"closed":   gate.Closed,
				"metadata": controls,
				"sha256":   onboardingDigest(controls),
			}
		}

		repo := &knowledgeRepository{db: tx, nextcloudRecoveryProbe: true}
		for i, p := range pairs {
			pair := recoveryPair(p)
			sha, _ := recoveryJSONSHA(pair)
			expected, _ := recoveryJSONSHA(inv.Pairs[i])
			scope := scopes[i]
			if sha != expected || scope.OperationID != p.OperationID || scope.PairSHA256 != sha {
				return ErrNextcloudRecoveryDenied
			}
			now, e := nextcloudLeaseNowMS(tx)
			if e != nil {
				return e
			}
			fence, e := nextcloudLockKBFence(tx, p.TenantID, p.KnowledgeBaseID, now)
			if e != nil {
				return e
			}
			if e = s.checkRecoveryScopeInTx(tx, scope); e != nil {
				return e
			}
			var ds types.DataSource
			if e = tx.Where("id=? AND tenant_id=? AND knowledge_base_id=? AND deleted_at IS NULL", p.DataSourceID, p.TenantID, p.KnowledgeBaseID).
				Take(&ds).
				Error; e != nil {
				return e
			}
			base, configSHA, binding, e := NextcloudEventDataSourceIdentity(ds.Config)
			if e != nil || base != p.BaseURL || configSHA != p.ConfigSHA || binding != p.BindingID {
				return ErrNextcloudRecoveryDenied
			}
			var coverage int64
			if e = tx.Table("nextcloud_content_lease_coverage").
				Where("tenant_id=? AND knowledge_base_id=?", p.TenantID, p.KnowledgeBaseID).
				Count(&coverage).
				Error; e != nil {
				return e
			}
			if coverage != 0 {
				return ErrNextcloudRecoveryDenied
			}
			entry := NextcloudRecoverySourceSnapshot{
				Pair:               pair,
				PairSHA256:         sha,
				ScopeState:         scope.State,
				SourceStatus:       ds.Status,
				KBEpoch:            fence.Epoch,
				RetiredGenerations: json.RawMessage(scope.RetiredGenerationsJSON),
				Published:          []NextcloudRecoveryPublishedSource{},
			}
			if scope.State == "reconciled" {
				if ds.Status != types.DataSourceStatusActive || fence.State != "open" {
					return ErrNextcloudRecoveryDenied
				}
				var reconciled NextcloudRecoveryReconciliationReceipt
				if recoveryStrictJSON([]byte(scope.ReconciliationJSON), &reconciled) != nil ||
					reconciled.ReconciliationSHA256 != scope.ReconciliationSHA256 {
					return ErrNextcloudRecoveryDenied
				}
				entry.Reconciliation = &reconciled
				cfg, e := ds.ParseConfig()
				if e != nil {
					return e
				}
				cfg.Type = types.ConnectorTypeNextcloud
				// Signed live pair acknowledgement, double complete current manifest;
				// a saved successful reconciliation cannot hide subsequent ACL drift.
				remote := nextcloud.SourcePairingCommit{
					OperationID:     p.OperationID,
					InstanceID:      p.InstanceID,
					BindingID:       p.BindingID,
					TenantID:        p.TenantID,
					KnowledgeBaseID: p.KnowledgeBaseID,
					DataSourceID:    p.DataSourceID,
				}
				if e = nextcloud.CommitSourcePairing(ctx, cfg, remote); e != nil {
					return e
				}
				observed, e := nextcloud.InspectRecoveryManifest(ctx, cfg)
				if e != nil {
					return e
				}
				a, _ := recoveryJSONSHA(observed)
				b, _ := recoveryJSONSHA(reconciled.Manifest)
				if a != b {
					return ErrNextcloudRecoveryDenied
				}
				if e = nextcloud.CommitSourcePairing(ctx, cfg, remote); e != nil {
					return e
				}
				var versions []nextcloudSourceVersion
				if e = tx.Where(("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND state='publish" +
					"ed'"), p.TenantID, p.KnowledgeBaseID, p.DataSourceID).
					Order("external_id").
					Find(&versions).
					Error; e != nil {
					return e
				}
				for _, v := range versions {
					k, _, e := repo.resolveNextcloudSourceLineageInTx(
						tx,
						p.TenantID,
						p.KnowledgeBaseID,
						v.CandidateKnowledgeID,
					)
					if e != nil {
						return e
					}
					metadata, e := strictLineageKnowledgeMetadata(k.Metadata)
					if e != nil {
						return e
					}
					var chunks []types.Chunk
					if e = tx.Where(("knowledge_id=? AND tenant_id=? AND knowledge_base_id=? AND is_enabled=? AN" +
						"D status=? AND index_status='ready'"), k.ID, p.TenantID, p.KnowledgeBaseID, true, types.ChunkStatusIndexed).
						Order("id").
						Find(&chunks).
						Error; e != nil {
						return e
					}
					ids := make([]string, 0, len(chunks))
					for _, c := range chunks {
						ids = append(ids, c.ID)
					}
					entry.Published = append(
						entry.Published,
						NextcloudRecoveryPublishedSource{
							KnowledgeID:           k.ID,
							FileID:                metadata["nextcloud_file_id"],
							ETag:                  metadata["nextcloud_etag"],
							IndexedChunkIDs:       ids,
							SourceContentRevision: metadata["nextcloud_content_revision"],
						},
					)
				}
			}
			result.Sources = append(result.Sources, entry)
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	result.StateSHA256, e = recoveryJSONSHA(result.Sources)
	return result, e
}
