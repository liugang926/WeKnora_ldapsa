package repository

import (
	"context"
	"encoding/json"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"sort"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Normal service requests cannot release this gate. A restored database and old successful ACK are
// never an authority to re-enable onboarding or personal source access.
type nextcloudControlRecoveryGate struct {
	InstanceID      string `gorm:"primaryKey"`
	PlanSHA256      string
	InventorySHA256 string
	Closed          bool
	ClosedAt        time.Time
}

func (nextcloudControlRecoveryGate) TableName() string { return "nextcloud_control_recovery_gates" }
func nextcloudControlsClosed(tx *gorm.DB, instance string) (bool, error) {
	if !tx.Migrator().HasTable(&nextcloudControlRecoveryGate{}) {
		return tx.Migrator().HasColumn("sessions", "nextcloud_scoped") || tx.Migrator().HasTable("nextcloud_onboarding_credentials") || tx.Migrator().HasTable("nextcloud_onboarding_operations"), nil
	}
	var row nextcloudControlRecoveryGate
	err := tx.Where("instance_id=?", instance).Take(&row).Error
	if err == gorm.ErrRecordNotFound {
		return false, nil
	}
	return row.Closed, err
}
func (s *NextcloudOnboardingStore) InstanceControlsClosed(ctx context.Context, instance string) (bool, error) {
	return nextcloudControlsClosed(s.db.WithContext(ctx), instance)
}
func sourceControlInstance(tx *gorm.DB, tenant uint64, kb, ds string) (string, error) {
	var pair NextcloudSourcePairing
	err := tx.Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=?", tenant, kb, ds).Take(&pair).Error
	return pair.InstanceID, err
}

func nextcloudMissingControlTables(tx *gorm.DB) []string {
	required := []string{"nextcloud_control_recovery_gates", "nextcloud_source_pairings", "nextcloud_source_versions", "nextcloud_source_revisions", "nextcloud_content_fences", "nextcloud_content_leases", "nextcloud_content_lease_coverage", "nextcloud_recovery_build_leases", "nextcloud_publication_recovery_receipts", "nextcloud_publication_recovery_scopes", "knowledge_bases", "knowledges", "chunks", "data_sources"}
	// The onboarding migration installs this independent session marker. Dropping
	// all onboarding tables cannot disguise a partial current schema as legacy.
	if tx.Migrator().HasColumn("sessions", "nextcloud_scoped") || tx.Migrator().HasTable("nextcloud_onboarding_credentials") || tx.Migrator().HasTable("nextcloud_onboarding_operations") || tx.Migrator().HasTable(&NextcloudTargetClosure{}) || tx.Migrator().HasTable(&NextcloudTenantDeletion{}) {
		required = append(required, "tenants", "nextcloud_onboarding_target_closures", "nextcloud_onboarding_tenant_deletions", "nextcloud_onboarding_credentials", "nextcloud_onboarding_delegations", "nextcloud_onboarding_operations", "nextcloud_source_grants", "nextcloud_kb_blank_witnesses", "nextcloud_root_migrations", "nextcloud_target_migrations", "directory_permission_versions", "directories", "directory_identities", "directory_groups", "directory_group_edges", "directory_group_memberships", "tenant_group_role_grants", "resource_group_grants", "resource_access_policies", "tenant_members", "nextcloud_file_states", "nextcloud_status_outbox", "nextcloud_graph_plans", "nextcloud_graph_tasks", "nextcloud_graph_records", "nextcloud_source_status_epochs")
	}
	missing := []string{}
	for _, table := range required {
		if !tx.Migrator().HasTable(table) {
			missing = append(missing, table)
		}
	}
	sort.Strings(missing)
	return missing
}

func nextcloudControlMetadataInventory(tx *gorm.DB) (map[string]any, error) {
	// Hash every protective/control table, including disabled state and pending
	// operations. No ciphertext, user GUID, paths or graph bodies are exported.
	tables := []string{"tenants", "nextcloud_onboarding_target_closures", "nextcloud_onboarding_tenant_deletions", "nextcloud_onboarding_credentials", "nextcloud_onboarding_delegations", "nextcloud_onboarding_operations", "nextcloud_source_grants", "nextcloud_kb_blank_witnesses", "nextcloud_root_migrations", "nextcloud_target_migrations", "nextcloud_source_status_epochs", "nextcloud_graph_plans", "nextcloud_graph_tasks", "nextcloud_graph_records", "nextcloud_file_states", "nextcloud_status_outbox", "nextcloud_control_recovery_gates", "nextcloud_source_pairings", "nextcloud_source_pairing_aborts", "nextcloud_source_rotations", "nextcloud_source_decommissions", "nextcloud_event_connections", "nextcloud_event_checkpoint", "nextcloud_source_versions", "nextcloud_source_revisions", "nextcloud_source_tombstones", "nextcloud_content_fences", "nextcloud_content_leases", "nextcloud_content_lease_coverage", "nextcloud_recovery_build_leases", "nextcloud_gc_jobs", "nextcloud_resource_refs", "nextcloud_backend_descriptors", "knowledge_bases", "data_sources", "resource_access_policies", "directory_permission_versions", "directories", "directory_identities", "directory_groups", "directory_group_edges", "directory_sync_runs", "directory_group_memberships", "tenant_group_role_grants", "resource_group_grants", "tenant_members"}
	out := map[string]any{"__missing_tables": nextcloudMissingControlTables(tx)}
	for _, table := range tables {
		if !tx.Migrator().HasTable(table) {
			continue
		}
		var rows []map[string]any
		if err := tx.Table(table).Find(&rows).Error; err != nil {
			return nil, err
		}
		hashes := make([]string, 0, len(rows))
		for _, row := range rows {
			raw, err := json.Marshal(row)
			if err != nil {
				return nil, err
			}
			hashes = append(hashes, recoverySHA(raw))
		}
		sort.Strings(hashes)
		out[table] = map[string]any{"rows": len(rows), "sha256": onboardingDigest(hashes)}
	}
	return out, nil
}
func closeNextcloudControlsInTx(tx *gorm.DB, instance, planSHA, inventorySHA string) error {
	if !tx.Migrator().HasTable(&nextcloudControlRecoveryGate{}) {
		return nil
	}
	now := time.Now().UTC()
	if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "instance_id"}}, DoUpdates: clause.Assignments(map[string]any{"closed": true, "plan_sha256": planSHA, "inventory_sha256": inventorySHA, "closed_at": now})}).Create(&nextcloudControlRecoveryGate{InstanceID: instance, PlanSHA256: planSHA, InventorySHA256: inventorySHA, Closed: true, ClosedAt: now}).Error; err != nil {
		return err
	}
	if tx.Migrator().HasTable(&types.NextcloudOnboardingCredential{}) {
		if err := tx.Model(&types.NextcloudOnboardingCredential{}).Where("instance_id=?", instance).Update("enabled", false).Error; err != nil {
			return err
		}
	}
	if tx.Migrator().HasTable(&types.NextcloudSourceGrant{}) {
		if err := tx.Where("operation_id IN (SELECT operation_id FROM nextcloud_onboarding_operations WHERE instance_id=?)", instance).Delete(&types.NextcloudSourceGrant{}).Error; err != nil {
			return err
		}
	}
	return nil
}
func recoveryPairCurrentQuery(tx *gorm.DB, instance string) *gorm.DB {
	q := tx.Where("nextcloud_instance_id=?", instance)
	if tx.Migrator().HasTable(&NextcloudTargetMigration{}) {
		q = q.Where("operation_id NOT IN (SELECT old_operation_id FROM nextcloud_target_migrations)")
	}
	return q
}

// ReopenControls is a private DB-owner maintenance phase. It requires current
// signed NC state for every source and a fresh independently authenticated tail;
// credentials and grants deliberately remain disabled until explicit current
// directory authorization rebuilds them. Old content generations stay retired.
func (s *NextcloudPublicationRecoveryStore) ReopenControls(ctx context.Context, planSHA string, inv *NextcloudRecoveryInventory, pin string, tail *NextcloudRecoveryPlan, expectedControlSHA, actorDir, actorGUID string) (map[string]any, error) {
	if tail == nil || tail.raw == "" || inv == nil || tail.InstanceID != inv.Anchor.InstanceID || tail.StreamID != inv.Anchor.StreamID || !recoveryHash.MatchString(expectedControlSHA) {
		return nil, ErrNextcloudRecoveryDenied
	}
	snapshot, err := s.Snapshot(ctx, planSHA, inv, pin)
	if err != nil {
		return nil, err
	}
	if snapshot.ControlState == nil || snapshot.ControlState["closed"] != true || snapshot.ControlState["sha256"] != expectedControlSHA {
		return nil, ErrNextcloudRecoveryDenied
	}
	if len(snapshot.Sources) != len(inv.Pairs) || len(snapshot.Sources) == 0 {
		return nil, ErrNextcloudRecoveryDenied
	}
	ncControlSHA := ""
	for _, source := range snapshot.Sources {
		if e := requireNextcloudTargetOpenInTx(s.db.WithContext(ctx), source.Pair.TenantID, source.Pair.KnowledgeBaseID); e != nil {
			return nil, ErrNextcloudRecoveryDenied
		}
		if source.ScopeState != "reconciled" || source.SourceStatus != types.DataSourceStatusActive || source.Reconciliation == nil {
			return nil, ErrNextcloudRecoveryDenied
		}
		m := source.Reconciliation.Manifest
		files := map[string]nextcloud.RecoveryManifestFile{}
		for _, f := range m.Files {
			files[strconv.FormatInt(f.FileID, 10)] = f
		}
		if len(files) != len(m.Files) || len(files) != len(source.Published) {
			return nil, ErrNextcloudRecoveryDenied
		}
		var retired []nextcloudRecoveryRetiredGeneration
		if recoveryStrictJSON(source.RetiredGenerations, &retired) != nil {
			return nil, ErrNextcloudRecoveryDenied
		}
		old := map[string]bool{}
		for _, r := range retired {
			old[r.KnowledgeID] = true
		}
		for _, p := range source.Published {
			f, ok := files[p.FileID]
			if !ok || f.ETag != p.ETag || old[p.KnowledgeID] || len(p.IndexedChunkIDs) == 0 {
				return nil, ErrNextcloudRecoveryDenied
			}
			want, e := types.NormalizeNextcloudContentRevision(f.SourceContentRevision)
			got, g := types.NormalizeNextcloudContentRevision(p.SourceContentRevision)
			if e != nil || g != nil || want != got {
				return nil, ErrNextcloudRecoveryDenied
			}
			delete(files, p.FileID)
		}
		if len(files) != 0 {
			return nil, ErrNextcloudRecoveryDenied
		}
		var ds types.DataSource
		if err = s.db.WithContext(ctx).Where("id=? AND tenant_id=? AND deleted_at IS NULL", source.Pair.DataSourceID, source.Pair.TenantID).Take(&ds).Error; err != nil {
			return nil, err
		}
		cfg, e := ds.ParseConfig()
		if e != nil {
			return nil, e
		}
		current, e := nextcloud.InspectRecoveryControls(ctx, cfg)
		if e != nil || current.InstanceID != tail.InstanceID || current.BindingID != source.Pair.BindingID || current.StreamID != tail.StreamID || current.Sequence != tail.ThroughSequence || current.ChainSHA256 != tail.ThroughDatabaseChainSHA256 || current.PublicationEpoch != m.Identity.PublicationEpoch || current.ControlsClosed {
			return nil, ErrNextcloudRecoveryDenied
		}
		if ncControlSHA != "" && ncControlSHA != current.InventorySHA256 {
			return nil, ErrNextcloudRecoveryDenied
		}
		ncControlSHA = current.InventorySHA256
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var gate nextcloudControlRecoveryGate
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("instance_id=? AND plan_sha256=? AND closed=?", tail.InstanceID, planSHA, true).Take(&gate).Error; e != nil {
			return ErrNextcloudRecoveryDenied
		}
		if len(nextcloudMissingControlTables(tx)) != 0 {
			return ErrNextcloudRecoveryDenied
		}
		metadata, e := nextcloudControlMetadataInventory(tx)
		if e != nil {
			return e
		}
		if onboardingDigest(metadata) != expectedControlSHA {
			return ErrNextcloudRecoveryDenied
		}
		// A freshly anchored tail may contain a deletion after the applied
		// checkpoint. Never reopen while its permanent owner facts are absent;
		// the existing closed global gate is retained on this failed transaction.
		if e := requireRecoveryPermanentFactOperationsInTx(tx, tail); e != nil {
			return e
		}
		if e := requireRecoveryPermanentFactsRetainedInTx(tx, tail); e != nil {
			return e
		}
		// A restored directory cache must not qualify the final source-control CAS.
		if tx.Migrator().HasTable(&types.NextcloudOnboardingOperation{}) {
			var ops []types.NextcloudOnboardingOperation
			if e := tx.Where("instance_id=? AND state='active'", tail.InstanceID).Find(&ops).Error; e != nil {
				return e
			}
			for _, op := range ops {
				var c types.NextcloudOnboardingCredential
				if e := tx.Where("id=?", op.CredentialID).Take(&c).Error; e != nil {
					return e
				}
				if _, e := NewNextcloudOnboardingStore(tx).Actor(ctx, c, actorDir, actorGUID); e != nil {
					return e
				}
			}
		}
		q := tx.Model(&nextcloudControlRecoveryGate{}).Where("instance_id=? AND plan_sha256=? AND closed=?", tail.InstanceID, planSHA, true).Update("closed", false)
		if q.Error != nil {
			return q.Error
		}
		if q.RowsAffected != 1 {
			return ErrNextcloudRecoveryDenied
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"schema_version": 1, "instance_id": tail.InstanceID, "controls_reconciled": true, "plan_sha256": planSHA, "external_tail_record_sha256": tail.RecoveryRecordSHA256, "nextcloud_control_inventory_sha256": ncControlSHA, "automation_credentials_reauthorization_required": true, "source_grants_reauthorization_required": true, "ingress_reopen_permitted": false}, nil
}

// ReauthorizeOnboardingCredential is an explicit local operator action after
// control reconciliation; recovery never silently re-enables a disabled token.
func (s *NextcloudPublicationRecoveryStore) ReauthorizeOnboardingCredential(ctx context.Context, planSHA, id, expectedControlSHA, actorDir, actorGUID string) (map[string]any, error) {
	if id == "" || !recoveryHash.MatchString(planSHA) || !recoveryHash.MatchString(expectedControlSHA) {
		return nil, ErrNextcloudRecoveryDenied
	}
	var c types.NextcloudOnboardingCredential
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", id).Take(&c).Error; e != nil {
			return ErrNextcloudRecoveryDenied
		}
		var gate nextcloudControlRecoveryGate
		if e := tx.Where("instance_id=? AND plan_sha256=? AND closed=?", c.InstanceID, planSHA, false).Take(&gate).Error; e != nil {
			return ErrNextcloudRecoveryDenied
		}
		if len(nextcloudMissingControlTables(tx)) != 0 {
			return ErrNextcloudRecoveryDenied
		}
		metadata, e := nextcloudControlMetadataInventory(tx)
		if e != nil {
			return e
		}
		if onboardingDigest(metadata) != expectedControlSHA {
			return ErrNextcloudRecoveryDenied
		}
		if _, e := NewNextcloudOnboardingStore(tx).Actor(ctx, c, actorDir, actorGUID); e != nil {
			return e
		}
		return tx.Model(&types.NextcloudOnboardingCredential{}).Where("id=? AND instance_id=?", id, c.InstanceID).Update("enabled", true).Error
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"schema_version": 1, "instance_id": c.InstanceID, "credential_id": id, "explicitly_reauthorized": true, "source_grants_reauthorization_required": true}, nil
}

func (s *NextcloudOnboardingStore) SourceControlsClosed(ctx context.Context, tenant uint64, kb, ds string) (bool, error) {
	instance, err := sourceControlInstance(s.db.WithContext(ctx), tenant, kb, ds)
	if err != nil {
		return true, err
	}
	return nextcloudControlsClosed(s.db.WithContext(ctx), instance)
}
