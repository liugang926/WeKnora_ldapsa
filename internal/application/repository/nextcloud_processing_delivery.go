package repository

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	nextcloudconnector "github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SignedFileStateV2 returns the signed file state v2 result.
func (r *NextcloudEventInboxRepository) SignedFileStateV2(
	ctx context.Context,
	request SignedNextcloudFileStatusRequest,
) ([]byte, string, error) {
	var conn nextcloudEventConnection
	if err := r.db.WithContext(ctx).
		Table("nextcloud_event_connections").
		Where("connection_id=? AND status='active'", request.ConnectionID).
		Take(&conn).
		Error; err != nil {
		return nil, "", ErrNextcloudEventUnauthorized
	}
	secret, err := nextcloudEventResponseSecret(conn, request.KeyID)
	if err != nil {
		return nil, "", err
	}
	mac := hmac.New(sha256.New, NextcloudStateSigningKey(secret))
	mac.Write(request.CanonicalRequest)
	if !hmac.Equal(mac.Sum(nil), request.Signature) {
		return nil, "", ErrNextcloudEventUnauthorized
	}
	if err := NewNextcloudProcessingStateStore(r.db).RefreshSourceEpoch(ctx, conn.ConnectionID); err != nil {
		return nil, "", err
	}
	var body []byte
	var responseSignature string
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var conn nextcloudEventConnection
		if err := tx.Table("nextcloud_event_connections").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("connection_id=? AND status='active'", request.ConnectionID).
			Take(&conn).
			Error; err != nil {
			return ErrNextcloudEventUnauthorized
		}
		secret, err := nextcloudEventResponseSecret(conn, request.KeyID)
		if err != nil {
			return err
		}
		key := NextcloudStateSigningKey(secret)
		mac := hmac.New(sha256.New, key)
		mac.Write(request.CanonicalRequest)
		if !hmac.Equal(mac.Sum(nil), request.Signature) {
			return ErrNextcloudEventUnauthorized
		}
		if err := checkNextcloudEventDataSource(tx, conn); err != nil {
			return err
		}
		if err := tx.Table("nextcloud_event_nonces").
			Where("connection_id=? AND expires_at<?", conn.ConnectionID, request.Now).
			Delete(nil).
			Error; err != nil {
			return err
		}
		insert := tx.Table("nextcloud_event_nonces").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(map[string]any{"connection_id": conn.ConnectionID, "key_id": request.KeyID, "nonce": request.Nonce, "expires_at": request.Now.Add(601 * time.Second)})
		if insert.Error != nil {
			return insert.Error
		}
		if insert.RowsAffected != 1 {
			return ErrNextcloudEventUnauthorized
		}
		file := strconv.FormatInt(request.FileID, 10)
		external := "nextcloud:" + conn.NextcloudInstanceID + ":" + file
		if err := captureNextcloudStateInTx(
			ctx,
			tx,
			conn.TenantID,
			conn.KnowledgeBaseID,
			conn.DatasourceID,
			external,
			true,
		); err != nil {
			return err
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND state='active'", conn.TenantID, conn.KnowledgeBaseID, conn.DatasourceID).
			Take(&pair).
			Error; err != nil {
			return err
		}
		var epoch struct{ PublicationEpoch int64 }
		if err := tx.Table("nextcloud_source_status_epochs").
			Where("connection_id=?", conn.ConnectionID).
			Take(&epoch).
			Error; err != nil {
			return err
		}
		var row nextcloudFileStateRow
		if err := tx.Where("connection_id=? AND file_id=? AND publication_epoch=?", conn.ConnectionID, file, epoch.PublicationEpoch).
			Take(&row).
			Error; err != nil {
			return err
		}
		var state NextcloudFileStateV2
		if err := json.Unmarshal([]byte(row.PayloadJSON), &state); err != nil {
			return err
		}
		// Each authenticated round trip has its own event ID. Reusing an outbox ID
		// while changing freshness would violate inbox event/body immutability.
		state.EventID = uuid.NewString()
		body, err = json.Marshal(state)
		if err != nil {
			return err
		}
		canonical := NextcloudFileStateResponseCanonical(request, body)
		mac = hmac.New(sha256.New, key)
		mac.Write(canonical)
		responseSignature = hex.EncodeToString(mac.Sum(nil))
		return nil
	})
	return body, responseSignature, err
}

// NextcloudStatusDelivery represents nextcloud status delivery repository data.
type NextcloudStatusDelivery struct {
	ConfigSHA, SecretCiphertext  string
	State                        NextcloudFileStateV2
	EventID, ConnectionID, KeyID string
	Config                       *types.DataSourceConfig
	SigningKey                   []byte
	Body                         []byte
}

// DueDeliveries returns the due deliveries result.
func (s *NextcloudProcessingStateStore) DueDeliveries(ctx context.Context, now time.Time) ([]string, error) {
	// Supersession is durable and local. A lost ACK for an older heartbeat need
	// not hold newer verified snapshots or real stage changes behind retries.
	if err := s.db.WithContext(ctx).
		Exec(("UPDATE nextcloud_status_outbox SET state='superseded' WHERE state='pending" +
			"' AND EXISTS(SELECT 1 FROM nextcloud_file_states f WHERE f.connection_id=n" +
			"extcloud_status_outbox.connection_id AND f.file_id=nextcloud_status_outbox" +
			".file_id AND f.publication_epoch=nextcloud_status_outbox.publication_epoch" +
			" AND f.latest_event_id<>nextcloud_status_outbox.event_id)")).
		Error; err != nil {
		return nil, err
	}
	if err := s.db.WithContext(ctx).
		Exec(("UPDATE nextcloud_status_outbox SET state='superseded' WHERE state='pending" +
			"' AND EXISTS(SELECT 1 FROM nextcloud_source_status_epochs e WHERE e.connec" +
			"tion_id=nextcloud_status_outbox.connection_id AND e.publication_epoch>next" +
			"cloud_status_outbox.publication_epoch)")).
		Error; err != nil {
		return nil, err
	}
	var ids []string
	err := s.db.WithContext(ctx).
		Model(&nextcloudStatusOutboxRow{}).
		Where("state='pending' AND next_attempt_at<=?", now.Unix()).
		Order("priority DESC,created_at,event_id").
		Limit(500).
		Pluck("event_id", &ids).
		Error
	return ids, err
}

// PrepareDelivery prepares delivery.
func (s *NextcloudProcessingStateStore) PrepareDelivery(
	ctx context.Context,
	event string,
) (NextcloudStatusDelivery, error) {
	var delivery NextcloudStatusDelivery
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var out nextcloudStatusOutboxRow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("event_id=? AND state='pending'", event).
			Take(&out).
			Error; err != nil {
			return err
		}
		var conn nextcloudEventConnection
		if err := tx.Table("nextcloud_event_connections").
			Where("connection_id=? AND status='active'", out.ConnectionID).
			Take(&conn).
			Error; err != nil {
			return ErrNextcloudEventScope
		}
		if err := checkNextcloudEventDataSource(tx, conn); err != nil {
			return err
		}
		var current nextcloudFileStateRow
		if err := tx.Where("connection_id=? AND file_id=? AND publication_epoch=?", out.ConnectionID, out.FileID, out.PublicationEpoch).
			Take(&current).
			Error; err != nil {
			return err
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND state='active'", conn.TenantID, conn.KnowledgeBaseID, conn.DatasourceID).
			Take(&pair).
			Error; err != nil {
			return err
		}
		var epoch struct{ PublicationEpoch int64 }
		if err := tx.Table("nextcloud_source_status_epochs").
			Where("connection_id=?", conn.ConnectionID).
			Take(&epoch).
			Error; err != nil {
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			epoch.PublicationEpoch = pair.PublicationEpoch
		}
		var version nextcloudSourceVersion
		if err := nextcloudVersionQuery(
			tx,
			conn.TenantID,
			conn.KnowledgeBaseID,
			conn.DatasourceID,
			"nextcloud:"+conn.NextcloudInstanceID+":"+out.FileID,
		).Take(&version).
			Error; err != nil {
			return err
		}
		var state NextcloudFileStateV2
		if err := json.Unmarshal([]byte(out.PayloadJSON), &state); err != nil {
			return err
		}
		if current.LatestEventID != out.EventID || current.HeadRevision != out.HeadRevision ||
			current.StatusRevision != out.StatusRevision ||
			epoch.PublicationEpoch != out.PublicationEpoch ||
			state.PairOperationID != pair.OperationID ||
			state.SourceETag != version.DesiredETag ||
			state.SourceContentRevision != func() string {
				revision, _ := types.NormalizeNextcloudContentRevision(version.DesiredContentRevision)
				return revision
			}() ||
			state.KnowledgeID != nil && *state.KnowledgeID != version.CandidateKnowledgeID {
			if err := tx.Model(&out).
				Updates(map[string]any{"state": "superseded", "last_error_code": "state_head_superseded"}).
				Error; err != nil {
				return err
			}
			return nil
		}
		var ds types.DataSource
		if err := tx.Where("id=? AND tenant_id=?", conn.DatasourceID, conn.TenantID).Take(&ds).Error; err != nil {
			return err
		}
		cfg, err := ds.ParseConfig()
		if err != nil {
			return err
		}
		secret, err := nextcloudEventResponseSecret(conn, conn.CurrentKeyID)
		if err != nil {
			return err
		}
		delivery = NextcloudStatusDelivery{
			EventID:      event,
			ConnectionID: conn.ConnectionID,
			KeyID:        conn.CurrentKeyID,
			Config:       cfg,
			SigningKey:   NextcloudStateSigningKey(secret),
			Body:         []byte(out.PayloadJSON),
		}
		return nil
	})
	return delivery, err
}

// FinishDelivery finishes delivery.
func (s *NextcloudProcessingStateStore) FinishDelivery(
	ctx context.Context,
	event string,
	acked bool,
	now time.Time,
) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var out nextcloudStatusOutboxRow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("event_id=? AND state='pending'", event).
			Take(&out).
			Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		var current nextcloudFileStateRow
		if err := tx.Where("connection_id=? AND file_id=? AND publication_epoch=?", out.ConnectionID, out.FileID, out.PublicationEpoch).
			Take(&current).
			Error; err != nil {
			return err
		}
		if current.HeadRevision != out.HeadRevision || current.StatusRevision != out.StatusRevision {
			return tx.Model(&out).Update("state", "superseded").Error
		}
		if acked {
			return tx.Model(&out).
				Updates(map[string]any{"state": "delivered", "delivered_at": now.Unix(), "last_error_code": ""}).
				Error
		}
		attempts := out.Attempts + 1
		delay := int64(5)
		for i := 1; i < attempts && delay < 300; i++ {
			delay *= 2
		}
		if delay > 300 {
			delay = 300
		}
		return tx.Model(&out).
			Updates(map[string]any{"attempts": attempts, "next_attempt_at": now.Unix() + delay, "last_error_code": "state_delivery_failed"}).
			Error
	})
}

// RefreshSourceEpoch verifies the signed live NC binding before advancing the
// source status generation. An old pairing receipt cannot restamp a resumed root.
func (s *NextcloudProcessingStateStore) RefreshSourceEpoch(ctx context.Context, connectionID string) error {
	var conn nextcloudEventConnection
	if err := s.db.WithContext(ctx).
		Table("nextcloud_event_connections").
		Where("connection_id=? AND status='active'", connectionID).
		Take(&conn).
		Error; err != nil {
		return err
	}
	var ds types.DataSource
	if err := s.db.WithContext(ctx).
		Where("id=? AND tenant_id=?", conn.DatasourceID, conn.TenantID).
		Take(&ds).
		Error; err != nil {
		return err
	}
	cfg, err := ds.ParseConfig()
	if err != nil {
		return err
	}
	cfg.Type = types.ConnectorTypeNextcloud
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	identity, err := nextcloudconnector.InspectPairing(rctx, cfg)
	if err != nil {
		return err
	}
	if identity.InstanceID != conn.NextcloudInstanceID || identity.BindingID != conn.BindingID ||
		identity.BaseURL != conn.DatasourceBaseURL ||
		identity.PublicationEpoch < 0 {
		return ErrNextcloudEventScope
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := checkNextcloudEventDataSource(tx, conn); err != nil {
			return err
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND state='active'", conn.TenantID, conn.KnowledgeBaseID, conn.DatasourceID).
			Take(&pair).
			Error; err != nil {
			return err
		}
		var old struct {
			PublicationEpoch int64
			PairOperationID  string
		}
		err := tx.Table("nextcloud_source_status_epochs").Where("connection_id=?", connectionID).Take(&old).Error
		if err == nil && (old.PairOperationID != pair.OperationID || old.PublicationEpoch > identity.PublicationEpoch) {
			return ErrNextcloudEventScope
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		return tx.Table("nextcloud_source_status_epochs").
			Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "connection_id"}}, DoUpdates: clause.AssignmentColumns([]string{"publication_epoch", "pair_operation_id", "verified_at"})}).
			Create(map[string]any{"connection_id": connectionID, "publication_epoch": identity.PublicationEpoch, "pair_operation_id": pair.OperationID, "verified_at": time.Now().UTC().Unix()}).
			Error
	})
}

// RefreshSources returns the refresh sources result.
func (s *NextcloudProcessingStateStore) RefreshSources(ctx context.Context) error {
	var ids []string
	if err := s.db.WithContext(ctx).
		Table("nextcloud_event_connections").
		Where("status='active'").
		Pluck("connection_id", &ids).
		Error; err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.RefreshSourceEpoch(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// NextcloudFileStateResponseCanonical returns the nextcloud file state response canonical result.
func NextcloudFileStateResponseCanonical(request SignedNextcloudFileStatusRequest, body []byte) []byte {
	sum := sha256.Sum256(body)
	return []byte(
		"weknora-file-status-hmac-sha256-v2\n" + request.RequestSignature + "\n" + hex.EncodeToString(
			sum[:],
		) + "\n" + request.ConnectionID + "\n" + request.KeyID + "\n" + request.Nonce,
	)
}

// PrepareDeliveryBatch amortizes connection/pair/credential validation while
// retaining one exact current-head check immediately before each HTTP send.
// Receiver and ACK checks remain independent of this preparation snapshot.
func (s *NextcloudProcessingStateStore) PrepareDeliveryBatch(
	ctx context.Context,
	ids []string,
) ([]NextcloudStatusDelivery, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var result []NextcloudStatusDelivery
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rows []nextcloudStatusOutboxRow
		if err := tx.Where("event_id IN ? AND state='pending'", ids).Find(&rows).Error; err != nil {
			return err
		}
		scopes := map[string]NextcloudStatusDelivery{}
		badScopes := map[string]bool{}
		retryIDs := []string{}
		for _, out := range rows {
			if badScopes[out.ConnectionID] {
				retryIDs = append(retryIDs, out.EventID)
				continue
			}
			scope, ok := scopes[out.ConnectionID]
			if !ok {
				var conn nextcloudEventConnection
				if err := tx.Table("nextcloud_event_connections").
					Where("connection_id=? AND status='active'", out.ConnectionID).
					Take(&conn).
					Error; err != nil {
					badScopes[out.ConnectionID] = true
					retryIDs = append(retryIDs, out.EventID)
					continue
				}
				if err := checkNextcloudEventDataSource(tx, conn); err != nil {
					badScopes[out.ConnectionID] = true
					retryIDs = append(retryIDs, out.EventID)
					continue
				}
				var ds types.DataSource
				if err := tx.Where("id=? AND tenant_id=?", conn.DatasourceID, conn.TenantID).
					Take(&ds).
					Error; err != nil {
					return err
				}
				cfg, err := ds.ParseConfig()
				if err != nil {
					return err
				}
				secret, err := nextcloudEventResponseSecret(conn, conn.CurrentKeyID)
				if err != nil {
					return err
				}
				scope = NextcloudStatusDelivery{
					ConnectionID:     conn.ConnectionID,
					KeyID:            conn.CurrentKeyID,
					Config:           cfg,
					SigningKey:       NextcloudStateSigningKey(secret),
					ConfigSHA:        conn.DatasourceConfigSHA256,
					SecretCiphertext: conn.CurrentSecretCiphertext,
				}
				scopes[out.ConnectionID] = scope
			}
			var state NextcloudFileStateV2
			if err := json.Unmarshal([]byte(out.PayloadJSON), &state); err != nil {
				return err
			}
			scope.EventID = out.EventID
			scope.Body = []byte(out.PayloadJSON)
			scope.State = state
			result = append(result, scope)
		}
		if len(retryIDs) > 0 {
			return tx.Model(&nextcloudStatusOutboxRow{}).
				Where("event_id IN ? AND state='pending'", retryIDs).
				Updates(map[string]any{"next_attempt_at": time.Now().UTC().Add(5 * time.Second).Unix(), "attempts": gorm.Expr("attempts+1"), "last_error_code": "state_connection_unavailable"}).
				Error
		}
		return nil
	})
	return result, err
}

// DeliveryCurrent returns the delivery current result.
func (s *NextcloudProcessingStateStore) DeliveryCurrent(
	ctx context.Context,
	delivery NextcloudStatusDelivery,
) (bool, error) {
	state := delivery.State
	if state.PublicationState == "published" {
		closed, err := nextcloudControlsClosed(s.db.WithContext(ctx), state.InstanceID)
		if err != nil || closed {
			return false, err
		}
	}
	knowledge := ""
	if state.KnowledgeID != nil {
		knowledge = *state.KnowledgeID
	}
	var count int64
	query := s.db.WithContext(ctx).
		Table("nextcloud_status_outbox o").
		Joins(("JOIN nextcloud_file_states f ON f.connection_id=o.connection_id AND f.file" +
			"_id=o.file_id AND f.publication_epoch=o.publication_epoch AND f.latest_eve" +
			"nt_id=o.event_id AND f.head_revision=o.head_revision AND f.status_revision" +
			"=o.status_revision")).
		Joins("JOIN nextcloud_event_connections c ON c.connection_id=o.connection_id AND c.status='active'").
		Joins(("JOIN data_sources d ON d.id=c.datasource_id AND d.tenant_id=c.tenant_id AN" +
			"D d.knowledge_base_id=c.knowledge_base_id AND d.type='nextcloud' AND d.sta" +
			"tus='active' AND d.deleted_at IS NULL")).
		Joins(("JOIN nextcloud_source_pairings p ON p.tenant_id=c.tenant_id AND p.knowledg" +
			"e_base_id=c.knowledge_base_id AND p.datasource_id=c.datasource_id AND p.st" +
			"ate='active'")).
		Joins("LEFT JOIN nextcloud_source_status_epochs e ON e.connection_id=c.connection_id").
		Joins(("JOIN nextcloud_source_versions v ON v.tenant_id=c.tenant_id AND v.knowledg" +
			"e_base_id=c.knowledge_base_id AND v.datasource_id=c.datasource_id AND v.ex" +
			"ternal_id=('nextcloud:' || c.nextcloud_instance_id || ':' || o.file_id)")).
		Where(("o.event_id=? AND o.state='pending' AND c.current_key_id=? AND c.current_se" +
			"cret_ciphertext=? AND c.datasource_config_sha256=? AND p.operation_id=? AN" +
			"D COALESCE(e.publication_epoch,p.publication_epoch)=o.publication_epoch AN" +
			"D v.desired_etag=? AND COALESCE(v.candidate_knowledge_id,'')=?"), delivery.EventID, delivery.KeyID, delivery.SecretCiphertext, delivery.ConfigSHA, state.PairOperationID, state.SourceETag, knowledge)
	if s.contentRevisionSupported {
		query = query.Where("v.desired_content_revision=?", state.SourceContentRevision)
	} else if state.SourceContentRevision != "0" {
		return false, nil
	}
	err := query.Count(&count).Error
	return count == 1, err
}

// AcknowledgeCurrentDelivery returns the acknowledge current delivery result.
func (s *NextcloudProcessingStateStore) AcknowledgeCurrentDelivery(
	ctx context.Context,
	event string,
	now time.Time,
) error {
	return s.db.WithContext(ctx).
		Exec(("UPDATE nextcloud_status_outbox SET state=CASE WHEN EXISTS(SELECT 1 FROM ne" +
			"xtcloud_file_states f WHERE f.connection_id=nextcloud_status_outbox.connec" +
			"tion_id AND f.file_id=nextcloud_status_outbox.file_id AND f.publication_ep" +
			"och=nextcloud_status_outbox.publication_epoch AND f.latest_event_id=nextcl" +
			"oud_status_outbox.event_id AND f.head_revision=nextcloud_status_outbox.hea" +
			"d_revision AND f.status_revision=nextcloud_status_outbox.status_revision) " +
			"THEN 'delivered' ELSE 'superseded' END,delivered_at=?,last_error_code='' W" +
			"HERE event_id=? AND state='pending'"), now.Unix(), event).
		Error
}

// AcknowledgeCurrentDeliveries returns the acknowledge current deliveries result.
func (s *NextcloudProcessingStateStore) AcknowledgeCurrentDeliveries(
	ctx context.Context,
	events []string,
	now time.Time,
) error {
	if len(events) == 0 {
		return nil
	}
	return s.db.WithContext(ctx).
		Exec(("UPDATE nextcloud_status_outbox SET state=CASE WHEN EXISTS(SELECT 1 FROM ne" +
			"xtcloud_file_states f WHERE f.connection_id=nextcloud_status_outbox.connec" +
			"tion_id AND f.file_id=nextcloud_status_outbox.file_id AND f.publication_ep" +
			"och=nextcloud_status_outbox.publication_epoch AND f.latest_event_id=nextcl" +
			"oud_status_outbox.event_id AND f.head_revision=nextcloud_status_outbox.hea" +
			"d_revision AND f.status_revision=nextcloud_status_outbox.status_revision) " +
			"THEN 'delivered' ELSE 'superseded' END,delivered_at=?,last_error_code='' W" +
			"HERE event_id IN ? AND state='pending'"), now.Unix(), events).
		Error
}
