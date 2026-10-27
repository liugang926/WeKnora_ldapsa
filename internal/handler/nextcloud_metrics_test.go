package handler

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestRenderNextcloudMetricsUsesStableContract(t *testing.T) {
	snapshot := nextcloudMetricsSnapshot{
		candidates:   map[string]int64{"staging": 3, "published": 4},
		receipts:     map[string]int64{"received": 12},
		stages:       map[string][]int64{"parse": {2, 8, 70}},
		stuck:        map[string]int64{"graph": 2},
		failures:     map[string]int64{"graph_manifest_pending": 1},
		authFailures: map[string]int64{"manager_scope_changed": 2},
		pairings:     map[string]int64{"revoked": 1},
		graphTasks: map[string]int64{
			"pending/unprepared":  5,
			"manifest_unprepared": 5,
		},
		graphOldest: 420,
		datasource:  1,
		mismatch:    2,
	}
	body := renderNextcloudMetrics(snapshot)
	for _, want := range []string{
		`sync_receipt_total{state="received"} 12`,
		"candidate_staging_total 3",
		"candidate_published_total 4",
		`candidate_failed_total{reason="graph_manifest_pending"} 1`,
		"graph_manifest_unprepared_total 5",
		"graph_queue_pending_age_seconds 420",
		"source_stuck_total{stage=\"graph\"} 2",
		"source_stage_duration_seconds_bucket{stage=\"parse\",le=\"5\"} 1",
		"source_stage_duration_seconds_count{stage=\"parse\"} 3",
		`manager_authorization_failure_total{reason="manager_scope_changed"} 2`,
		`binding_stopped_total{reason="revoked"} 1`,
		"configuration_projection_mismatch_total 2",
		`graph_task_total{state="pending",queue_state="unprepared"} 5`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics body does not contain %q:\n%s", want, body)
		}
	}
}

func TestMetricLabelRemovesUnboundedText(t *testing.T) {
	got := metricLabel(" Manager Authorization / source changed! ")
	if got != "manager_authorization_source_changed" {
		t.Fatalf("metricLabel = %q", got)
	}
}

func TestCollectNextcloudMetricsFromBoundedTables(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "metrics.db")), &gorm.Config{})
	require.NoError(t, err)
	for _, statement := range []string{
		`CREATE TABLE nextcloud_source_versions (state TEXT NOT NULL)`,
		`CREATE TABLE nextcloud_event_inbox (state TEXT NOT NULL)`,
		`CREATE TABLE nextcloud_file_states (
			connection_id TEXT NOT NULL,
			file_id TEXT NOT NULL,
			publication_epoch INTEGER NOT NULL,
			status_revision INTEGER NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE nextcloud_graph_tasks (
			state TEXT NOT NULL,
			queue_state TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE data_sources (
			type TEXT NOT NULL,
			status TEXT NOT NULL,
			deleted_at DATETIME
		)`,
		`CREATE TABLE nextcloud_source_pairings (
			state TEXT NOT NULL,
			last_error_code TEXT NOT NULL
		)`,
	} {
		require.NoError(t, db.Exec(statement).Error)
	}
	now := time.Now().UTC()
	payload := `{"sync_state":"failed","publication_state":"building",` +
		`"error_code":"graph_task_manifest_pending","occurred_at":` +
		strconv.FormatInt(now.Unix(), 10) +
		`,"graph":{"state":"blocked","error_code":"graph_configuration_changed","required":true},` +
		`"stages":{"parse":{"state":"succeeded","started_at":1,"completed_at":3},` +
		`"chunk":{"state":"running","started_at":1,"completed_at":null},` +
		`"embedding":{"state":"pending","started_at":null,"completed_at":null},` +
		`"index":{"state":"pending","started_at":null,"completed_at":null}}}`
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_source_versions(state) VALUES ('staging')`).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_inbox(state) VALUES ('received')`).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_file_states
		VALUES ('connection-1','77',1,2,?)`, payload).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_graph_tasks
		VALUES ('pending','unprepared','',?)`, now.Add(-6*time.Minute).Unix()).Error)
	require.NoError(t, db.Exec(`INSERT INTO data_sources VALUES ('nextcloud','error',NULL)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_source_pairings
		VALUES ('stopped','manager_authorization_timeout')`).Error)

	snapshot, err := collectNextcloudMetrics(context.Background(), db, now)
	require.NoError(t, err)
	body := renderNextcloudMetrics(snapshot)
	for _, want := range []string{
		"candidate_staging_total 1",
		`sync_receipt_total{state="received"} 1`,
		"graph_manifest_unprepared_total 1",
		"graph_queue_pending_age_seconds 360",
		"datasource_error_total 1",
		`candidate_failed_total{reason="graph_task_manifest_pending"} 1`,
		"configuration_projection_mismatch_total 1",
		`binding_stopped_total{reason="manager_authorization_timeout"} 1`,
		`manager_authorization_failure_total{reason="manager_authorization_timeout"} 1`,
	} {
		require.Contains(t, body, want)
	}
}
