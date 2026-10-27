package handler

import (
	"strings"
	"testing"
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
