package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type nextcloudMetricStatus struct {
	SyncState        string `json:"sync_state"`
	PublicationState string `json:"publication_state"`
	ErrorCode        string `json:"error_code"`
	OccurredAt       int64  `json:"occurred_at"`
	Graph            struct {
		State     string `json:"state"`
		ErrorCode string `json:"error_code"`
		Required  bool   `json:"required"`
	} `json:"graph"`
	Stages map[string]struct {
		State       string `json:"state"`
		StartedAt   *int64 `json:"started_at"`
		CompletedAt *int64 `json:"completed_at"`
	} `json:"stages"`
}

type nextcloudMetricStatusRow struct {
	ConnectionID     string
	FileID           string
	PublicationEpoch int64
	StatusRevision   int64
	PayloadJSON      string
}

type nextcloudMetricsSnapshot struct {
	candidates   map[string]int64
	receipts     map[string]int64
	stages       map[string][]int64
	stuck        map[string]int64
	failures     map[string]int64
	authFailures map[string]int64
	pairings     map[string]int64
	graphTasks   map[string]int64
	graphOldest  int64
	datasource   int64
	mismatch     int64
}

// ServeNextcloudMetrics exposes bounded-cardinality Prometheus text metrics
// for the Nextcloud ingestion path. Query strings, filenames, tenant IDs and
// error messages are never emitted.
func ServeNextcloudMetrics(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		snapshot, err := collectNextcloudMetrics(c.Request.Context(), db, time.Now().UTC())
		if err != nil {
			c.String(http.StatusServiceUnavailable, "nextcloud_metrics_unavailable 1\n")
			return
		}
		c.Data(http.StatusOK, "text/plain; version=0.0.4; charset=utf-8",
			[]byte(renderNextcloudMetrics(snapshot)))
	}
}

func collectNextcloudMetrics(ctx context.Context, db *gorm.DB, now time.Time) (nextcloudMetricsSnapshot, error) {
	out := nextcloudMetricsSnapshot{
		candidates: map[string]int64{}, receipts: map[string]int64{},
		stages: map[string][]int64{}, stuck: map[string]int64{},
		failures: map[string]int64{}, authFailures: map[string]int64{}, pairings: map[string]int64{},
		graphTasks: map[string]int64{},
	}
	var versionCounts []struct {
		State string
		Count int64
	}
	if err := db.WithContext(ctx).Table("nextcloud_source_versions").
		Select("state, count(*) AS count").Group("state").Scan(&versionCounts).Error; err != nil {
		return out, err
	}
	for _, row := range versionCounts {
		out.candidates[row.State] = row.Count
	}

	var receiptCounts []struct {
		State string
		Count int64
	}
	if err := db.WithContext(ctx).Table("nextcloud_event_inbox").
		Select("state, count(*) AS count").Group("state").Scan(&receiptCounts).Error; err != nil {
		return out, err
	}
	for _, row := range receiptCounts {
		out.receipts[row.State] = row.Count
	}

	var statuses []nextcloudMetricStatusRow
	if err := db.WithContext(ctx).Table("nextcloud_file_states").
		Select("connection_id, file_id, publication_epoch, status_revision, payload_json").
		Order("connection_id, file_id, publication_epoch, status_revision DESC").
		Scan(&statuses).Error; err != nil {
		return out, err
	}
	latest := map[string]nextcloudMetricStatusRow{}
	for _, row := range statuses {
		key := row.ConnectionID + "\x00" + row.FileID + "\x00" + strconv.FormatInt(row.PublicationEpoch, 10)
		if _, exists := latest[key]; !exists {
			latest[key] = row
		}
	}
	for _, row := range latest {
		var status nextcloudMetricStatus
		if json.Unmarshal([]byte(row.PayloadJSON), &status) != nil {
			continue
		}
		for stage, value := range status.Stages {
			stage = metricLabel(stage)
			if value.StartedAt != nil && value.CompletedAt != nil && *value.CompletedAt >= *value.StartedAt {
				out.stages[stage] = append(out.stages[stage], *value.CompletedAt-*value.StartedAt)
			}
			if (value.State == "pending" || value.State == "running") &&
				metricStageAge(value.StartedAt, status.OccurredAt, now) > 300 {
				out.stuck[stage]++
			}
		}
		if status.Graph.State == "pending" || status.Graph.State == "running" {
			out.stuck["graph"]++
		}
		if status.Graph.ErrorCode == "graph_task_manifest_pending" ||
			status.Graph.ErrorCode == "graph_task_manifest_unverified" {
			out.failures["graph_manifest_pending"]++
		}
		if status.Graph.ErrorCode == "graph_configuration_changed" {
			out.mismatch++
		}
		reason := status.ErrorCode
		if reason == "" {
			for stage, value := range status.Stages {
				if value.State == "failed" {
					reason = metricLabel(stage) + "_failed"
					break
				}
			}
		}
		if status.SyncState == "failed" || status.PublicationState == "failed" || reason != "" {
			if reason == "" {
				reason = "document_processing_failed"
			}
			out.failures[metricLabel(reason)]++
		}
	}

	var graphTasks []struct {
		State      string
		QueueState string
		CreatedAt  int64
		Count      int64
	}
	if db.WithContext(ctx).Migrator().HasTable("nextcloud_graph_tasks") {
		if err := db.WithContext(ctx).Table("nextcloud_graph_tasks").
			Select("state, queue_state, MIN(created_at) AS created_at, count(*) AS count").
			Where("state IN ?", []string{"pending", "running"}).
			Group("state, queue_state").
			Scan(&graphTasks).Error; err != nil {
			return out, err
		}
	}
	graphManifestUnprepared := int64(0)
	for _, row := range graphTasks {
		key := metricLabel(row.State) + "/" + metricLabel(row.QueueState)
		out.graphTasks[key] += row.Count
		if row.State == "pending" && row.QueueState == "unprepared" {
			if age := now.Unix() - row.CreatedAt; age > out.graphOldest {
				out.graphOldest = age
			}
		}
	}
	if db.WithContext(ctx).Migrator().HasTable("nextcloud_graph_tasks") {
		if err := db.WithContext(ctx).Table("nextcloud_graph_tasks").
			Where("state = 'pending' AND queue_state = 'unprepared' AND payload_json = ''").
			Count(&graphManifestUnprepared).Error; err != nil {
			return out, err
		}
	}
	out.graphTasks["manifest_unprepared"] = graphManifestUnprepared

	if err := db.WithContext(ctx).Table("data_sources").
		Where("type = 'nextcloud' AND status = 'error' AND deleted_at IS NULL").
		Count(&out.datasource).Error; err != nil {
		return out, err
	}
	var pairingRows []struct {
		State         string
		LastErrorCode string
	}
	if err := db.WithContext(ctx).Table("nextcloud_source_pairings").
		Select("state, last_error_code").Scan(&pairingRows).Error; err != nil {
		return out, err
	}
	for _, row := range pairingRows {
		if row.State != "active" {
			reason := row.LastErrorCode
			if reason == "" {
				reason = row.State
			}
			out.pairings[metricLabel(reason)]++
		}
		if strings.Contains(strings.ToLower(row.LastErrorCode), "manager") {
			out.authFailures[metricLabel(row.LastErrorCode)]++
		}
	}
	return out, nil
}

func metricStageAge(started *int64, occurredAt int64, now time.Time) int64 {
	if started != nil {
		return maxInt64(0, now.Unix()-*started)
	}
	if occurredAt > 0 {
		return maxInt64(0, now.Unix()-occurredAt)
	}
	return 0
}

func renderNextcloudMetrics(s nextcloudMetricsSnapshot) string {
	var out strings.Builder
	writeCounter := func(name, help string, values map[string]int64) {
		fmt.Fprintf(&out, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			label := metricLabel(key)
			fmt.Fprintf(&out, "%s{reason=%q} %d\n", name, label, values[key])
		}
	}
	writeGauge := func(name, help string, value int64) {
		fmt.Fprintf(&out, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, value)
	}
	fmt.Fprintf(&out, "# HELP sync_receipt_total Durable Nextcloud event receipts by state.\n")
	fmt.Fprintf(&out, "# TYPE sync_receipt_total counter\n")
	keys := sortedMetricKeys(s.receipts)
	for _, key := range keys {
		fmt.Fprintf(&out, "sync_receipt_total{state=%q} %d\n", metricLabel(key), s.receipts[key])
	}
	writeGauge(
		"candidate_staging_total",
		"Current Nextcloud candidates staged but not published.",
		s.candidates["staging"],
	)
	writeGauge("candidate_published_total", "Current published Nextcloud source versions.", s.candidates["published"])
	writeCounter("candidate_failed_total", "Current Nextcloud failures by static reason.", s.failures)
	writeGauge(
		"graph_manifest_unprepared_total",
		"Graph tasks lacking a durable payload.",
		s.graphTasks["manifest_unprepared"],
	)
	writeGauge("graph_queue_pending_age_seconds", "Age of the oldest unprepared graph task.", s.graphOldest)
	writeGauge("datasource_error_total", "Nextcloud data sources in the global error state.", s.datasource)
	writeCounter("binding_stopped_total", "Stopped Nextcloud bindings by static reason.", s.pairings)
	writeCounter(
		"manager_authorization_failure_total",
		"Manager authorization failures by static reason.",
		s.authFailures,
	)
	writeGauge(
		"configuration_projection_mismatch_total",
		"Current graph configuration projection mismatches.",
		s.mismatch,
	)
	out.WriteString("# HELP graph_task_total Current graph tasks by state and queue state.\n")
	out.WriteString("# TYPE graph_task_total gauge\n")
	for _, key := range sortedMetricKeys(s.graphTasks) {
		if key == "manifest_unprepared" {
			continue
		}
		parts := strings.SplitN(key, "/", 2)
		if len(parts) != 2 {
			continue
		}
		fmt.Fprintf(&out, "graph_task_total{state=%q,queue_state=%q} %d\n",
			parts[0], parts[1], s.graphTasks[key])
	}

	out.WriteString("# HELP source_stage_duration_seconds Completed current-source stage durations.\n")
	out.WriteString("# TYPE source_stage_duration_seconds histogram\n")
	buckets := []int64{1, 5, 15, 60, 300, 900, 3600}
	stageKeys := make([]string, 0, len(s.stages))
	for stage := range s.stages {
		stageKeys = append(stageKeys, stage)
	}
	sort.Strings(stageKeys)
	for _, stage := range stageKeys {
		values := append([]int64(nil), s.stages[stage]...)
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		for _, bucket := range buckets {
			count := 0
			for _, value := range values {
				if value <= bucket {
					count++
				}
			}
			fmt.Fprintf(&out, "source_stage_duration_seconds_bucket{stage=%q,le=%q} %d\n",
				metricLabel(stage), strconv.FormatInt(bucket, 10), count)
		}
		fmt.Fprintf(&out, "source_stage_duration_seconds_bucket{stage=%q,le=\"+Inf\"} %d\n",
			metricLabel(stage), len(values))
		var sum int64
		for _, value := range values {
			sum += value
		}
		fmt.Fprintf(&out, "source_stage_duration_seconds_sum{stage=%q} %d\n", metricLabel(stage), sum)
		fmt.Fprintf(&out, "source_stage_duration_seconds_count{stage=%q} %d\n", metricLabel(stage), len(values))
	}
	out.WriteString("# HELP source_stuck_total Source stages pending beyond five minutes.\n")
	out.WriteString("# TYPE source_stuck_total gauge\n")
	for _, stage := range sortedMetricKeys(s.stuck) {
		fmt.Fprintf(&out, "source_stuck_total{stage=%q} %d\n", metricLabel(stage), s.stuck[stage])
	}
	return out.String()
}

func sortedMetricKeys[V ~int64](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func metricLabel(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "unknown"
	}
	var out strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' || r == ':' {
			out.WriteRune(r)
		} else if out.Len() > 0 && !strings.HasSuffix(out.String(), "_") {
			out.WriteByte('_')
		}
		if out.Len() >= 96 {
			break
		}
	}
	if out.Len() == 0 {
		return "unknown"
	}
	return strings.Trim(out.String(), "_")
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
