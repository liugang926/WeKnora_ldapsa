package datasource

import "github.com/Tencent/WeKnora/internal/types"

// NextcloudSyncEnqueueUncertain is a safe, source-independent status for a
// running sync log whose queue enqueue reply was lost. The log remains the
// admission lock until a worker completes or an operator has stronger proof.
const NextcloudSyncEnqueueUncertain = "sync_enqueue_uncertain_review_required"

// NextcloudSyncTaskID is stable for the lifetime of one admitted sync log.
// The log ID is generated before enqueue and survives a lost queue reply.
func NextcloudSyncTaskID(syncLogID string) string {
	return "dssync:" + syncLogID
}

// NextcloudSyncReceiptAccepted re-reads a successful enqueue's durable intent.
// The queue call can return after the five-minute absence proof already failed
// the log; its late success must not be reported as an admitted sync.
func NextcloudSyncReceiptAccepted(log *types.SyncLog, dsID string, tenantID uint64,
	trigger, returnedTaskID string,
) bool {
	if log == nil || log.ID == "" || log.DataSourceID != dsID || log.TenantID != tenantID ||
		log.RecoveryVersion != 1 || log.RecoveryTrigger != trigger ||
		log.QueueTaskID != NextcloudSyncTaskID(log.ID) || returnedTaskID != log.QueueTaskID {
		return false
	}
	switch log.Status {
	case types.SyncLogStatusRunning, types.SyncLogStatusSuccess, types.SyncLogStatusPartial:
		return true
	default:
		return false
	}
}
