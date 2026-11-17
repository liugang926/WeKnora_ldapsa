package datasource

import "github.com/Tencent/WeKnora/internal/types/interfaces"

// WithCronLogRepository installs admission on the cron producer only, during DI
// construction and before Start. Manual, event and recovery log writers are separate.
func (s *Scheduler) WithCronLogRepository(repo interfaces.SyncLogRepository) *Scheduler {
	s.syncLogRepo = repo
	return s
}
