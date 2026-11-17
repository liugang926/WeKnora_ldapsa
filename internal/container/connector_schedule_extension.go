package container

import (
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/runtime"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

// Both server and desktop build their providers in the runtime container.
// Register decorators before BuildContainer starts the cron or resolves workers.
func init() {
	c := runtime.GetContainer()
	must(c.Decorate(func(
		s *datasource.Scheduler, logs interfaces.SyncLogRepository, db *gorm.DB,
		inputs *repository.KnowledgeInputProvenanceStore,
	) (*datasource.Scheduler, error) {
		if err := repository.InstallConnectorScheduleSchema(db); err != nil {
			return nil, err
		}
		return s.WithCronLogRepository(repository.ConnectorScheduleLogs{
			SyncLogRepository: logs, DB: db, ScheduleAuthority: inputs,
		}), nil
	}))
	must(c.Decorate(service.WithConnectorScheduleIdentity))
}
