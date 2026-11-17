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
		groups interfaces.GroupAccessService, shares interfaces.KBShareService,
	) (*datasource.Scheduler, error) {
		if err := repository.InstallConnectorScheduleSchema(db); err != nil {
			return nil, err
		}
		// Cron starts before the ingestion store is registered. Its backoff check
		// only needs the database and the existing live group and sharing ACLs.
		inputs := repository.NewKnowledgeInputProvenanceStore(db)
		repository.ConfigureKnowledgeInputActorGroupAccess(inputs, groups)
		repository.ConfigureKnowledgeInputActorShares(inputs, shares)
		return s.WithCronLogRepository(repository.ConnectorScheduleLogs{
			SyncLogRepository: logs, DB: db, ScheduleAuthority: inputs,
		}), nil
	}))
	must(c.Decorate(service.WithConnectorScheduleIdentity))
}
