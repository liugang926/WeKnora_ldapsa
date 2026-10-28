// nextcloud-event-recovery closes one proven-terminated event sync attempt.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	connectionID := flag.String("connection-id", "", "exact Nextcloud event connection")
	syncLogID := flag.String("sync-log-id", "", "exact running sync log")
	dsnEnv := flag.String("dsn-env", "WEKNORA_POSTGRES_DSN", "environment variable containing the PostgreSQL DSN")
	flag.Parse()
	if *connectionID == "" || *syncLogID == "" {
		exit(errors.New("connection-id and sync-log-id are required"))
	}
	dsn := os.Getenv(*dsnEnv)
	if dsn == "" {
		exit(errors.New("PostgreSQL DSN environment is empty"))
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		exit(errors.New("PostgreSQL connection failed"))
	}
	sqlDB, err := db.DB()
	if err != nil {
		exit(errors.New("PostgreSQL pool unavailable"))
	}
	defer sqlDB.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	recovered, err := repository.NewNextcloudEventInboxRepository(db).
		RecoverQueuedEventSync(ctx, *connectionID, *syncLogID, time.Now().UTC())
	if err != nil {
		exit(err)
	}
	encoded, _ := json.Marshal(map[string]any{
		"connection_id": *connectionID, "sync_log_id": *syncLogID, "recovered": recovered,
	})
	fmt.Println(string(encoded))
}

func exit(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
