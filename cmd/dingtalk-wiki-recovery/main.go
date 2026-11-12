// dingtalk-wiki-recovery plans one failed document and submits an exact
// maintenance request. Only the application worker can restore its actor.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/common"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	mode := flag.String("mode", "plan", "plan or apply (enqueue only)")
	action := flag.String("action", "", "resume-wiki or reparse")
	tenant := flag.Uint64("tenant-id", 0, "exact tenant")
	kb := flag.String("kb-id", "", "exact knowledge base")
	source := flag.String("source-id", "", "exact DingTalk source")
	id := flag.String("knowledge-id", "", "exact failed document")
	path := flag.String("plan", "", "private plan file (created exclusively in plan mode)")
	flag.Parse()
	if (*mode != "plan" && *mode != "apply") || *tenant == 0 || *kb == "" ||
		*source == "" || *id == "" || *path == "" {
		return errors.New("mode, action, tenant-id, kb-id, source-id, knowledge-id and plan are required")
	}
	dsn := os.Getenv("WEKNORA_POSTGRES_DSN")
	if dsn == "" {
		if os.Getenv("DB_DRIVER") != "postgres" || os.Getenv("RETRIEVE_DRIVER") != "postgres" ||
			os.Getenv("DB_HOST") == "" || os.Getenv("DB_USER") == "" || os.Getenv("DB_NAME") == "" {
			return errors.New("existing PostgreSQL DB_* and RETRIEVE_DRIVER environment required")
		}
		port := os.Getenv("DB_PORT")
		if port == "" {
			port = "5432"
		}
		u := url.URL{
			Scheme: "postgres", Host: net.JoinHostPort(os.Getenv("DB_HOST"), port), Path: os.Getenv("DB_NAME"),
			User: url.UserPassword(os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD")),
		}
		q := u.Query()
		q.Set("sslmode", "disable")
		q.Set("TimeZone", "UTC")
		u.RawQuery = q.Encode()
		dsn = u.String()
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return errors.New("database connection failed")
	}
	pool, err := db.DB()
	if err != nil {
		return errors.New("database unavailable")
	}
	defer func() { _ = pool.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var owner bool
	if err = db.WithContext(ctx).Raw(
		"SELECT r.rolsuper OR pg_has_role(current_user, d.datdba, 'USAGE') FROM pg_roles r " +
			"JOIN pg_database d ON d.datname=current_database() WHERE r.rolname=current_user",
	).Scan(&owner).Error; err != nil || !owner {
		return errors.New("database owner maintenance connection required")
	}
	// Reuse the application's retention read path, including vault-backed
	// fields. Maintenance planning must never install triggers or migrate data.
	if err = repository.NewOriginalBodyRetentionStore(db.WithContext(ctx)).VerifyJournalReady(); err != nil {
		return fmt.Errorf("original body retention readiness: %w", err)
	}
	if err = repository.InstallOriginalBodyRetentionCallbacks(db); err != nil {
		return fmt.Errorf("original body retention callbacks: %w", err)
	}
	fresh, err := service.PlanDingTalkDocumentRecovery(ctx, db, *tenant, *kb, *source, *id, *action)
	if err != nil {
		return err
	}
	if *mode == "plan" {
		data, err := json.MarshalIndent(fresh, "", "  ")
		if err != nil {
			return err
		}
		f, err := os.OpenFile(*path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return errors.New("plan path must be new and private")
		}
		_, err = f.Write(append(data, '\n'))
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"mode": "plan", "knowledge_id": *id, "action": *action, "digest": fresh.Digest,
			"authorization": "worker must recheck original actor before execution",
		})
	}
	info, err := os.Lstat(*path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("plan must be a private regular file")
	}
	data, err := os.ReadFile(*path)
	if err != nil {
		return errors.New("cannot read plan")
	}
	var saved repository.DingTalkRecoveryPlan
	if err = json.Unmarshal(data, &saved); err != nil {
		return errors.New("invalid plan")
	}
	if err = saved.Validate(); err != nil {
		return err
	}
	if saved.Digest != fresh.Digest {
		return errors.New("plan changed; no task submitted")
	}
	if os.Getenv("REDIS_ADDR") == "" {
		return errors.New("REDIS_ADDR required")
	}
	redisDB := 0
	if v := os.Getenv("REDIS_DB"); v != "" {
		redisDB, err = strconv.Atoi(v)
		if err != nil {
			return errors.New("invalid REDIS_DB")
		}
	}
	client := asynq.NewClient(asynq.RedisClientOpt{
		Addr: os.Getenv("REDIS_ADDR"), Username: os.Getenv("REDIS_USERNAME"),
		Password: os.Getenv("REDIS_PASSWORD"), DB: redisDB, TLSConfig: common.RedisTLSConfig(),
	})
	defer func() { _ = client.Close() }()
	taskID := "dingtalk-recovery-" + saved.Digest
	_, err = client.EnqueueContext(
		ctx, asynq.NewTask(repository.DingTalkRecoveryTask, data), asynq.Queue(types.QueueMaintenance),
		asynq.TaskID(taskID), asynq.MaxRetry(3), asynq.Timeout(10*time.Minute), asynq.Retention(7*24*time.Hour),
	)
	if err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) {
		return errors.New("maintenance enqueue failed")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"mode": "apply", "task_id": taskID, "knowledge_id": *id, "action": *action,
		"submitted": true, "completed": false,
	})
}
