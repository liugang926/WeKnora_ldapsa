// original-body-retention performs explicit offline initialization or restore
// reconciliation and explicit migrations. It never grants content-lease coverage.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/database"
	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
)

func main() {
	mode := flag.String("mode", "verify", "migrate, init, reconcile, or verify; run with app/readers stopped")
	driver := flag.String("driver", os.Getenv("DB_DRIVER"), "postgres or sqlite")
	flag.Parse()
	dsn := os.Getenv("WEKNORA_ORIGINAL_BODY_MAINTENANCE_DSN")
	if dsn == "" {
		fail("WEKNORA_ORIGINAL_BODY_MAINTENANCE_DSN required")
	}
	if *mode == "migrate" {
		var e error
		switch *driver {
		case "postgres":
			e = database.RunMigrations(dsn)
		case "sqlite":
			sqlite_vec.Auto()
			e = database.RunMigrationsWithOptions("sqlite3://"+dsn, database.MigrationOptions{SQLiteDBPath: dsn})
		default:
			fail("driver must be postgres or sqlite")
		}
		if e != nil {
			fail("actual database migration failed; preserve the migration failure state")
		}
		fmt.Println("original body database migration complete")
		return
	}
	var dialect gorm.Dialector
	switch *driver {
	case "postgres":
		dialect = postgres.Open(dsn)
	case "sqlite":
		dialect = sqlite.Open(dsn)
	default:
		fail("driver must be postgres or sqlite")
	}
	db, e := gorm.Open(dialect, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if e != nil {
		fail("database connection unavailable")
	}
	sql, e := db.DB()
	if e != nil {
		fail("database handle unavailable")
	}
	defer sql.Close()
	sql.SetMaxOpenConns(1)
	if !db.Migrator().HasTable("original_body_journal_heads") {
		fail("original body migration required")
	}
	if *mode != "verify" {
		if e = repository.InstallOriginalBodyRetentionTriggers(db); e != nil {
			fail("exact body rewrite protocol unavailable")
		}
	}
	store := repository.NewOriginalBodyRetentionStore(db)
	count := 0
	switch *mode {
	case "init":
		e = store.InitializeJournal()
	case "reconcile":
		count, e = store.ReconcileExternalPurge(context.Background())
	case "verify":
		e = store.VerifyJournalReady()
	default:
		fail("unknown mode")
	}
	if e != nil {
		fail("original body maintenance refused; preserve the database and external anchor for diagnosis")
	}
	fmt.Printf("original body %s complete; replayed=%d\n", *mode, count)
}
func fail(message string) { fmt.Fprintln(os.Stderr, message); os.Exit(1) }
