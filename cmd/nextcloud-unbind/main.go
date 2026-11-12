// nextcloud-unbind is a local, DB-owner maintenance command that revokes the
// NextCloud association of a WeKnora deployment: it soft-deletes every active
// nextcloud data source and clears knowledge_bases.ever_had_nextcloud_source
// so the affected KBs become ordinary native KBs. It performs no deletes of
// knowledge/chunk content and never injects a user principal.
//
// It must run only AFTER migration 000175_nextcloud_unbind (PG) /
// 000092_nextcloud_unbind (SQLite) disarmed the provenance guard, otherwise
// the in-transaction UPDATE fails atomically and nothing is changed.
package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func privatePath(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("absolute private path required")
	}
	clean, e := filepath.EvalSymlinks(path)
	if e != nil || clean != filepath.Clean(path) {
		return errors.New("symlink path refused")
	}
	for _, p := range []string{filepath.Dir(path), path} {
		info, e := os.Lstat(p)
		if e != nil {
			return e
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		expected := os.FileMode(0o600)
		if p == filepath.Dir(path) {
			expected = os.FileMode(0o700)
		}
		if !ok || int(stat.Uid) != os.Geteuid() || info.Mode().Perm() != expected ||
			(p == path && (!info.Mode().IsRegular() || stat.Nlink != 1)) {
			return errors.New("private DB owner file required")
		}
	}
	return nil
}

func privateFile(path string) ([]byte, error) {
	if e := privatePath(path); e != nil {
		return nil, e
	}
	data, e := os.ReadFile(path) // #nosec G304 -- path validated by privatePath
	if e != nil {
		return nil, e
	}
	if len(data) > 16<<20 {
		return nil, errors.New("oversize private input")
	}
	return data, nil
}

const (
	requiredSchemaPG     = 175
	requiredSchemaSQLite = 92
)

func schemaVersion(db *gorm.DB) (int, bool) {
	var rows []struct {
		Version int  `gorm:"column:version"`
		Dirty   bool `gorm:"column:dirty"`
	}
	if e := db.Raw(
		"SELECT version, dirty FROM schema_migrations ORDER BY version DESC LIMIT 1",
	).Scan(&rows).Error; e != nil || len(rows) != 1 {
		return 0, false
	}
	return rows[0].Version, !rows[0].Dirty
}

type counts struct {
	ActiveNextcloudSources int64 `gorm:"column:sources"`
	FlaggedKnowledgeBases  int64 `gorm:"column:kbs"`
}

func survey(db *gorm.DB) (counts, error) {
	var c counts
	e := db.Raw(`SELECT
		(SELECT COUNT(*) FROM data_sources WHERE type='nextcloud' AND deleted_at IS NULL) AS sources,
		(SELECT COUNT(*) FROM knowledge_bases WHERE ever_had_nextcloud_source) AS kbs`).Scan(&c).Error
	return c, e
}

func run() error {
	dbFile := flag.String("sqlite-db", "", "owned stopped SQLite database")
	connectionFile := flag.String("postgres-connection-file", "", "private owner-only PostgreSQL DSN file")
	dryRun := flag.Bool("dry-run", false, "print the planned changes without applying them")
	flag.Parse()
	if (*dbFile == "") == (*connectionFile == "") {
		return errors.New("exactly one owned DB (sqlite-db or postgres-connection-file) required")
	}
	var dialector gorm.Dialector
	isPG := false
	if *dbFile != "" {
		if e := privatePath(*dbFile); e != nil {
			return e
		}
		sourceURL := url.URL{Scheme: "file", Path: *dbFile}
		dialector = sqlite.Open(sourceURL.String() + "?_busy_timeout=5000&_foreign_keys=on")
	} else {
		dsn, e := privateFile(*connectionFile)
		if e != nil {
			return e
		}
		dialector = postgres.Open(strings.TrimSpace(string(dsn)))
		isPG = true
	}
	db, e := gorm.Open(dialector, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if e != nil {
		return fmt.Errorf("open DB: %w", e)
	}
	required := requiredSchemaSQLite
	if isPG {
		required = requiredSchemaPG
	}
	version, clean := schemaVersion(db)
	if !clean || version < required {
		return fmt.Errorf(
			"schema migration %d (nextcloud unbind) must be applied first; current version=%d dirty=%v",
			required, version, !clean,
		)
	}
	before, e := survey(db)
	if e != nil {
		return fmt.Errorf("survey: %w", e)
	}
	fmt.Printf("before: active nextcloud data sources=%d, flagged knowledge bases=%d\n",
		before.ActiveNextcloudSources, before.FlaggedKnowledgeBases)
	if *dryRun {
		fmt.Println("dry-run: no changes applied")
		fmt.Println("plan: soft-delete nextcloud data_sources; clear ever_had_nextcloud_source on knowledge_bases")
		return nil
	}
	return db.Transaction(func(tx *gorm.DB) error {
		res := tx.Exec(
			"UPDATE data_sources SET deleted_at = ?, updated_at = ? WHERE type = 'nextcloud' AND deleted_at IS NULL",
			time.Now(), time.Now(),
		)
		if res.Error != nil {
			return fmt.Errorf("soft-delete nextcloud data sources: %w", res.Error)
		}
		sources := res.RowsAffected
		res = tx.Exec("UPDATE knowledge_bases SET ever_had_nextcloud_source = FALSE WHERE ever_had_nextcloud_source")
		if res.Error != nil {
			return fmt.Errorf("clear knowledge-base provenance flags: %w", res.Error)
		}
		kbs := res.RowsAffected
		after, e := survey(tx)
		if e != nil {
			return fmt.Errorf("post-change survey: %w", e)
		}
		fmt.Printf("applied: soft-deleted %d data source(s), cleared %d knowledge base flag(s)\n", sources, kbs)
		fmt.Printf("after:  active nextcloud data sources=%d, flagged knowledge bases=%d\n",
			after.ActiveNextcloudSources, after.FlaggedKnowledgeBases)
		if after.ActiveNextcloudSources != 0 || after.FlaggedKnowledgeBases != 0 {
			return errors.New("unbind incomplete: rows remain; rolling back")
		}
		fmt.Println("unbind complete; retained knowledge and chunk content is untouched")
		fmt.Println("rollback guidance: restore from the pre-unbind backup (pg_dump) " +
			"if the binding must be re-established")
		return nil
	})
}

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "nextcloud-unbind:", e)
		os.Exit(1)
	}
}
