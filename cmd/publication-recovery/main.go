// publication-recovery is a local, DB-owner maintenance command. It has no HTTP
// route and never injects a user principal or converts machine tokens to reads.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
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
		expected := os.FileMode(0600)
		if p == filepath.Dir(path) {
			expected = 0700
		}
		if !ok || int(stat.Uid) != os.Geteuid() || info.Mode().Perm() != expected || (p == path && (!info.Mode().IsRegular() || stat.Nlink != 1)) {
			return errors.New("private DB owner file required")
		}
	}
	return nil
}

func privateFile(path string) ([]byte, error) {
	if e := privatePath(path); e != nil {
		return nil, e
	}
	fd, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	return ioReadBounded(f)
}
func ioReadBounded(f *os.File) ([]byte, error) {
	info, e := f.Stat()
	if e != nil {
		return nil, e
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() || info.Mode().Perm() != 0600 || !info.Mode().IsRegular() || stat.Nlink != 1 || info.Size() > 16<<20 {
		return nil, errors.New("unsafe private input")
	}
	data, e := io.ReadAll(io.LimitReader(f, 16<<20+1))
	if e != nil || len(data) > 16<<20 {
		return nil, errors.New("oversize private input")
	}
	return data, nil
}
func writePrivate(path string, data []byte) error {
	if !filepath.IsAbs(path) {
		return errors.New("absolute private output required")
	}
	dir := filepath.Dir(path)
	resolved, e := filepath.EvalSymlinks(dir)
	if e != nil || resolved != dir {
		return errors.New("output directory symlink refused")
	}
	info, e := os.Stat(dir)
	if e != nil {
		return e
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() || info.Mode().Perm() != 0700 {
		return errors.New("private owned output directory required")
	}
	fd, e := syscall.Open(path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	if _, e = f.Write(data); e != nil {
		return e
	}
	return f.Sync()
}
func run() error {
	mode := flag.String("mode", "", "capture, apply, or reconcile")
	dbFile := flag.String("sqlite-db", "", "owned stopped SQLite database")
	connectionFile := flag.String("postgres-connection-file", "", "private owner-only PostgreSQL DSN file")
	planFile := flag.String("plan", "", "external signed journal replay plan")
	keyFile := flag.String("key-file", "", "private external journal 32-byte key")
	inventoryFile := flag.String("inventory", "", "externally retained backup inventory")
	pin := flag.String("inventory-sha256", "", "independently recorded inventory pin")
	output := flag.String("output", "", "private output file; created exclusively")
	instance := flag.String("instance-id", "", "checkpoint Nextcloud instance")
	stream := flag.String("stream-id", "", "checkpoint external journal stream")
	record := flag.String("checkpoint-record-sha256", "", "external checkpoint record pin")
	sequence := flag.String("checkpoint-sequence", "", "external checkpoint sequence")
	prefix := flag.String("checkpoint-database-chain-sha256", "", "external checkpoint database prefix")
	receipt := flag.String("plan-sha256", "", "closure receipt digest")
	operation := flag.String("pair-operation-id", "", "original persisted pair operation")
	flag.Parse()
	if *output == "" {
		return errors.New("private output file required")
	}
	if (*dbFile == "") == (*connectionFile == "") {
		return errors.New("one owned DB required")
	}
	var dialector gorm.Dialector
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
	}
	db, e := gorm.Open(dialector, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if e != nil {
		return errors.New("maintenance database connection failed")
	}
	sqlDB, e := db.DB()
	if e != nil {
		return e
	}
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(1)
	if *connectionFile != "" {
		var owner bool
		if e = db.Raw("SELECT current_user = pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()").Scan(&owner).Error; e != nil || !owner {
			return errors.New("database owner role required")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	store := repository.NewNextcloudPublicationRecoveryStore(db)
	var value any
	switch *mode {
	case "capture":
		value, e = store.Capture(ctx, repository.NextcloudRecoveryAnchor{InstanceID: *instance, StreamID: *stream, RecordSHA256: *record, Sequence: *sequence, DatabaseChainSHA256: *prefix})
	case "apply":
		key, er := privateFile(*keyFile)
		if er != nil {
			return er
		}
		envelope, er := privateFile(*planFile)
		if er != nil {
			return er
		}
		plan, er := repository.VerifyNextcloudRecoveryPlan(envelope, key)
		if er != nil {
			return er
		}
		raw, er := privateFile(*inventoryFile)
		if er != nil {
			return er
		}
		inventory, er := repository.DecodeNextcloudRecoveryInventory(raw, *pin)
		if er != nil {
			return er
		}
		value, e = store.Apply(ctx, plan, inventory, *pin)
	case "reconcile":
		value, e = store.Reconcile(ctx, *receipt, *operation)
	default:
		return errors.New("capture, apply or reconcile mode required")
	}
	if e != nil {
		return e
	}
	data, e := json.Marshal(value)
	if e != nil {
		return e
	}
	if *output == "" {
		return errors.New("private output file required")
	}
	if e = writePrivate(*output, data); e != nil {
		return e
	}
	hash := sha256.Sum256(data)
	fmt.Println("evidence_sha256=" + hex.EncodeToString(hash[:]))
	return nil
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "Publication recovery refused; inspect retained evidence and maintenance owner context")
		os.Exit(1)
	}
}
