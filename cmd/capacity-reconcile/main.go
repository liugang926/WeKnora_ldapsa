// capacity-reconcile is an owner-only maintenance command. It deletes no file.
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
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
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
			expected = 0o700
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
	fd, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), path)
	// The file is read-only; close cannot affect any persisted output.
	defer func() { _ = f.Close() }()
	return ioReadBounded(f)
}

func ioReadBounded(f *os.File) ([]byte, error) {
	info, e := f.Stat()
	if e != nil {
		return nil, e
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() || info.Mode().Perm() != 0o600 ||
		!info.Mode().IsRegular() || stat.Nlink != 1 || info.Size() > 16<<20 {
		return nil, errors.New("unsafe private input")
	}
	data, e := io.ReadAll(io.LimitReader(f, 16<<20+1))
	if e != nil || len(data) > 16<<20 {
		return nil, errors.New("oversize private input")
	}
	return data, nil
}

func reservePrivate(path string) (*os.File, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("absolute private output required")
	}
	dir := filepath.Dir(path)
	resolved, e := filepath.EvalSymlinks(dir)
	if e != nil || resolved != dir {
		return nil, errors.New("output directory symlink refused")
	}
	info, e := os.Stat(dir)
	if e != nil {
		return nil, e
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() || info.Mode().Perm() != 0o700 {
		return nil, errors.New("private owned output directory required")
	}
	fd, e := syscall.Open(path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if e != nil {
		return nil, e
	}
	return os.NewFile(uintptr(fd), path), nil
}

func run() (resultErr error) {
	mode := flag.String("mode", "", "inspect or reconcile")
	sqlitePath := flag.String("sqlite-db", "", "private owned SQLite DB")
	connection := flag.String("postgres-connection-file", "", "private owner-role DSN")
	token := flag.String("reservation-token", "", "exact persisted reservation token")
	root := flag.String("local-storage-base-dir", "", "original configured local storage base")
	output := flag.String("output", "", "new private output")
	flag.Parse()
	if *token == "" || *output == "" || (*mode != "inspect" && *mode != "reconcile") ||
		(*sqlitePath == "") == (*connection == "") {
		return errors.New("maintenance scope required")
	}
	out, e := reservePrivate(*output)
	if e != nil {
		return e
	}
	complete := false
	outputClosed := false
	defer func() {
		if !outputClosed {
			resultErr = errors.Join(resultErr, out.Close())
		}
		if !complete {
			_ = os.Remove(*output)
		}
	}()
	var dialect gorm.Dialector
	if *sqlitePath != "" {
		if e = privatePath(*sqlitePath); e != nil {
			return e
		}
		u := url.URL{Scheme: "file", Path: *sqlitePath}
		dialect = sqlite.Open(u.String() + "?_busy_timeout=5000&_foreign_keys=on")
	} else {
		b, e := privateFile(*connection)
		if e != nil {
			return e
		}
		dialect = postgres.Open(strings.TrimSpace(string(b)))
	}
	db, e := gorm.Open(dialect, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if e != nil {
		return errors.New("maintenance DB unavailable")
	}
	sql, e := db.DB()
	if e != nil {
		return e
	}
	// Closing the DB pool is cleanup after all checked SQL operations.
	defer func() { _ = sql.Close() }()
	sql.SetMaxOpenConns(2)
	if *connection != "" {
		var owner bool
		if e = db.Raw("SELECT current_user=pg_get_userbyid(datdba) " +
			"FROM pg_database WHERE datname=current_database()").Scan(&owner).Error; e != nil || !owner {
			return errors.New("database owner role required")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var r repository.NextcloudCapacityReservation
	if e = db.WithContext(ctx).Where("token=?", *token).Take(&r).Error; e != nil {
		return e
	}
	store := repository.NewNextcloudCapacityStore(db)
	if *mode == "reconcile" {
		var kb types.KnowledgeBase
		if e = db.WithContext(ctx).Where("id=? AND tenant_id=?", r.KnowledgeBaseID, r.TenantID).
			Take(&kb).Error; e != nil {
			return e
		}
		var tenant types.Tenant
		if e = db.WithContext(ctx).Where("id=?", r.TenantID).Take(&tenant).Error; e != nil {
			return e
		}
		backends := repository.NewStorageBackendRepository(db)
		backend, e := backends.GetByID(ctx, r.TenantID, r.BackendID)
		if e != nil || backend == nil || backend.Provider != "local" ||
			backend.Status != types.StorageBackendStatusActive {
			return repository.ErrNextcloudCapacityUnavailable
		}
		resolver := service.NewStorageBackendService(
			backends, db, service.NewResourceCatalog(repository.NewResourceRepository(db)))
		admission := service.NewNextcloudCapacityAdmission(store,
			func(ctx context.Context, _ *types.KnowledgeBase) (interfaces.FileService, error) {
				fs, provider, e := resolver.ResolveFileService(ctx, &tenant, r.BackendID, "local", *root)
				if e != nil || provider != "local" {
					return nil, repository.ErrNextcloudCapacityUnavailable
				}
				return fs, nil
			})
		if e = admission.Reconcile(ctx, &kb, &r); e != nil {
			return e
		}
	}
	if e = db.WithContext(ctx).Where("token=?", r.Token).Take(&r).Error; e != nil {
		return e
	}
	intent, e := store.Intent(ctx, &r)
	if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
		return e
	}
	if errors.Is(e, gorm.ErrRecordNotFound) && r.ProtocolVersion != 1 {
		return e
	}
	var intentValue *repository.NextcloudCapacityWriteIntent
	if e == nil {
		intentValue = &intent
	}
	value := struct {
		Reservation       repository.NextcloudCapacityReservation  `json:"reservation"`
		Intent            *repository.NextcloudCapacityWriteIntent `json:"intent"`
		DeletionPerformed bool                                     `json:"deletion_performed"`
	}{r, intentValue, false}
	b, e := json.Marshal(value)
	if e != nil {
		return e
	}
	if _, e = out.Write(b); e != nil {
		return e
	}
	if e = out.Sync(); e != nil {
		return e
	}
	e = out.Close()
	outputClosed = true
	if e != nil {
		return e
	}
	complete = true
	digest := sha256.Sum256(b)
	fmt.Println("evidence_sha256=" + hex.EncodeToString(digest[:]))
	return nil
}

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr,
			"Capacity reconciliation unavailable; retained charges and objects require owner review")
		os.Exit(1)
	}
}
