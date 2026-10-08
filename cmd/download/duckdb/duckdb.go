package main

import (
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

// duckdbExtensions is the list of DuckDB extensions required by WeKnora's
// data analysis tool. `spatial` is used for layer metadata (st_read_meta)
// so we can enumerate sheet names from Excel files, while `excel` provides
// the dedicated read_xlsx reader with proper type inference.
var duckdbExtensions = []string{"httpfs", "spatial", "excel"}

func installExtension(ctx context.Context, sqlDB *sql.DB, dir, version, platform, ext string) error {
	archive := filepath.Join(dir, ext+".duckdb_extension.gz")
	path := filepath.Join(dir, ext+".duckdb_extension")
	url := fmt.Sprintf("https://extensions.duckdb.org/%s/%s/%s.duckdb_extension.gz",
		version, platform, ext)
	// DuckDB publishes compressed signed extensions. Its INSTALL path form
	// expects the uncompressed file, so fetch the exact engine version and
	// platform first and decompress locally before installing it.
	cmd := exec.CommandContext(ctx, "curl", "--proto", "=https", "--tlsv1.2",
		"--retry", "5", "--retry-all-errors", "--retry-delay", "2",
		"--fail", "--silent", "--show-error", "--location", "--output", archive, url)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("download %s extension: %w: %s", ext, err, strings.TrimSpace(string(output)))
	}
	compressed, err := os.Open(archive)
	if err != nil {
		return fmt.Errorf("open %s extension archive: %w", ext, err)
	}
	defer compressed.Close()
	reader, err := gzip.NewReader(compressed)
	if err != nil {
		return fmt.Errorf("decompress %s extension: %w", ext, err)
	}
	defer reader.Close()
	uncompressed, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s extension: %w", ext, err)
	}
	if _, err := io.Copy(uncompressed, reader); err != nil {
		uncompressed.Close()
		return fmt.Errorf("unpack %s extension: %w", ext, err)
	}
	if err := uncompressed.Close(); err != nil {
		return fmt.Errorf("close %s extension: %w", ext, err)
	}
	// Keep DuckDB's default signature verification enabled. LOAD rejects a
	// corrupt, unsigned, or wrong-version/platform binary.
	quotedPath := strings.ReplaceAll(path, "'", "''")
	if _, err := sqlDB.ExecContext(ctx, "INSTALL '"+quotedPath+"';"); err != nil {
		return fmt.Errorf("install %s extension: %w", ext, err)
	}
	if _, err := sqlDB.ExecContext(ctx, "LOAD "+ext+";"); err != nil {
		return fmt.Errorf("load %s extension: %w", ext, err)
	}
	return nil
}

func downloadExtensions() {
	ctx := context.Background()

	sqlDB, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		panic(err)
	}
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(1)
	var version, platform string
	if err := sqlDB.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		panic(fmt.Errorf("read DuckDB version: %w", err))
	}
	if err := sqlDB.QueryRowContext(ctx, "PRAGMA platform").Scan(&platform); err != nil {
		panic(fmt.Errorf("read DuckDB platform: %w", err))
	}
	if !strings.HasPrefix(version, "v") || strings.ContainsAny(version+platform, "/\\'") ||
		!strings.HasPrefix(platform, "linux_") {
		panic(fmt.Errorf("unexpected DuckDB extension target %q/%q", version, platform))
	}
	fmt.Printf("Installing signed DuckDB extensions for %s/%s\n", version, platform)
	dir, err := os.MkdirTemp("", "weknora-duckdb-extensions-")
	if err != nil {
		panic(fmt.Errorf("create DuckDB extension directory: %w", err))
	}
	defer os.RemoveAll(dir)

	for _, ext := range duckdbExtensions {
		if err := installExtension(ctx, sqlDB, dir, version, platform, ext); err != nil {
			panic(err)
		}
	}
}

func main() {
	downloadExtensions()
}
