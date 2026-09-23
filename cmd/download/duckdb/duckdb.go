package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
)

// duckdbExtensions is the list of DuckDB extensions required by WeKnora's
// data analysis tool. `spatial` is used for layer metadata (st_read_meta)
// so we can enumerate sheet names from Excel files, while `excel` provides
// the dedicated read_xlsx reader with proper type inference.
var duckdbExtensions = []string{"spatial", "excel"}

func downloadExtensions() {
	ctx := context.Background()

	sqlDB, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		panic(err)
	}
	defer sqlDB.Close()
	// Bootstrap the HTTPS transport from DuckDB's signed core repository.
	// DuckDB cannot fetch HTTPS extension URLs until httpfs is loaded.
	if _, err := sqlDB.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
		panic(fmt.Errorf("install DuckDB HTTPS transport: %w", err))
	}
	// DuckDB's default core repository is HTTP. HTTPS avoids local HTTP
	// proxies returning 502 for signed extension archives during Docker builds.
	if _, err := sqlDB.ExecContext(ctx,
		"SET custom_extension_repository = 'https://extensions.duckdb.org';"); err != nil {
		panic(fmt.Errorf("set DuckDB extension repository: %w", err))
	}

	for _, ext := range duckdbExtensions {
		if _, err := sqlDB.ExecContext(ctx, fmt.Sprintf("INSTALL %s;", ext)); err != nil {
			panic(fmt.Errorf("failed to install %s extension: %w", ext, err))
		}
		if _, err := sqlDB.ExecContext(ctx, fmt.Sprintf("LOAD %s;", ext)); err != nil {
			panic(fmt.Errorf("failed to load %s extension: %w", ext, err))
		}
	}
}

func main() {
	downloadExtensions()
}
