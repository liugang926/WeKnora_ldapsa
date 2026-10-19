package repository

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type originalBodySchemaProbeTestKey struct{}

func runOriginalBodySchemaProbeCancelActual(t *testing.T, pg bool) {
	for _, mode := range []string{"normal-schema", "cancel-at-driver-probe", "real-schema-absent"} {
		t.Run(mode, func(t *testing.T) {
			db, _ := bodyProtocolFixture(t, pg)
			bodyProtocolCreate(t, db, "schema-probe-live", "one")
			ctx, cancel := context.WithCancel(
				context.WithValue(context.Background(), originalBodySchemaProbeTestKey{}, "owned-probe"),
			)
			defer cancel()
			tx := db.WithContext(ctx)
			require.NoError(t, ctx.Err())
			require.True(
				t,
				originalBodySchemaPresent(tx),
				"the real target schema exists before the probe cancellation",
			)
			if mode == "normal-schema" {
				require.NoError(
					t,
					CheckOriginalBodyAvailableInTx(ctx, tx, "knowledge_auto_tag_input_manifests", "schema-probe-live"),
				)
				return
			}
			if mode == "real-schema-absent" {
				// This fixture owns its private SQLite file / PostgreSQL schema.
				// Remove the actual table; do not mock the schema predicate.
				require.NoError(t, db.Migrator().DropTable(&originalBodyPayload{}))
				require.False(t, originalBodySchemaPresent(tx))
				require.NoError(t, ctx.Err())
				require.NoError(
					t,
					CheckOriginalBodyAvailableInTx(ctx, tx, "knowledge_auto_tag_input_manifests", "schema-probe-live"),
					"actual no-schema compatibility remains unchanged",
				)
				return
			}
			probe := func(tx *gorm.DB) bool {
				sql := tx.Statement.SQL.String()
				return tx.Statement.Context.Value(originalBodySchemaProbeTestKey{}) == "owned-probe" &&
					strings.Contains(sql, "original_body_payloads") &&
					(strings.Contains(sql, "sqlite_master") || strings.Contains(sql, "information_schema.tables"))
			}
			beforeCalls, afterCalls := 0, 0
			var queryErr error
			require.NoError(
				t,
				db.Callback().
					Row().
					Before("gorm:row").
					Register("actual-body-schema-probe-cancel-before", func(tx *gorm.DB) {
						if !probe(tx) {
							return
						}
						beforeCalls++
						require.NoError(t, ctx.Err(), "the API entry reached the real driver probe with a live caller")
						require.Equal(t, "owned-probe", tx.Statement.Context.Value(originalBodySchemaProbeTestKey{}))
						cancel()
					}),
			)
			defer func() { require.NoError(t, db.Callback().Row().Remove("actual-body-schema-probe-cancel-before")) }()
			require.NoError(
				t,
				db.Callback().
					Row().
					After("gorm:row").
					Register("actual-body-schema-probe-cancel-after", func(tx *gorm.DB) {
						if probe(tx) {
							afterCalls++
							queryErr = tx.Error
						}
					}),
			)
			defer func() { require.NoError(t, db.Callback().Row().Remove("actual-body-schema-probe-cancel-after")) }()
			err := CheckOriginalBodyAvailableInTx(ctx, tx, "knowledge_auto_tag_input_manifests", "schema-probe-live")
			require.Equal(t, 1, beforeCalls, "cancel at the actual Raw.Scan schema query, not before API entry")
			require.Equal(t, 1, afterCalls)
			require.ErrorIs(
				t,
				queryErr,
				context.Canceled,
				"the real SQL query path reports cancellation; no AddError or predicate stub",
			)
			require.ErrorIs(
				t,
				err,
				context.Canceled,
				"a canceled schema probe cannot look like successful no-schema compatibility",
			)
			require.True(t, originalBodySchemaPresent(db), "the real table was not removed by cancellation")
			require.Equal(
				t,
				1,
				beforeCalls,
				"the separate uncanceled schema check must not retrigger the owned caller's callback",
			)
			require.Equal(t, 1, afterCalls)
		})
	}
}

func TestOriginalBodySchemaProbeCancelActualSQLite(t *testing.T) {
	runOriginalBodySchemaProbeCancelActual(t, false)
}

func TestPostgresOriginalBodySchemaProbeCancelActual(t *testing.T) {
	runOriginalBodySchemaProbeCancelActual(t, true)
}
