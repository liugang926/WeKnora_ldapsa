package repository

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func runOriginalBodyInlineRowActual(t *testing.T, pg bool) {
	for _, mode := range []string{"all-heads-once", "two-rows", "unselected-corrupt", "second-row-pin-change", "sql-head-change", "caller-cancel", "concurrent-purge", "vault-body-digest", "mid-head-and-pin", "last-payload-cancel"} {
		t.Run(mode, func(t *testing.T) {
			db, store := bodyProtocolFixture(t, pg)
			for _, id := range []string{"one", "two"} {
				require.NoError(t, db.Model(&types.Knowledge{}).Where("id=?", id).Updates(map[string]any{
					"title":       "native title " + id,
					"file_name":   "native file " + id,
					"description": "native description " + id,
					"profile": types.JSON(
						"{\"gist\":\"native profile " + id + "\"}",
					),
					"custom_metadata": types.JSON("{\"native\":\"" + id + "\"}"),
				}).Error)
			}
			checks := 0
			triggered := false
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "unselected-corrupt" {
				var head originalBodyFieldHead
				require.NoError(
					t,
					bodyInternal(
						db,
					).Where("source_table='knowledges' AND row_key='one' AND column_name='metadata.manual.content'").
						Take(&head).
						Error,
				)
				require.NoError(
					t,
					bodyInternal(
						db,
					).Model(&originalBodyPayload{}).
						Where("id=?", head.BodyID).
						Update("state", "purged").
						Error,
				)
			}
			require.NoError(
				t,
				db.Callback().Query().Before("gorm:query").Register("actual-inline-row-count", func(tx *gorm.DB) {
					if tx.Statement.Table == "original_body_field_heads" {
						checks++
					}
					if tx.Statement.Table != "original_body_journal_heads" {
						return
					}
					if triggered {
						return
					}
					switch mode {
					case "sql-head-change":
						triggered = true
						require.NoError(
							t,
							bodyInternal(
								tx,
							).Exec("UPDATE original_body_journal_heads SET head_sha256=? WHERE id=1", strings.Repeat("0", 64)).
								Error,
						)
					case "caller-cancel":
						triggered = true
						cancel()
					}
				}),
			)
			defer func() { require.NoError(t, db.Callback().Query().Remove("actual-inline-row-count")) }()
			if mode == "second-row-pin-change" {
				require.NoError(
					t,
					db.Callback().
						Query().
						After("gorm:query").
						Before("original_body:resolve").
						Register("actual-inline-row-transition", func(tx *gorm.DB) {
							if tx.Statement.Table != "original_body_field_heads" || triggered {
								return
							}
							for _, value := range tx.Statement.Vars {
								if value == "two" {
									triggered = true
									raw, err := os.ReadFile(store.journal.paths.PinPath)
									require.NoError(t, err)
									raw[0] = 'x'
									require.NoError(t, os.WriteFile(store.journal.paths.PinPath, raw, 0o600))
									return
								}
							}
						}),
				)
				defer func() { require.NoError(t, db.Callback().Query().Remove("actual-inline-row-transition")) }()
			}
			if mode == "concurrent-purge" {
				require.NoError(
					t,
					db.Callback().
						Query().
						After("gorm:query").
						Before("original_body:resolve").
						Register("actual-inline-row-transition", func(tx *gorm.DB) {
							if tx.Statement.Table != "knowledges" || triggered ||
								tx.Statement.Context.Value(originalBodyInternalKey{}) != nil {
								return
							}
							triggered = true
							type outcome struct {
								count int
								err   error
							}
							done := make(chan outcome, 1)
							go func() {
								expr := "datetime('now','-2 hours')"
								if pg {
									expr = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
								}
								err := db.Exec("UPDATE knowledges SET deleted_at=" + expr + " WHERE id='one'").Error
								if err != nil {
									done <- outcome{err: err}
									return
								}
								purgeCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
								defer stop()
								n, err := store.PurgeDue(purgeCtx, 100)
								done <- outcome{count: n, err: err}
							}()
							select {
							case result := <-done:
								require.NoError(t, result.err)
								require.Positive(
									t,
									result.count,
									"a real durable purge commits before cached row resolution",
								)
							case <-time.After(3 * time.Second):
								t.Fatal("owned concurrent purge did not finish")
							}
						}),
				)
				defer func() { require.NoError(t, db.Callback().Query().Remove("actual-inline-row-transition")) }()
			}
			if mode == "mid-head-and-pin" || mode == "last-payload-cancel" {
				var headCount int64
				require.NoError(
					t,
					bodyInternal(
						db,
					).Table("original_body_field_heads").
						Where("source_table='knowledges' AND row_key='one'").
						Count(&headCount).
						Error,
				)
				payloadReads := int64(0)
				require.NoError(
					t,
					db.Callback().
						Query().
						After("gorm:query").
						Register("actual-mid-availability-transition", func(tx *gorm.DB) {
							if tx.Statement.Table != "original_body_payloads" || triggered {
								return
							}
							payloadReads++
							if mode == "last-payload-cancel" {
								if payloadReads == headCount {
									triggered = true
									cancel()
								}
								return
							}
							triggered = true
							// This actual native writer replaces the previously captured head.
							require.NoError(
								t,
								db.Model(&types.Knowledge{}).
									Where("id='one'").
									Update("title", "current replacement after earlier body check").
									Error,
							)
							// A different closed owner undergoes a real purge, advancing SQL
							// and external journal facts while this original row is read.
							bodyProtocolDeleted(t, db, "two", true)
							n, err := store.PurgeDue(context.Background(), 100)
							require.NoError(t, err)
							require.Positive(t, n)
						}),
				)
				defer func() { require.NoError(t, db.Callback().Query().Remove("actual-mid-availability-transition")) }()
			}
			if mode == "vault-body-digest" {
				id := bodyProtocolCreate(t, db, "actual-vault", "two")
				require.NoError(
					t,
					bodyInternal(
						db,
					).Model(&originalBodyPayload{}).
						Where("id=?", id).
						Update("payload", "unproven replacement bytes").
						Error,
				)
				var row bodyProtocolRow
				require.ErrorIs(t, db.Where("manifest_id='actual-vault'").Take(&row).Error, ErrOriginalBodyUnavailable)
				return
			}
			var err error
			if mode == "two-rows" || mode == "second-row-pin-change" {
				var rows []*types.Knowledge
				err = db.WithContext(ctx).Where("id IN ?", []string{"one", "two"}).Order("id").Find(&rows).Error
				if mode == "two-rows" {
					require.NoError(t, err)
					require.Len(t, rows, 2)
					require.Equal(
						t,
						6,
						checks,
						"each row enumerates initial, all-head, and final bound heads independently",
					)
					return
				}
			} else {
				var row types.Knowledge
				query := db.WithContext(ctx).Where("id='one'")
				if mode == "unselected-corrupt" {
					query = query.Select("id", "title")
				}
				err = query.Take(&row).Error
				if mode == "all-heads-once" {
					require.NoError(t, err)
					require.Equal(
						t,
						3,
						checks,
						"many inline fields use one all-head check with initial/final head binding",
					)
					require.Equal(t, "native title one", row.Title)
					return
				}
			}
			if mode == "caller-cancel" || mode == "last-payload-cancel" {
				require.True(t, triggered)
				require.True(t, errors.Is(err, context.Canceled), "cancellation cannot return cached body success")
			} else {
				if mode != "unselected-corrupt" {
					require.True(t, triggered)
				}
				require.ErrorIs(t, err, ErrOriginalBodyUnavailable)
			}
		})
	}
}

func TestOriginalBodyInlineRowActualSQLite(t *testing.T)   { runOriginalBodyInlineRowActual(t, false) }
func TestPostgresOriginalBodyInlineRowActual(t *testing.T) { runOriginalBodyInlineRowActual(t, true) }
