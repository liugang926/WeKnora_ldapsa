//go:build sqlite_fts5

package service

import "testing"

func TestNextcloudLocalDerivedActualSQLitePipeline(t *testing.T) {
	for _, mode := range []string{
		"normal",
		"reader",
		"builder",
		"late-writer",
		"foreign-parent",
		"receipt-crash",
		"restored-current",
		"sqlite-ftserror",
		"physical-original",
		"unwitnessed-row",
		"restored-after-collection",
		"orphan-native",
	} {
		t.Run(mode, func(t *testing.T) { localDerivedActualPipeline(t, false, mode) })
	}
}

func TestOriginalBodyActualLocalDerivedSQLite(t *testing.T) {
	for _, mode := range []string{"body-retention", "body-inline-restore"} {
		t.Run(mode, func(t *testing.T) { localDerivedActualPipeline(t, false, mode) })
	}
}
