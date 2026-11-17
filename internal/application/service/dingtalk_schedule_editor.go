package service

import (
	"maps"
	"reflect"
	"slices"

	"github.com/Tencent/WeKnora/internal/types"
)

// The source editor sends only public settings and selected resources.
// Credentials have their own write endpoint and must never participate in an
// ordinary form save. A missing include_uploaded_files has legacy semantics;
// keep absent switches distinct from explicitly configured ones.
func unchangedDingTalkEditorConfig(before, after *types.DataSource) bool {
	stored, err := before.ParseConfig()
	if err != nil || stored == nil {
		return false
	}
	proposed, err := after.ParseConfig()
	if err != nil || proposed == nil {
		return false
	}
	if proposed.Type != "" && proposed.Type != stored.Type {
		return false
	}
	if !maps.EqualFunc(stored.Settings, proposed.Settings, reflect.DeepEqual) {
		return false
	}
	// Selection order in the editor is presentation only. Retain the stored
	// traversal order when the same set was selected.
	oldIDs := slices.Clone(stored.ResourceIDs)
	newIDs := slices.Clone(proposed.ResourceIDs)
	slices.Sort(oldIDs)
	slices.Sort(newIDs)
	return slices.Equal(oldIDs, newIDs)
}
