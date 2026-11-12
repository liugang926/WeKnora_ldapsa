package dingtalk

import "github.com/Tencent/WeKnora/internal/types"

// CursorContainsAnyDocument asks whether a locally retired document was still
// present in the last committed remote inventory. Remote-confirmed removals no
// longer match, avoiding a permanent full fetch after the first deletion.
func CursorContainsAnyDocument(cursor *types.SyncCursor, ids []string) (bool, error) {
	state, err := decodeCursor(cursor)
	if err != nil {
		return false, err
	}
	for _, revisions := range state.Resources {
		for _, id := range ids {
			if _, ok := revisions[id]; ok {
				return true, nil
			}
		}
	}
	return false, nil
}
