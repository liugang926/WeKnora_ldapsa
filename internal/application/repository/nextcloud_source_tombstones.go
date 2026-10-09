package repository

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
)

// NextcloudSourcePresence deliberately has no "never" state. Absence of
// retained evidence cannot prove that a legacy scope was source-independent.
type NextcloudSourcePresence string

const (
	// NextcloudSourcePresenceUnknown identifies a source-presence state that has not been observed.
	NextcloudSourcePresenceUnknown NextcloudSourcePresence = "unknown"
	// NextcloudSourcePresenceEver records retained evidence of a source-influenced scope.
	NextcloudSourcePresenceEver NextcloudSourcePresence = "ever"
)

// LookupNextcloudSourcePresence reads independent positive tombstones. It is a
// foundation API, not yet wired into message authorization. Ever is evidence
// for conservative classification; it never grants access to an old answer.
func (r *DataSourceRepository) LookupNextcloudSourcePresence(
	ctx context.Context, tenantID uint64, scopeType, scopeID string,
) (NextcloudSourcePresence, error) {
	if r == nil || r.db == nil || tenantID == 0 || tenantID > math.MaxInt64 ||
		scopeID == "" || strings.TrimSpace(scopeID) != scopeID || strings.ContainsAny(scopeID, "\x00\r\n") {
		return NextcloudSourcePresenceUnknown, errors.New("invalid Nextcloud source tombstone lookup")
	}
	switch scopeType {
	case "tenant":
		if scopeID != strconv.FormatUint(tenantID, 10) {
			return NextcloudSourcePresenceUnknown, apperrors.NewProtocolError(errors.New(
				"nextcloud tenant tombstone identity mismatch",
			), "Nextcloud tenant tombstone identity mismatch")
		}
	case "knowledge_base", "datasource", "pair":
	default:
		return NextcloudSourcePresenceUnknown, errors.New("invalid Nextcloud source tombstone scope")
	}
	var count int64
	if err := r.db.WithContext(ctx).Table("nextcloud_source_tombstones").
		Where("tenant_id = ? AND scope_type = ? AND scope_id = ?", tenantID, scopeType, scopeID).
		Count(&count).Error; err != nil {
		return NextcloudSourcePresenceUnknown, fmt.Errorf("read Nextcloud source tombstone: %w", err)
	}
	if count > 0 {
		return NextcloudSourcePresenceEver, nil
	}
	return NextcloudSourcePresenceUnknown, nil
}
