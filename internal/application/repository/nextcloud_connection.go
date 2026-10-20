package repository

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	connectionInstancePattern  = regexp.MustCompile(`\A[A-Za-z0-9._:-]{1,128}\z`)
	connectionDirectoryPattern = regexp.MustCompile(`\A[A-Za-z0-9._:-]{1,128}\z`)
	connectionHostPattern      = regexp.MustCompile(`\A[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\z`)
	connectionGUIDPattern      = regexp.MustCompile(
		`\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\z`,
	)
)

type nextcloudConnectionTenant struct {
	ID   uint64
	Name string
}

// ConnectionWorkspaces returns the workspaces a Nextcloud administrator may
// authorize. A platform API key may discover all live workspaces; a browser
// session may discover only workspaces where its role is Admin or Owner.
func (s *NextcloudOnboardingStore) ConnectionWorkspaces(
	ctx context.Context,
	userID string,
	platform bool,
) ([]types.NextcloudConnectionWorkspace, error) {
	var rows []nextcloudConnectionTenant
	tx := s.db.WithContext(ctx)
	if platform {
		if err := tx.Table("tenants").
			Select("id, name").
			Where("deleted_at IS NULL").
			Order("id").
			Limit(200).
			Scan(&rows).
			Error; err != nil {
			return nil, err
		}
	} else {
		if userID == "" {
			return nil, ErrNextcloudOnboardingDenied
		}
		if err := tx.Table("tenant_members m").
			Select("t.id, t.name").
			Joins("JOIN tenants t ON t.id=m.tenant_id AND t.deleted_at IS NULL").
			Where("m.user_id=? AND m.status=? AND m.deleted_at IS NULL AND m.role IN (?,?)",
				userID,
				types.TenantMemberStatusActive,
				types.TenantRoleOwner,
				types.TenantRoleAdmin).
			Order("t.id").
			Limit(200).
			Scan(&rows).
			Error; err != nil {
			return nil, err
		}
	}
	out := make([]types.NextcloudConnectionWorkspace, 0, len(rows))
	for _, row := range rows {
		workspace := types.NextcloudConnectionWorkspace{
			TenantID:   row.ID,
			TenantName: row.Name,
		}
		template, err := connectionDefaultTemplate(tx, row.ID)
		if err != nil {
			workspace.Reason = "no_processing_template"
		} else {
			workspace.Eligible = true
			workspace.ProcessingTemplateID = template.ID
			workspace.ProcessingTemplate = template.Name
		}
		out = append(out, workspace)
	}
	return out, nil
}

// AuthorizeConnection creates one scoped onboarding credential per selected
// workspace. The returned tokens are one-time values; only their hashes remain
// in WeKnora.
func (s *NextcloudOnboardingStore) AuthorizeConnection(
	ctx context.Context,
	req types.NextcloudConnectionAuthorizeRequest,
	userID string,
	platform bool,
) (types.NextcloudConnectionAuthorizeResponse, error) {
	var out types.NextcloudConnectionAuthorizeResponse
	if !connectionInstancePattern.MatchString(req.InstanceID) ||
		!connectionDirectoryPattern.MatchString(req.DirectoryID) ||
		!connectionBaseURLCanonical(req.BaseURL) ||
		len(req.TenantIDs) == 0 || len(req.TenantIDs) > 32 {
		return out, ErrNextcloudOnboardingDenied
	}
	if (req.ActorDirectoryID == "") != (req.ActorObjectGUID == "") {
		return out, ErrNextcloudOnboardingDenied
	}
	req.BaseURL = strings.TrimSuffix(req.BaseURL, "/")
	if req.ActorDirectoryID != "" &&
		(!connectionDirectoryPattern.MatchString(req.ActorDirectoryID) ||
			!connectionGUIDPattern.MatchString(req.ActorObjectGUID)) {
		return out, ErrNextcloudOnboardingDenied
	}
	seen := map[uint64]struct{}{}
	for _, tenantID := range req.TenantIDs {
		if tenantID == 0 {
			return out, ErrNextcloudOnboardingDenied
		}
		if _, exists := seen[tenantID]; exists {
			return out, ErrNextcloudOnboardingDenied
		}
		seen[tenantID] = struct{}{}
	}

	connectionID := uuid.NewString()
	out.ConnectionID = connectionID
	out.Credentials = make([]types.NextcloudConnectionCredential, 0, len(req.TenantIDs))
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var directoryCount int64
		if err := tx.Table("directories").
			Where("id=? AND enabled=?", req.DirectoryID, true).
			Count(&directoryCount).
			Error; err != nil || directoryCount != 1 {
			return ErrNextcloudOnboardingDenied
		}
		for _, tenantID := range req.TenantIDs {
			if err := connectionTenantAllowed(tx, tenantID, userID, platform); err != nil {
				return err
			}
			var tenant nextcloudConnectionTenant
			if err := tx.Table("tenants").
				Select("id, name").
				Where("id=? AND deleted_at IS NULL", tenantID).
				Take(&tenant).
				Error; err != nil {
				return ErrNextcloudOnboardingDenied
			}
			template, err := connectionDefaultTemplate(tx, tenantID)
			if err != nil {
				return ErrNextcloudOnboardingConflict
			}
			// Reconnecting one Nextcloud instance/workspace replaces only the
			// connection-issued credentials; operator credentials are retained.
			if err := tx.Exec(
				"UPDATE nextcloud_onboarding_credentials SET enabled=FALSE WHERE instance_id=? AND tenant_id=? AND id LIKE 'nc-conn-%' AND NOT EXISTS (SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.credential_id=nextcloud_onboarding_credentials.id)",
				req.InstanceID,
				tenantID,
			).Error; err != nil {
				return err
			}
			raw := make([]byte, 32)
			if _, err := rand.Read(raw); err != nil {
				return err
			}
			token := hex.EncodeToString(raw)
			hash := sha256.Sum256([]byte(token))
			credentialID := "nc-conn-" + connectionID + "-" + strconv.FormatUint(tenantID, 10)
			credential := types.NextcloudOnboardingCredential{
				ID:                credentialID,
				TokenSHA256:       hex.EncodeToString(hash[:]),
				InstanceID:        req.InstanceID,
				BaseURL:           req.BaseURL,
				TenantID:          tenantID,
				DirectoryID:       req.DirectoryID,
				TemplateKBID:      template.ID,
				Enabled:           true,
				MaxKnowledgeBases: 1000,
			}
			if err := tx.Create(&credential).Error; err != nil {
				return err
			}
			if req.ActorDirectoryID != "" {
				var directoryCount int64
				if err := tx.Table("directories").
					Where("id=? AND enabled=?", req.ActorDirectoryID, true).
					Count(&directoryCount).
					Error; err != nil {
					return err
				}
				if directoryCount != 1 {
					return ErrNextcloudOnboardingDenied
				}
				delegation := types.NextcloudOnboardingDelegation{
					TenantID:    tenantID,
					DirectoryID: req.ActorDirectoryID,
					ObjectGUID:  strings.ToLower(req.ActorObjectGUID),
				}
				if err := tx.Clauses(clause.OnConflict{DoNothing: true}).
					Create(&delegation).
					Error; err != nil {
					return err
				}
			}
			out.Credentials = append(out.Credentials, types.NextcloudConnectionCredential{
				TenantID:             tenantID,
				TenantName:           tenant.Name,
				CredentialID:         credentialID,
				Token:                token,
				ProcessingTemplateID: template.ID,
				ProcessingTemplate:   template.Name,
			})
		}
		return nil
	})
	if err != nil {
		return types.NextcloudConnectionAuthorizeResponse{}, err
	}
	return out, nil
}

// RevokeConnection disables only credentials issued by a Nextcloud connection.
// Existing workspaces and knowledge bases remain untouched.
func (s *NextcloudOnboardingStore) RevokeConnection(
	ctx context.Context,
	credentialIDs []string,
	userID string,
	platform bool,
) (int, error) {
	if len(credentialIDs) == 0 || len(credentialIDs) > 64 {
		return 0, ErrNextcloudOnboardingDenied
	}
	revoked := 0
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		seen := map[string]struct{}{}
		for _, id := range credentialIDs {
			if !strings.HasPrefix(id, "nc-conn-") || len(id) > 255 {
				return ErrNextcloudOnboardingDenied
			}
			if _, exists := seen[id]; exists {
				continue
			}
			seen[id] = struct{}{}
			var credential types.NextcloudOnboardingCredential
			if err := tx.Where("id=?", id).Take(&credential).Error; err != nil {
				continue
			}
			if !platform {
				if err := connectionTenantAllowed(tx, credential.TenantID, userID, false); err != nil {
					return err
				}
			}
			result := tx.Exec(
				"UPDATE nextcloud_onboarding_credentials SET enabled=FALSE WHERE id=? AND NOT EXISTS (SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.credential_id=nextcloud_onboarding_credentials.id)",
				id,
			)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 1 {
				revoked++
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return revoked, nil
}

// RevokeConnectionByCredential lets a Nextcloud instance disconnect its own
// connection using one of the scoped onboarding credentials it already holds.
// It cannot disable credentials belonging to another instance or workspace.
func (s *NextcloudOnboardingStore) RevokeConnectionByCredential(
	ctx context.Context,
	credential types.NextcloudOnboardingCredential,
	credentialIDs []string,
) (int, error) {
	if len(credentialIDs) == 0 || len(credentialIDs) > 64 {
		return 0, ErrNextcloudOnboardingDenied
	}
	connectionPrefix := strings.TrimSuffix(
		credential.ID,
		"-"+strconv.FormatUint(credential.TenantID, 10),
	)
	if !strings.HasPrefix(credential.ID, "nc-conn-") ||
		!strings.HasPrefix(connectionPrefix, "nc-conn-") ||
		connectionPrefix == credential.ID {
		return 0, ErrNextcloudOnboardingDenied
	}
	revoked := 0
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		seen := map[string]struct{}{}
		for _, id := range credentialIDs {
			if !strings.HasPrefix(id, "nc-conn-") || len(id) > 255 {
				return ErrNextcloudOnboardingDenied
			}
			if _, exists := seen[id]; exists {
				continue
			}
			seen[id] = struct{}{}
			var target types.NextcloudOnboardingCredential
			if err := tx.Where("id=?", id).Take(&target).Error; err != nil {
				continue
			}
			if target.InstanceID != credential.InstanceID ||
				!strings.HasPrefix(target.ID, connectionPrefix+"-") {
				return ErrNextcloudOnboardingDenied
			}
			result := tx.Exec(
				"UPDATE nextcloud_onboarding_credentials SET enabled=FALSE WHERE id=? AND NOT EXISTS (SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.credential_id=nextcloud_onboarding_credentials.id)",
				id,
			)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 1 {
				revoked++
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return revoked, nil
}

func connectionTenantAllowed(tx *gorm.DB, tenantID uint64, userID string, platform bool) error {
	var count int64
	if err := tx.Table("tenants").
		Where("id=? AND deleted_at IS NULL", tenantID).
		Count(&count).
		Error; err != nil {
		return err
	}
	if count != 1 {
		return ErrNextcloudOnboardingDenied
	}
	if platform {
		return nil
	}
	if userID == "" {
		return ErrNextcloudOnboardingDenied
	}
	count = 0
	if err := tx.Table("tenant_members").
		Where("tenant_id=? AND user_id=? AND status=? AND deleted_at IS NULL AND role IN (?,?)",
			tenantID,
			userID,
			types.TenantMemberStatusActive,
			types.TenantRoleOwner,
			types.TenantRoleAdmin).
		Count(&count).
		Error; err != nil {
		return err
	}
	if count != 1 {
		return ErrNextcloudOnboardingDenied
	}
	return nil
}

func connectionDefaultTemplate(tx *gorm.DB, tenantID uint64) (*types.KnowledgeBase, error) {
	var rows []types.KnowledgeBase
	if err := tx.Where(
		"tenant_id=? AND type=? AND is_temporary=? AND deleted_at IS NULL",
		tenantID,
		types.KnowledgeBaseTypeDocument,
		false,
	).
		Order("created_at ASC").
		Limit(64).
		Find(&rows).
		Error; err != nil {
		return nil, err
	}
	for i := range rows {
		if err := validateOnboardingTemplate(tx, &rows[i]); err == nil {
			return &rows[i], nil
		}
	}
	return nil, ErrNextcloudOnboardingConflict
}

func connectionBaseURLCanonical(raw string) bool {
	if raw == "" || len(raw) > 512 || strings.TrimSpace(raw) != raw {
		return false
	}
	u, err := url.Parse(raw)
	devHTTP := strings.TrimSpace(os.Getenv("WEKNORA_EVENT_DEV_HTTP")) == "1"
	if err != nil || (u.Scheme != "https" && !(devHTTP && u.Scheme == "http")) ||
		u.Host == "" || u.User != nil ||
		u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return false
	}
	if port := u.Port(); port != "" {
		parsed, err := strconv.Atoi(port)
		if err != nil || parsed < 1 || parsed > 65535 {
			return false
		}
	}
	host := strings.ToLower(u.Hostname())
	return host != "" && len(host) <= 253 &&
		connectionHostPattern.MatchString(host) &&
		!strings.Contains(host, "..")
}
