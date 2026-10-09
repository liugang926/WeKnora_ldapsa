package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
)

// resourceCatalogFileService keeps provider drivers physical-path-only while
// exposing stable resource:// references to the application layer.
type resourceCatalogFileService struct {
	inner            interfaces.FileService
	catalog          interfaces.ResourceCatalog
	externalURL      string
	publicationGuard *access.NextcloudPublicationGuard
}

// NewResourceCatalogFileService decorates a physical FileService with stable
// resource registration and resolution.
func NewResourceCatalogFileService(
	inner interfaces.FileService,
	catalog interfaces.ResourceCatalog,
	publicationGuards ...*access.NextcloudPublicationGuard,
) interfaces.FileService {
	if inner == nil || catalog == nil {
		return inner
	}
	service := &resourceCatalogFileService{
		inner:       inner,
		catalog:     catalog,
		externalURL: strings.TrimRight(strings.TrimSpace(os.Getenv("APP_EXTERNAL_URL")), "/"),
	}
	if len(publicationGuards) > 0 {
		service.publicationGuard = publicationGuards[0]
	}
	return service
}

func (s *resourceCatalogFileService) CheckConnectivity(ctx context.Context) error {
	return s.inner.CheckConnectivity(ctx)
}

func resourceKind(name string) (string, string) {
	mimeType := mime.TypeByExtension(strings.ToLower(filepath.Ext(name)))
	kind := "file"
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		kind = "image"
	case strings.HasPrefix(mimeType, "audio/"):
		kind = "audio"
	case strings.HasPrefix(mimeType, "video/"):
		kind = "video"
	}
	return kind, mimeType
}

func (s *resourceCatalogFileService) register(
	ctx context.Context,
	physical string,
	tenantID uint64,
	name string,
	size int64,
	temporary bool,
	contentHash string,
) (string, error) {
	kind, mimeType := resourceKind(name)
	ref, err := s.catalog.Register(ctx, tenantID, physical, interfaces.ResourceRegistration{
		Kind:             kind,
		MimeType:         mimeType,
		OriginalName:     filepath.Base(name),
		Size:             size,
		ContentHash:      contentHash,
		Temporary:        temporary,
		SourceProvenance: types.ResourceProvenanceFromContext(ctx),
	})
	if err != nil {
		_ = s.inner.DeleteFile(ctx, physical)
		return "", fmt.Errorf("register stored resource: %w", err)
	}
	return ref, nil
}

func (s *resourceCatalogFileService) SaveFile(
	ctx context.Context,
	file *multipart.FileHeader,
	tenantID uint64,
	knowledgeID string,
) (string, error) {
	physical, err := s.inner.SaveFile(ctx, file, tenantID, knowledgeID)
	if err != nil {
		return "", err
	}
	ref, err := s.register(ctx, physical, tenantID, file.Filename, file.Size, false, "")
	if err != nil {
		return "", err
	}
	if knowledgeID != "" {
		if err := s.catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, knowledgeID, types.ResourceRelationSourceFile); err != nil {
			_ = s.DeleteFile(ctx, ref)
			return "", fmt.Errorf("bind stored resource: %w", err)
		}
	}
	return ref, nil
}

func (s *resourceCatalogFileService) SaveBytes(
	ctx context.Context,
	data []byte,
	tenantID uint64,
	fileName string,
	temp bool,
) (string, error) {
	physical, err := s.inner.SaveBytes(ctx, data, tenantID, fileName, temp)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return s.register(ctx, physical, tenantID, fileName, int64(len(data)), temp, hex.EncodeToString(sum[:]))
}

func (s *resourceCatalogFileService) resolve(ctx context.Context, value string) (string, bool, error) {
	physical, resource, err := s.catalog.ResolvePath(ctx, value)
	return physical, resource != nil, err
}

func (s *resourceCatalogFileService) GetFile(ctx context.Context, filePath string) (io.ReadCloser, error) {
	physical, _, err := s.resolve(ctx, filePath)
	if err != nil {
		return nil, err
	}
	return s.inner.GetFile(ctx, physical)
}

func (s *resourceCatalogFileService) GetFileURL(ctx context.Context, filePath string) (string, error) {
	if s.catalog != nil {
		lookup, ok := s.catalog.(interfaces.ResourceKnowledgeLookup)
		if !ok {
			return "", fmt.Errorf("resource source lookup unavailable")
		}
		physical, resource, err := s.catalog.ResolvePath(ctx, filePath)
		if err != nil {
			return "", err
		}
		tenantID := uint64(0)
		if resource != nil {
			tenantID = resource.TenantID
		} else {
			tenantID = secutils.ParseTenantIDFromStoragePath(physical)
		}
		if tenantID == 0 {
			return "", fmt.Errorf("resource tenant cannot be verified")
		}
		owners, recognized, err := lookup.ListResourceKnowledgeOwners(ctx, tenantID, filePath)
		if err != nil {
			return "", err
		}
		if !recognized {
			return "", fmt.Errorf("resource source cannot be verified")
		}
		provenance, err := lookup.GetResourceSourceProvenance(ctx, tenantID, filePath)
		if err != nil {
			return "", err
		}
		// Public and storage-provider URLs outlive a source revocation. A
		// Nextcloud object must only be read through a guarded proxy.
		if provenance != types.ResourceProvenanceOrdinary {
			return "", fmt.Errorf("resource cannot be published: %s provenance", provenance)
		}
		for _, owner := range owners {
			if owner == nil || owner.DeletedAt.Valid {
				return "", fmt.Errorf("resource owner is unavailable")
			}
			if s.publicationGuard == nil {
				if owner.Channel == types.ConnectorTypeNextcloud {
					return "", apperrors.NewProtocolError(fmt.Errorf(
						"nextcloud resource requires a publication guard",
					), "Nextcloud resource requires a publication guard")
				}
				continue
			}
			// A URL has no stable human caller. Always evaluate it as an
			// anonymous principal, even when a logged-in user requests it.
			if err := s.publicationGuard.CheckKnowledge(context.Background(), owner); err != nil {
				return "", apperrors.NewProtocolError(fmt.Errorf(
					"resource cannot be published: %w",
					err,
				), fmt.Sprintf("resource cannot be published: %s", apperrors.PublicMessage(
					err,
				)))
			}
		}
	}
	physical, isResource, err := s.resolve(ctx, filePath)
	if err != nil {
		return "", err
	}
	if isResource && s.externalURL != "" {
		token, grantErr := s.catalog.CreateAccessGrant(ctx, filePath, 2*time.Hour)
		if grantErr != nil {
			return "", grantErr
		}
		return s.externalURL + "/r/" + token, nil
	}
	return s.inner.GetFileURL(ctx, physical)
}

func (s *resourceCatalogFileService) DeleteFile(ctx context.Context, filePath string) error {
	physical, isResource, err := s.resolve(ctx, filePath)
	if err != nil {
		return err
	}
	if err := s.inner.DeleteFile(ctx, physical); err != nil {
		return err
	}
	if isResource {
		return s.catalog.MarkDeleted(ctx, filePath)
	}
	return nil
}

func (s *resourceCatalogFileService) CopyFile(
	ctx context.Context,
	filePath string,
	tenantID uint64,
	knowledgeID string,
) (string, error) {
	physical, sourceResource, err := s.catalog.ResolvePath(ctx, filePath)
	if err != nil {
		return "", err
	}
	provenance := types.ResourceProvenanceUnknown
	if sourceResource != nil && sourceResource.SourceProvenance != "" {
		provenance = sourceResource.SourceProvenance
	}
	ctx = types.WithResourceProvenance(ctx, provenance)
	copied, err := s.inner.CopyFile(ctx, physical, tenantID, knowledgeID)
	if err != nil {
		return "", err
	}
	ref, err := s.register(ctx, copied, tenantID, filepath.Base(physical), 0, false, "")
	if err != nil {
		return "", err
	}
	if knowledgeID != "" {
		if err := s.catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, knowledgeID, types.ResourceRelationSourceFile); err != nil {
			_ = s.DeleteFile(ctx, ref)
			return "", fmt.Errorf("bind copied resource: %w", err)
		}
	}
	return ref, nil
}

var _ interfaces.FileService = (*resourceCatalogFileService)(nil)
