package file

import (
	"context"
	"io"
	"mime/multipart"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type catalogStub struct {
	resource    *types.StoredResource
	ref         string
	owners      []*types.Knowledge
	grantCalled bool
}

func (c *catalogStub) Register(
	_ context.Context,
	tenantID uint64,
	physicalPath string,
	meta interfaces.ResourceRegistration,
) (string, error) {
	c.resource = &types.StoredResource{
		ID:               "resource-1",
		Handle:           "AbCdEfGhIjKlMnOpQrStUv",
		TenantID:         tenantID,
		PhysicalPath:     physicalPath,
		OriginalName:     meta.OriginalName,
		SourceProvenance: meta.SourceProvenance,
	}
	c.ref = types.BuildResourcePath(c.resource.Handle)
	return c.ref, nil
}

func (c *catalogStub) Resolve(_ context.Context, _ string) (*types.StoredResource, error) {
	return c.resource, nil
}

func (c *catalogStub) ResolvePath(_ context.Context, value string) (string, *types.StoredResource, error) {
	if value == c.ref {
		return c.resource.PhysicalPath, c.resource, nil
	}
	return value, nil, nil
}
func (c *catalogStub) Bind(context.Context, string, string, string, string) error { return nil }
func (c *catalogStub) MarkDeleted(context.Context, string) error                  { return nil }

func (c *catalogStub) ListKnowledgeBaseIDs(context.Context, uint64, string) ([]string, error) {
	return nil, nil
}

func (c *catalogStub) Release(context.Context, string, string, string) (int64, error) {
	return 0, nil
}
func (c *catalogStub) CreateAccessGrant(context.Context, string, time.Duration) (string, error) {
	c.grantCalled = true
	return "GrantTokenAbCdEfGhIjKl", nil
}

func (c *catalogStub) ListResourceKnowledgeOwners(
	context.Context, uint64, string,
) ([]*types.Knowledge, bool, error) {
	return c.owners, c.resource != nil, nil
}

func (c *catalogStub) GetResourceSourceProvenance(
	context.Context, uint64, string,
) (string, error) {
	if c.resource == nil {
		return types.ResourceProvenanceUnknown, nil
	}
	if c.resource.SourceProvenance != "" {
		return c.resource.SourceProvenance, nil
	}
	return types.ResourceProvenanceOrdinary, nil
}

func (c *catalogStub) ResolveAccessGrant(context.Context, string) (*types.StoredResource, error) {
	return c.resource, nil
}

func (c *catalogStub) RevokeAccessGrantsByKnowledgeBase(context.Context, uint64, string) (int64, error) {
	return 0, nil
}

type physicalFileStub struct {
	savedPath string
	readPath  string
}

func (s *physicalFileStub) CheckConnectivity(context.Context) error { return nil }
func (s *physicalFileStub) SaveFile(context.Context, *multipart.FileHeader, uint64, string) (string, error) {
	return s.savedPath, nil
}

func (s *physicalFileStub) SaveBytes(context.Context, []byte, uint64, string, bool) (string, error) {
	return s.savedPath, nil
}

func (s *physicalFileStub) GetFile(_ context.Context, path string) (io.ReadCloser, error) {
	s.readPath = path
	return io.NopCloser(strings.NewReader("body")), nil
}
func (s *physicalFileStub) GetFileURL(context.Context, string) (string, error) { return "", nil }
func (s *physicalFileStub) DeleteFile(context.Context, string) error           { return nil }
func (s *physicalFileStub) CopyFile(context.Context, string, uint64, string) (string, error) {
	return "", nil
}

func TestResourceCatalogFileServiceReturnsReferenceAndResolvesReads(t *testing.T) {
	inner := &physicalFileStub{savedPath: "local://7/exports/a.png"}
	catalog := &catalogStub{}
	svc := NewResourceCatalogFileService(inner, catalog)

	ref, err := svc.SaveBytes(context.Background(), []byte("image"), 7, "a.png", false)
	require.NoError(t, err)
	require.Equal(t, "resource://AbCdEfGhIjKlMnOpQrStUv", ref)
	reader, err := svc.GetFile(context.Background(), ref)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, inner.savedPath, inner.readPath)
}

func TestResourceCatalogFileServiceReturnsShortExternalGrantURL(t *testing.T) {
	t.Setenv("APP_EXTERNAL_URL", "https://weknora.example.com/")
	inner := &physicalFileStub{savedPath: "local://7/exports/a.png"}
	catalog := &catalogStub{}
	svc := NewResourceCatalogFileService(inner, catalog)

	ref, err := svc.SaveBytes(context.Background(), []byte("image"), 7, "a.png", false)
	require.NoError(t, err)
	externalURL, err := svc.GetFileURL(context.Background(), ref)
	require.NoError(t, err)
	require.Equal(t, "https://weknora.example.com/r/GrantTokenAbCdEfGhIjKl", externalURL)
}

func TestResourceCatalogFileServiceNeverMintsNextcloudPublicURL(t *testing.T) {
	t.Setenv("APP_EXTERNAL_URL", "https://weknora.example.com/")
	inner := &physicalFileStub{savedPath: "local://7/doc/source.pdf"}
	catalog := &catalogStub{owners: []*types.Knowledge{{Channel: types.ConnectorTypeNextcloud}}}
	svc := NewResourceCatalogFileService(inner, catalog,
		access.NewNextcloudPublicationGuard(nil, nil))
	ref, err := svc.SaveBytes(context.Background(), []byte("source"), 7, "source.pdf", false)
	require.NoError(t, err)
	_, err = svc.GetFileURL(context.Background(), ref)
	require.Error(t, err)
	require.False(t, catalog.grantCalled)
}

func TestResourceCatalogFileServicePreservesSourceProvenanceBeforeKnowledgeInsert(t *testing.T) {
	t.Setenv("APP_EXTERNAL_URL", "https://weknora.example.com/")
	inner := &physicalFileStub{savedPath: "local://7/doc/source.pdf"}
	catalog := &catalogStub{}
	svc := NewResourceCatalogFileService(inner, catalog,
		access.NewNextcloudPublicationGuard(nil, nil))
	ctx := types.WithResourceProvenance(context.Background(), types.ResourceProvenanceNextcloud)
	_, err := svc.SaveFile(ctx, &multipart.FileHeader{Filename: "source.pdf", Size: 6}, 7, "knowledge-id")
	require.NoError(t, err)
	require.Equal(t, types.ResourceProvenanceNextcloud, catalog.resource.SourceProvenance)
	_, err = svc.GetFileURL(context.Background(), catalog.ref)
	require.Error(t, err)
	require.False(t, catalog.grantCalled)
}
