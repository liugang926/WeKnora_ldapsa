package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type qaRerankTenantService struct {
	interfaces.TenantService
	tenant *types.Tenant
	err    error
}

func (s *qaRerankTenantService) GetTenantByID(context.Context, uint64) (*types.Tenant, error) {
	return s.tenant, s.err
}

func TestSelectDefaultRerankModelID(t *testing.T) {
	rank := func(id string, builtin, def bool, status types.ModelStatus) *types.Model {
		return &types.Model{
			ID: id, Type: types.ModelTypeRerank, IsBuiltin: builtin,
			IsDefault: def, Status: status,
		}
	}
	tests := []struct {
		name   string
		models []*types.Model
		want   string
	}{
		{
			name: "platform default wins over earlier nondefault",
			models: []*types.Model{
				rank("tenant-other", false, false, types.ModelStatusActive),
				rank("builtin-bge", true, true, types.ModelStatusActive),
			},
			want: "builtin-bge",
		},
		{
			name: "tenant default overrides platform default",
			models: []*types.Model{
				rank("builtin-bge", true, true, types.ModelStatusActive),
				rank("tenant-choice", false, true, types.ModelStatusActive),
			},
			want: "tenant-choice",
		},
		{
			name: "unavailable default is skipped",
			models: []*types.Model{
				rank("downloading", false, true, types.ModelStatusDownloading),
				rank("builtin-bge", true, true, types.ModelStatusActive),
			},
			want: "builtin-bge",
		},
		{
			name: "fallback remains deterministic without defaults",
			models: []*types.Model{
				rank("tenant-z", false, false, types.ModelStatusActive),
				rank("builtin-a", true, false, types.ModelStatusActive),
				rank("tenant-a", false, false, types.ModelStatusActive),
			},
			want: "tenant-a",
		},
		{
			name: "irrelevant and unavailable models are ignored",
			models: []*types.Model{
				nil,
				{ID: "embedding", Type: types.ModelTypeEmbedding, IsDefault: true},
				rank("failed", true, true, types.ModelStatusDownloadFailed),
			},
			want: "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := selectDefaultRerankModelID(test.models); got != test.want {
				t.Fatalf("selectDefaultRerankModelID() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestResolveKnowledgeQARerankModelID(t *testing.T) {
	defaultModel := &types.Model{
		ID: "builtin-bge", Type: types.ModelTypeRerank,
		IsBuiltin: true, IsDefault: true, Status: types.ModelStatusActive,
	}
	tests := []struct {
		name      string
		tenant    *types.Tenant
		tenantErr error
		models    []*types.Model
		modelErr  error
		want      string
	}{
		{
			name: "workspace choice is honored",
			tenant: &types.Tenant{RetrievalConfig: &types.RetrievalConfig{
				RerankModelID: "workspace-reranker",
			}},
			models: []*types.Model{defaultModel},
			want:   "workspace-reranker",
		},
		{
			name:   "quick answer inherits platform default",
			tenant: &types.Tenant{RetrievalConfig: &types.RetrievalConfig{}},
			models: []*types.Model{defaultModel},
			want:   "builtin-bge",
		},
		{
			name:      "tenant lookup failure still uses visible default",
			tenantErr: errors.New("tenant unavailable"),
			models:    []*types.Model{defaultModel},
			want:      "builtin-bge",
		},
		{
			name:     "model listing failure does not block ordinary chat",
			tenant:   &types.Tenant{},
			modelErr: errors.New("models unavailable"),
			want:     "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			svc := &sessionService{
				tenantService: &qaRerankTenantService{tenant: test.tenant, err: test.tenantErr},
				modelService:  &stubModelService{availableModels: test.models, listErr: test.modelErr},
			}
			if got := svc.resolveKnowledgeQARerankModelID(context.Background(), 10001); got != test.want {
				t.Fatalf("resolveKnowledgeQARerankModelID() = %q, want %q", got, test.want)
			}
		})
	}
}
