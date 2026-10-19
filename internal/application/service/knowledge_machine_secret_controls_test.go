package service

import (
	"context"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type machineSecretModels struct {
	interfaces.ModelService
	t      *testing.T
	mode   string
	secret string
	appID  string
	mutate func()
}

func (m machineSecretModels) GetChatModel(ctx context.Context, id string) (chat.Chat, error) {
	tenant, ok := types.TenantInfoFromContext(ctx)
	require.True(m.t, ok)
	require.NotNil(m.t, tenant.Credentials)
	require.Equal(m.t, m.secret, tenant.Credentials.WeKnoraCloud.AppSecret)
	c, err := m.ModelService.GetChatModel(ctx, id)
	if err != nil {
		return nil, err
	}
	return machineSecretChat{delegate: c, input: m}, nil
}

type machineSecretChat struct {
	delegate chat.Chat
	input    machineSecretModels
}

func (m machineSecretChat) Chat(
	ctx context.Context,
	messages []chat.Message,
	options *chat.ChatOptions,
) (*types.ChatResponse, error) {
	for _, message := range messages {
		require.False(
			m.input.t,
			strings.Contains(message.Content, m.input.secret) || strings.Contains(message.Content, m.input.appID),
			"routing credentials cannot enter the classification prompt",
		)
	}
	result, err := m.delegate.Chat(ctx, messages, options)
	switch m.input.mode {
	case "credential ABA":
		m.input.mutate()
	case "runtime context changed":
		tenant, _ := types.TenantInfoFromContext(ctx)
		tenant.Credentials.WeKnoraCloud.AppSecret = "different isolated runtime secret"
	}
	return result, err
}

func (m machineSecretChat) ChatStream(
	ctx context.Context,
	messages []chat.Message,
	options *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	return m.delegate.ChatStream(ctx, messages, options)
}
func (m machineSecretChat) GetModelName() string { return m.delegate.GetModelName() }
func (m machineSecretChat) GetModelID() string   { return m.delegate.GetModelID() }

func runKnowledgeMachineSecretControls(t *testing.T, pg bool) {
	for _, mode := range []string{"secret digest only", "credential ABA", "runtime context changed"} {
		t.Run(mode, func(t *testing.T) {
			m := newMachinePipelineFixture(t, pg)
			secret, appID := "isolated-never-retain-secret", "isolated-never-retain-app-id"
			credentials := &types.CredentialsConfig{
				WeKnoraCloud: &types.WeKnoraCloudCredentials{AppID: appID, AppSecret: secret},
			}
			require.NoError(t, m.f.db.Model(&types.Tenant{}).Where("id=7").Update("credentials", credentials).Error)
			m.auto.modelService = machineSecretModels{
				ModelService: machinePipelineModels{auto: m.model, summary: m.f.model},
				t:            t,
				mode:         mode,
				secret:       secret,
				appID:        appID,
				mutate: func() {
					changed := &types.CredentialsConfig{
						WeKnoraCloud: &types.WeKnoraCloudCredentials{
							AppID:     appID,
							AppSecret: "different isolated SQL secret",
						},
					}
					require.NoError(t, m.f.db.Model(&types.Tenant{}).Where("id=7").Update("credentials", changed).Error)
					require.NoError(
						t,
						m.f.db.Model(&types.Tenant{}).Where("id=7").Update("credentials", credentials).Error,
					)
				},
			}
			row := m.ingest(t)
			denied := false
			for i := 0; i < len(m.f.queue.tasks); i++ {
				if err := m.run(t, m.f.queue.tasks[i]); err != nil {
					denied = true
					break
				}
			}
			require.Equal(t, mode != "secret digest only", denied)
			require.Equal(t, 1, m.model.calls)
			var added int64
			require.NoError(
				t,
				m.f.db.Model(&types.KnowledgeTagRelation{}).
					Where("knowledge_id = ? AND tag_id='machine-topic'", row.ID).
					Count(&added).
					Error,
			)
			if denied {
				require.Zero(t, added)
			} else {
				require.Equal(t, int64(1), added)
			}
			for _, table := range []string{"knowledge_auto_tag_input_manifests", "datasource_name_inputs", "nextcloud_original_upload_inputs"} {
				var inputs []string
				require.NoError(t, m.f.db.Table(table).Pluck("actual_inputs", &inputs).Error)
				require.NotEmpty(t, inputs)
				for _, input := range inputs {
					require.False(
						t,
						strings.Contains(input, secret) || strings.Contains(input, appID) || strings.Contains(input, "tenant_input") ||
							strings.Contains(input, "app_secret"),
						"immutable receipt must contain only credential/configuration digest and version",
					)
				}
			}
		})
	}
}

func TestKnowledgeMachineSecretControlsSQLite(t *testing.T) {
	runKnowledgeMachineSecretControls(t, false)
}

func TestPostgresKnowledgeMachineSecretControls(t *testing.T) {
	runKnowledgeMachineSecretControls(t, true)
}
