package repository

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudRotationOnlyChangesMachineCredential(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	config := func(token, keyID string, extraSetting bool) types.JSON {
		t.Helper()
		settings := map[string]interface{}{"base_url": "https://nextcloud.example.test"}
		if extraSetting {
			settings["unrelated"] = true
		}
		value := &types.DataSourceConfig{
			Type:        types.ConnectorTypeNextcloud,
			Credentials: map[string]interface{}{"token": token, "key_id": keyID},
			ResourceIDs: []string{"binding-one"}, Settings: settings,
		}
		blob, err := value.ToJSON()
		require.NoError(t, err)
		return blob
	}
	old := config("old-machine-token", "pair_old", false)
	replacement := config("new-machine-token", "rot_new", false)
	rotation := NextcloudSourceRotation{
		OldKeyID: "pair_old", NewKeyID: "rot_new",
		OldConfig: old, NewConfig: replacement,
	}
	require.True(t, nextcloudRotationOnlyChangesMachineKey(rotation))
	rotation.NewConfig = config("new-machine-token", "rot_new", true)
	require.False(t, nextcloudRotationOnlyChangesMachineKey(rotation), "unrelated settings edit cannot rebind")
	rotation.NewConfig = config("old-machine-token", "rot_new", false)
	require.False(t, nextcloudRotationOnlyChangesMachineKey(rotation), "same token cannot prove rotation")
	rotation.NewConfig = replacement
	rotation.NewKeyID = "rot_other"
	require.False(t, nextcloudRotationOnlyChangesMachineKey(rotation), "new key ID must match the record")
}
