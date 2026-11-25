package container

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

func TestConnectorRegistryIncludesDingTalk(t *testing.T) {
	registry, err := initConnectorRegistry()
	if err != nil {
		t.Fatalf("initConnectorRegistry() error = %v", err)
	}
	connector, err := registry.Get(types.ConnectorTypeDingTalk)
	if err != nil {
		t.Fatalf("DingTalk connector is not registered: %v", err)
	}
	if connector.Type() != types.ConnectorTypeDingTalk {
		t.Fatalf("connector.Type() = %q", connector.Type())
	}
}

func TestConnectorRegistryIncludesSeafile(t *testing.T) {
	registry, err := initConnectorRegistry()
	if err != nil {
		t.Fatalf("initConnectorRegistry() error = %v", err)
	}
	connector, err := registry.Get(types.ConnectorTypeSeafile)
	if err != nil {
		t.Fatalf("Seafile connector is not registered: %v", err)
	}
	if connector.Type() != types.ConnectorTypeSeafile {
		t.Fatalf("connector.Type() = %q", connector.Type())
	}
	if _, ok := connector.(datasource.FullStreamingConnector); !ok {
		t.Fatal("Seafile connector must implement datasource.FullStreamingConnector")
	}
}

func TestConnectorRegistryIncludesNativeWebDAV(t *testing.T) {
	registry, err := initConnectorRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{types.ConnectorTypeNextcloud, types.ConnectorTypeNextcloudNativeWebDAV} {
		registered, err := registry.Get(typ)
		if err != nil || registered.Type() != typ {
			t.Fatalf("registry.Get(%q) = %v, %v", typ, registered, err)
		}
	}
	connector, err := registry.Get(types.ConnectorTypeNextcloudNativeWebDAV)
	if err != nil {
		t.Fatalf("Nextcloud connector is not registered: %v", err)
	}
	if _, ok := connector.(datasource.FullStreamingConnector); !ok {
		t.Fatal("Nextcloud must support resumable full sync with deletion reconciliation")
	}
}
