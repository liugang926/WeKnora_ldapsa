package nextcloud

import (
	"io"
	"testing"
)

func writeNextcloudTestResponse(t *testing.T, writer io.Writer, body string) {
	t.Helper()
	if _, err := io.WriteString(writer, body); err != nil {
		t.Errorf("write isolated Nextcloud response: %v", err)
	}
}
