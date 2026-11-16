package repository

import "testing"

func TestDingTalkGraphSkipReasons(t *testing.T) {
	for reason, want := range map[string]bool{"graph_backend_disabled": true, "extract_disabled": true, "model_declined": true, "truncated": false, "context_length": false, "model_declined:timeout": false, "": false} {
		if got := validDingTalkGraphSkipReason(reason); got != want {
			t.Errorf("skip %q: got %t, want %t", reason, got, want)
		}
	}
}
