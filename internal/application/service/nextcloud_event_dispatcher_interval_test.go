package service

import (
	"testing"
	"time"
)

func TestNextcloudEventDispatchInterval(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want time.Duration
	}{
		{name: "default", raw: "", want: 5 * time.Second},
		{name: "lower_bound", raw: "1s", want: time.Second},
		{name: "override", raw: " 8s ", want: 8 * time.Second},
		{name: "upper_bound", raw: "1m", want: time.Minute},
		{name: "too_fast", raw: "500ms", want: 5 * time.Second},
		{name: "too_slow", raw: "61s", want: 5 * time.Second},
		{name: "invalid", raw: "fast", want: 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(nextcloudEventDispatchIntervalEnv, tc.raw)
			if got := nextcloudEventDispatchInterval(); got != tc.want {
				t.Fatalf("dispatch interval = %s, want %s", got, tc.want)
			}
		})
	}
}
