package repository

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/stretchr/testify/require"
)

func TestCanUseDurableManagerPurposeOnlyForTransientUnavailability(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "direct transient unavailability",
			err:  access.ErrNextcloudPublicationUnavailable,
			want: true,
		},
		{
			name: "wrapped transient unavailability",
			err:  fmt.Errorf("manager read: %w", access.ErrNextcloudPublicationUnavailable),
			want: true,
		},
		{
			name: "explicit manager denial",
			err:  access.ErrNextcloudPublicationDenied,
			want: false,
		},
		{
			name: "local scope change",
			err:  ErrKnowledgeInputChanged,
			want: false,
		},
		{
			name: "unclassified error",
			err:  errors.New("unexpected"),
			want: false,
		},
		{
			name: "nil error",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, canUseDurableManagerPurpose(tt.err))
		})
	}
}
