package service

import (
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/stretchr/testify/require"
)

func TestNextcloudAutomaticAuthorizationFailureCodeIsStableAndNonSensitive(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "manager denied", err: access.ErrNextcloudPublicationDenied, want: "manager_denied"},
		{name: "manager unavailable", err: access.ErrNextcloudPublicationUnavailable, want: "manager_unavailable"},
		{name: "local authority denied", err: apprepo.ErrNextcloudOnboardingDenied, want: "local_authority_denied"},
		{name: "machine task invalid", err: apprepo.ErrNextcloudMachineTaskInvalid, want: "machine_task_invalid"},
		{name: "scope changed", err: apprepo.ErrKnowledgeInputChanged, want: "scope_changed"},
		{name: "wrapped denied", err: errors.Join(errors.New("source probe"), access.ErrNextcloudPublicationDenied), want: "manager_denied"},
		{name: "unclassified", err: errors.New("private implementation detail"), want: "authorization_unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, nextcloudAutomaticAuthorizationFailureCode(tt.err))
		})
	}
}
