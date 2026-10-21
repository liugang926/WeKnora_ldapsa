package handler

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	repo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestNextcloudOnboardingErrorsUseOpaqueFixedCodes(t *testing.T) {
	for _, item := range []struct {
		err    error
		status int
		code   string
	}{
		{repo.ErrNextcloudOnboardingNotFound, http.StatusNotFound, "nextcloud_onboarding_not_found"},
		{repo.ErrNextcloudOnboardingDenied, http.StatusForbidden, "nextcloud_onboarding_denied"},
		{repo.ErrNextcloudOnboardingConflict, http.StatusConflict, "nextcloud_onboarding_conflict"},
		{fmt.Errorf("database unavailable"), http.StatusServiceUnavailable, "onboarding_unavailable"},
	} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		onboardingError(c, fmt.Errorf("private wrapped context: %w", item.err))
		require.Equal(t, item.status, w.Code)
		require.JSONEq(t, `{"error":"`+item.code+`"}`, w.Body.String())
	}
}

func TestNextcloudOnboardingErrorsExposeOnlyApprovedFailureStages(t *testing.T) {
	for _, stage := range []string{"operation_read", "tenant_deletion", "target_observation", "operation_refresh"} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		onboardingErrorStage(c, fmt.Errorf("private database detail"), stage)
		require.Equal(t, http.StatusServiceUnavailable, w.Code)
		require.JSONEq(t, `{"error":"onboarding_unavailable","failure_stage":"`+stage+`"}`, w.Body.String())
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	onboardingErrorStage(c, fmt.Errorf("private database detail"), "untrusted_remote_text")
	require.JSONEq(t, `{"error":"onboarding_unavailable"}`, w.Body.String())
}
