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
