package handler

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/service"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	ldapdirectory "github.com/Tencent/WeKnora/internal/infrastructure/directory"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
)

type directoryLoginRuntimeStub struct {
	interfaces.DirectoryRuntimeService
	err error
}

func (s *directoryLoginRuntimeStub) Login(
	context.Context,
	string,
	string,
) (*types.LoginResponse, error) {
	return nil, s.err
}

func TestLDAPLoginMapsDirectoryOperationalFailuresToServiceUnavailable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name     string
		err      error
		httpCode int
	}{
		{
			name: "all controllers unavailable",
			err: &ldapdirectory.FailoverError{
				Operation: "authentication",
				Attempts: []ldapdirectory.ControllerAttempt{{
					ControllerURL: "ldaps://dc.example.test", Err: errors.New("connection refused"),
				}},
			},
			httpCode: http.StatusServiceUnavailable,
		},
		{
			name:     "incomplete membership page",
			err:      ldapdirectory.ErrIncompleteResults,
			httpCode: http.StatusServiceUnavailable,
		},
		{
			name:     "invalid live group",
			err:      ldapdirectory.ErrInvalidDirectoryObject,
			httpCode: http.StatusServiceUnavailable,
		},
		{
			name:     "membership mismatch",
			err:      service.ErrDirectoryMembershipMismatch,
			httpCode: http.StatusServiceUnavailable,
		},
		{
			name:     "wrong password",
			err:      ldapdirectory.ErrInvalidCredentials,
			httpCode: http.StatusUnauthorized,
		},
		{
			name:     "unknown user",
			err:      ldapdirectory.ErrUserNotFound,
			httpCode: http.StatusUnauthorized,
		},
		{
			name:     "ambiguous user",
			err:      ldapdirectory.ErrAmbiguousUser,
			httpCode: http.StatusUnauthorized,
		},
		{
			name:     "disabled user",
			err:      ldapdirectory.ErrUserDisabled,
			httpCode: http.StatusUnauthorized,
		},
		{
			name:     "database unavailable",
			err:      errors.New("database down"),
			httpCode: http.StatusServiceUnavailable,
		},
		{name: "deadline", err: context.DeadlineExceeded, httpCode: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(
				http.MethodPost,
				"/api/v1/auth/ldap",
				bytes.NewBufferString(`{"identifier":"alice","password":"secret"}`),
			)
			ctx.Request.Header.Set("Content-Type", "application/json")
			NewDirectoryHandler(&directoryLoginRuntimeStub{err: test.err}).LDAPLogin(ctx)
			if len(ctx.Errors) != 1 {
				t.Fatalf("handler errors = %v, want one", ctx.Errors)
			}
			appErr, ok := ctx.Errors[0].Err.(*apperrors.AppError)
			if !ok {
				t.Fatalf("handler error type = %T, want *errors.AppError", ctx.Errors[0].Err)
			}
			if appErr.HTTPCode != test.httpCode {
				t.Fatalf("HTTP status mapping = %d, want %d", appErr.HTTPCode, test.httpCode)
			}
			if test.httpCode == http.StatusUnauthorized &&
				appErr.Message != "Directory login failed" {
				t.Fatalf(
					"authentication failures must be indistinguishable, got %q",
					appErr.Message,
				)
			}
		})
	}
}
