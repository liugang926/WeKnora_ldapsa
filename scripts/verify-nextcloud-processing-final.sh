#!/usr/bin/env bash
# Run inside the single externally coordinated 2GiB/1CPU Go actor. The actor
# itself must have a hard wallclock timer; this script starts no containers.
set -uo pipefail
cd /app
EVIDENCE_DIR=docs/evidence/auto-space-prd-20261010
mkdir -p "$EVIDENCE_DIR"
validation_exit=0
run_check() {
  local label="$1"; shift
  "$@" > "$EVIDENCE_DIR/$label.log" 2>&1
  local check_exit=$?
  printf '%s exit=%s\n' "$label" "$check_exit" | tee -a "$EVIDENCE_DIR/processing-final-command-exits.log"
  if [ "$check_exit" -ne 0 ]; then validation_exit=1; fi
}
run_check final-recovery-regressions go test -p 1 -tags=sqlite ./internal/application/repository ./internal/application/access ./internal/middleware ./internal/types -run 'Test(Nextcloud(RootMigration|TargetMigration|Onboarding|PublicationRecoveryNewGeneration|ClosedRecoveryControls|ActualOwnerReadControls|PlannedGraphPayload|ActualGraphExactGC|BatchDelivery|ScopedRoutes)|PostgresNextcloudPublicationRecoveryNewGeneration|DataSourceRepository(UpdatePersistsDisabled|CreatePersistsEnabled)SyncDeletions|SQLiteNextcloudVersionMigration|ResolveTenantRoleSourceLookupFailure|RequiredGraphConfiguration)' -count=1 -v -timeout=7m
run_check final-live-graph go test -p 1 -tags=sqlite ./internal/application/service ./internal/datasource/connector/nextcloud -run 'TestNextcloud(LiveOriginalExtractionProviderAndStatusInbox|LiveOwnedRootAuthorizationAndContentRevision|SameETagHigherContentRevision|DownloadContentRevisionMismatch)' -count=1 -v -timeout=2m
if [ "${WEKNORA_ONBOARDING_LIVE_FIXTURE:-}" != "" ]; then
  # Peer NC CLI must already be watching a freshly removed .ready/.done pair.
  run_check final-live-onboarding go test -p 1 -tags=sqlite ./internal/handler -run '^TestNextcloudOnboardingActualNCRuntime$' -count=1 -v -timeout=8m
fi
run_check final-tree-compile go test -p 1 -tags=sqlite ./... -run '^$'
# Opt in to this exact measured count. Never label a smaller smoke count 10k.
if [ "${WEKNORA_GRAPH_PIPELINE_SCALE_FILES:-}" != "" ]; then
  actor_age=$(ps -o etimes= -p 1 | tr -d ' ')
  available_seconds=$(( ${WEKNORA_PROCESSING_WINDOW_SECONDS:-1200} - actor_age - 10 ))
  if [ "$available_seconds" -gt 120 ]; then
    run_check final-real-graph-scale go test -p 1 -tags=sqlite ./internal/application/service -run '^TestNextcloudControlledTenThousandFileProcessingPipeline$' -count=1 -v -timeout="${available_seconds}s"
  else
    printf 'final-real-graph-scale not-executed insufficient coordinated window\n' | tee -a "$EVIDENCE_DIR/processing-final-command-exits.log"
    validation_exit=1
  fi
fi
exit "$validation_exit"
