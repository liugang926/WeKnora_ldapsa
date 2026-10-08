#!/usr/bin/env bash
set -euo pipefail

# Publishing is a separate, explicit capability from image verification.
# Preserve the upstream repository's publishing behavior; forks must opt in.
# Never print credentials, including when publication configuration is invalid.
publish=false
if [[ "${DOCKER_PUBLICATION_REPOSITORY:-}" == "Tencent/WeKnora" || "${DOCKER_PUBLICATION_ENABLED:-}" == "true" ]]; then
  if [[ -z "${DOCKERHUB_USERNAME:-}" || -z "${DOCKERHUB_PASSWORD:-}" ]]; then
    echo "::error::Docker Hub publishing is enabled, but both DOCKERHUB_USERNAME and DOCKERHUB_PASSWORD secrets are required."
    exit 1
  fi
  publish=true
else
  echo "::notice::Docker Hub publishing is disabled for this fork. The independent app/UI image verification workflows do not require publishing credentials. To publish, explicitly set repository variable DOCKERHUB_PUBLISH_ENABLED=true and configure both Docker Hub secrets."
fi

printf 'publish=%s\n' "$publish" >> "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
