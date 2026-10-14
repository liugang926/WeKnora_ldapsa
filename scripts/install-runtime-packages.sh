#!/usr/bin/env bash
# Retry the complete runtime install without discarding successful downloads.
# The caller performs the signed apt-get update once before this helper.
set -euo pipefail

if [ "$#" -eq 0 ]; then
    printf 'Usage: install-runtime-packages.sh PACKAGE...\n' >&2
    exit 64
fi

# Debian container docker-clean hooks remove the default archive directory.
# This helper owns a separate temporary archive directory for this one batch;
# no caller-supplied cleanup path or persistent APT configuration is accepted.
archive_dir=$(mktemp -d /tmp/weknora-runtime-apt.XXXXXX)
chmod 0755 "$archive_dir"
apt_options=(-o Acquire::Retries=5 -o Acquire::http::Timeout=180
    -o "Dir::Cache::archives=$archive_dir"
    -o APT::Keep-Downloaded-Packages=true)
max_attempts=3
attempt=1

while true; do
    if apt-get "${apt_options[@]}" install -y --no-install-recommends "$@"; then
        # Cleanup is reached only after the entire requested batch succeeds.
        # A cleanup error still fails the build and preserves this directory.
        apt-get -o "Dir::Cache::archives=$archive_dir" clean
        rm -rf -- "$archive_dir"
        exit 0
    else
        install_status=$?
    fi

    if [ "$attempt" -ge "$max_attempts" ]; then
        printf 'Runtime APT install failed after %s attempts (exit %s).\n' "$attempt" "$install_status" >&2
        exit "$install_status"
    fi
    printf 'Retrying runtime APT install (%s/%s, previous exit %s); downloaded archives retained.\n' "$((attempt + 1))" "$max_attempts" "$install_status" >&2
    sleep 2
    attempt=$((attempt + 1))
done
