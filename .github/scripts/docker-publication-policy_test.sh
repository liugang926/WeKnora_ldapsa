#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
policy_script="$script_dir/docker-publication-policy.sh"
output_file=$(mktemp)
log_file=$(mktemp)
trap 'rm -f -- "$output_file" "$log_file"' EXIT

check_policy() {
  local description="$1" repository="$2" enabled="$3" username="$4" password="$5" expected="$6"
  local status=0
  : > "$output_file"
  DOCKER_PUBLICATION_REPOSITORY="$repository" \
    DOCKER_PUBLICATION_ENABLED="$enabled" \
    DOCKERHUB_USERNAME="$username" \
    DOCKERHUB_PASSWORD="$password" \
    GITHUB_OUTPUT="$output_file" \
    bash "$policy_script" >"$log_file" 2>&1 || status=$?

  if [[ "$expected" == error ]]; then
    [[ "$status" -ne 0 && ! -s "$output_file" ]] || {
      printf 'FAIL: %s must reject missing publishing credentials\n' "$description" >&2
      exit 1
    }
    grep -Fq '::error::Docker Hub publishing is enabled, but both DOCKERHUB_USERNAME and DOCKERHUB_PASSWORD secrets are required.' "$log_file" || {
      printf 'FAIL: %s must explain its configuration failure\n' "$description" >&2
      exit 1
    }
  else
    [[ "$status" -eq 0 && "$(< "$output_file")" == "publish=$expected" ]] || {
      printf 'FAIL: %s\n' "$description" >&2
      exit 1
    }
  fi
  if grep -Eq 'test-user|test-password' "$log_file"; then
    printf 'FAIL: %s disclosed publishing credentials\n' "$description" >&2
    exit 1
  fi
  printf 'PASS: %s\n' "$description"
}

check_policy 'fork default, no credentials' 'example/WeKnora' '' '' '' false
check_policy 'fork default, credentials alone do not authorize publication' 'example/WeKnora' '' 'test-user' 'test-password' false
check_policy 'fork explicit opt-in with both credentials' 'example/WeKnora' true 'test-user' 'test-password' true
check_policy 'upstream with both credentials' 'Tencent/WeKnora' '' 'test-user' 'test-password' true
check_policy 'fork opt-in missing username' 'example/WeKnora' true '' 'test-password' error
check_policy 'fork opt-in missing password' 'example/WeKnora' true 'test-user' '' error
check_policy 'fork opt-in missing both credentials' 'example/WeKnora' true '' '' error
check_policy 'upstream missing username' 'Tencent/WeKnora' '' '' 'test-password' error
check_policy 'upstream missing password' 'Tencent/WeKnora' '' 'test-user' '' error
