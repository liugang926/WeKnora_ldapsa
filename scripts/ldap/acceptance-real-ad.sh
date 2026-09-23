#!/usr/bin/env bash
# Real Active Directory acceptance helper.
#
# This script is intentionally opt-in and has NOT been run against an
# enterprise domain as part of this delivery. It performs certificate-verified
# LDAPS/StartTLS, service bind, critical paged search, escaped user/group lookup,
# optional user bind, multi-controller preflight, and optional WeKnora API sync
# checks. Passwords are accepted only through protected files.
set -euo pipefail

die() { echo "$*" >&2; exit 2; }
is_true() { case "${1:-}" in 1|true|TRUE|yes|YES) return 0 ;; *) return 1 ;; esac; }
trim() {
  local value=$1
  value=${value#"${value%%[![:space:]]*}"}
  value=${value%"${value##*[![:space:]]}"}
  printf '%s' "$value"
}

required=(AD_BASE_DN AD_BIND_DN AD_BIND_PASSWORD_FILE AD_CA_FILE AD_TEST_LOGIN)
for name in "${required[@]}"; do
  [[ -n "${!name:-}" ]] || die "missing required environment variable: ${name}"
done
for command_name in ldapsearch openssl python3; do
  command -v "$command_name" >/dev/null || die "${command_name} is required"
done
for secret_file in "$AD_BIND_PASSWORD_FILE" "$AD_CA_FILE"; do
  [[ -f "$secret_file" ]] || die "required file does not exist: $secret_file"
done
if [[ -n "${AD_TEST_USER_PASSWORD_FILE:-}" ]]; then
  [[ -f "$AD_TEST_USER_PASSWORD_FILE" ]] || die "AD_TEST_USER_PASSWORD_FILE does not exist"
  command -v ldapwhoami >/dev/null || die "ldapwhoami is required for the optional user-bind check"
fi

# AD_URLS matches LDAP_URLS and is preferred for multi-controller acceptance.
# AD_URL and the original AD_LDAPS_URL remain supported for one controller.
raw_urls=${AD_URLS:-${AD_URL:-${AD_LDAPS_URL:-}}}
[[ -n "$raw_urls" ]] || die "set AD_URLS, AD_URL, or legacy AD_LDAPS_URL"
IFS=',' read -r -a untrimmed_urls <<< "$raw_urls"
directory_urls=()
for value in "${untrimmed_urls[@]}"; do
  value=$(trim "$value")
  [[ -n "$value" ]] && directory_urls+=("$value")
done
(( ${#directory_urls[@]} > 0 )) || die "no non-empty directory URL was supplied"

server_names=()
if [[ -n "${AD_TLS_SERVER_NAMES:-}" ]]; then
  IFS=',' read -r -a untrimmed_names <<< "$AD_TLS_SERVER_NAMES"
  for value in "${untrimmed_names[@]}"; do server_names+=("$(trim "$value")"); done
elif [[ -n "${AD_TLS_SERVER_NAME:-}" ]]; then
  server_names+=("$AD_TLS_SERVER_NAME")
fi

transport=${AD_TLS_MODE:-}
if [[ -z "$transport" ]]; then
  case "${directory_urls[0]}" in
    ldaps://*) transport=ldaps ;;
    ldap://*) transport=starttls ;;
    *) die "directory URLs must use ldaps:// or ldap://" ;;
  esac
fi
case "$transport" in ldaps|starttls) ;; *) die "AD_TLS_MODE must be ldaps or starttls" ;; esac

user_base_dn=${AD_USER_BASE_DN:-$AD_BASE_DN}
group_base_dn=${AD_GROUP_BASE_DN:-$AD_BASE_DN}
user_filter=${AD_USER_FILTER:-'(&(objectCategory=person)(objectClass=user))'}
group_filter=${AD_GROUP_FILTER:-'(objectCategory=group)'}
if [[ -n "${AD_ALLOWED_LOGIN_FILTER:-}" ]]; then user_filter="(&${user_filter}${AD_ALLOWED_LOGIN_FILTER})"; fi
login_filter=${AD_LOGIN_FILTER:-'(|(sAMAccountName={login})(userPrincipalName={login}))'}
[[ "$login_filter" == *'{login}'* ]] || die "AD_LOGIN_FILTER must contain {login}"
page_size=${AD_PAGE_SIZE:-200}
result_limit=${AD_RESULT_LIMIT:-10000}
[[ "$page_size" =~ ^[1-9][0-9]*$ ]] || die "AD_PAGE_SIZE must be a positive integer"
[[ "$result_limit" =~ ^[1-9][0-9]*$ ]] || die "AD_RESULT_LIMIT must be a positive integer"

escape_filter_value() {
  AD_FILTER_VALUE=$1 python3 - <<'PY'
import os
raw = os.environ["AD_FILTER_VALUE"].encode("utf-8")
print("".join(f"\\{b:02x}" if b in (0, 40, 41, 42, 92) or b < 32 or b >= 127 else chr(b) for b in raw))
PY
}
escaped_login=$(escape_filter_value "$AD_TEST_LOGIN")
login_filter=${login_filter//\{login\}/$escaped_login}
login_filter="(&${user_filter}${login_filter})"

export LDAPTLS_CACERT="$AD_CA_FILE"
export LDAPTLS_REQCERT=demand
healthy_urls=()
failed_urls=()
first_user_dn=""

check_controller() {
  local index=$1 url=$2 host port server_name cert_output login_output entry_count user_dn
  read -r host port < <(AD_CHECK_URL="$url" AD_CHECK_TRANSPORT="$transport" python3 - <<'PY'
import os
from urllib.parse import urlsplit
u = urlsplit(os.environ["AD_CHECK_URL"])
expected = "ldaps" if os.environ["AD_CHECK_TRANSPORT"] == "ldaps" else "ldap"
if u.scheme.lower() != expected or not u.hostname or u.username or u.password or u.path not in ("", "/"):
    raise SystemExit(2)
print(u.hostname, u.port or (636 if expected == "ldaps" else 389))
PY
  ) || { echo "invalid URL/transport combination: $url" >&2; return 1; }
  server_name=${server_names[$index]:-$host}

  echo "  certificate: ${url} (verify name: ${server_name})"
  openssl_args=(s_client -connect "${host}:${port}" -servername "$server_name" -CAfile "$AD_CA_FILE" -verify_return_error)
  if python3 - "$server_name" <<'PY' >/dev/null 2>&1
import ipaddress, sys
ipaddress.ip_address(sys.argv[1])
PY
  then openssl_args+=(-verify_ip "$server_name"); else openssl_args+=(-verify_hostname "$server_name"); fi
  ldap_tls_args=()
  if [[ "$transport" == starttls ]]; then openssl_args+=(-starttls ldap); ldap_tls_args=(-ZZ); fi
  cert_output=$(openssl "${openssl_args[@]}" </dev/null 2>/dev/null | openssl x509 -noout -subject -issuer -dates) || return 1
  printf '%s\n' "$cert_output" | sed 's/^/    /'

  echo "  service bind and RootDSE"
  ldapsearch -LLL -x "${ldap_tls_args[@]}" -H "$url" -D "$AD_BIND_DN" \
    -y "$AD_BIND_PASSWORD_FILE" -b "" -s base defaultNamingContext supportedControl >/dev/null || return 1
  echo "  critical paged user search"
  ldapsearch -LLL -x "${ldap_tls_args[@]}" -H "$url" -D "$AD_BIND_DN" \
    -y "$AD_BIND_PASSWORD_FILE" -b "$user_base_dn" -z "$result_limit" \
    -E "!pr=${page_size}/noprompt" "$user_filter" dn >/dev/null || return 1

  echo "  escaped login lookup"
  login_output=$(ldapsearch -LLL -o ldif-wrap=no -x "${ldap_tls_args[@]}" -H "$url" -D "$AD_BIND_DN" \
    -y "$AD_BIND_PASSWORD_FILE" -b "$user_base_dn" -z 2 "$login_filter" \
    objectGUID objectSid distinguishedName sAMAccountName userPrincipalName mail \
    memberOf primaryGroupID userAccountControl) || return 1
  entry_count=$(printf '%s\n' "$login_output" | awk '/^dn::? / { count++ } END { print count+0 }')
  if is_true "${AD_EXPECT_LOGIN_ABSENT:-false}"; then
    (( entry_count == 0 )) || { echo "expected login outside scope, found ${entry_count} entry" >&2; return 1; }
    return 0
  fi
  (( entry_count == 1 )) || { echo "expected exactly one in-scope login entry, found ${entry_count}" >&2; return 1; }
  printf '%s\n' "$login_output" | grep -Eq '^objectGUID::? ' || { echo "objectGUID missing" >&2; return 1; }
  printf '%s\n' "$login_output" | grep -Eq '^objectSid::? ' || { echo "objectSid missing" >&2; return 1; }
  printf '%s\n' "$login_output" | grep -q '^primaryGroupID: ' || { echo "primaryGroupID missing" >&2; return 1; }
  user_dn=$(printf '%s\n' "$login_output" | python3 -c 'import base64,sys
for line in sys.stdin:
    if line.startswith("dn:: "):
        print(base64.b64decode(line[5:].strip()).decode()); break
    if line.startswith("dn: "):
        print(line[4:].strip()); break')
  [[ -n "$first_user_dn" ]] || first_user_dn=$user_dn

  if [[ -n "${AD_TEST_GROUP:-}" ]]; then
    local escaped_group group_lookup_filter group_output group_count
    escaped_group=$(escape_filter_value "$AD_TEST_GROUP")
    group_lookup_filter="(&${group_filter}(|(sAMAccountName=${escaped_group})(cn=${escaped_group})))"
    group_output=$(ldapsearch -LLL -x "${ldap_tls_args[@]}" -H "$url" -D "$AD_BIND_DN" \
      -y "$AD_BIND_PASSWORD_FILE" -b "$group_base_dn" -z 2 "$group_lookup_filter" \
      objectGUID objectSid member memberOf) || return 1
    group_count=$(printf '%s\n' "$group_output" | awk '/^dn::? / { count++ } END { print count+0 }')
    (( group_count == 1 )) || { echo "expected exactly one AD_TEST_GROUP entry, found ${group_count}" >&2; return 1; }
  fi
}

echo "[1/3] validating ${#directory_urls[@]} configured controller(s) with ${transport}"
for index in "${!directory_urls[@]}"; do
  url=${directory_urls[$index]}
  echo "controller $((index + 1))/${#directory_urls[@]}: ${url}"
  if check_controller "$index" "$url"; then healthy_urls+=("$url"); else echo "  FAILED: ${url}" >&2; failed_urls+=("$url"); fi
done
(( ${#healthy_urls[@]} > 0 )) || die "no controller passed certificate, bind, paging, and lookup checks"
if is_true "${AD_FAILOVER_DRILL:-false}"; then
  (( ${#directory_urls[@]} >= 2 )) || die "AD_FAILOVER_DRILL requires at least two AD_URLS"
  (( ${#failed_urls[@]} >= 1 )) || die "AD_FAILOVER_DRILL expects at least one intentionally unavailable controller"
else
  (( ${#failed_urls[@]} == 0 )) || die "controller failure; for a planned outage rerun with AD_FAILOVER_DRILL=true"
fi

echo "[2/3] optional user bind"
if [[ -n "${AD_TEST_USER_PASSWORD_FILE:-}" && -n "$first_user_dn" ]]; then
  bind_url=${healthy_urls[0]}
  ldap_tls_args=()
  [[ "$transport" == starttls ]] && ldap_tls_args=(-ZZ)
  ldapwhoami -x "${ldap_tls_args[@]}" -H "$bind_url" -D "$first_user_dn" \
    -y "$AD_TEST_USER_PASSWORD_FILE" >/dev/null
  echo "  user bind passed on ${bind_url}"
else
  echo "  skipped (set AD_TEST_USER_PASSWORD_FILE to verify the user bind)"
fi

echo "[3/3] optional WeKnora admin API preview/sync/status"
if [[ -n "${WEKNORA_BASE_URL:-}" && -n "${WEKNORA_ADMIN_TOKEN_FILE:-}" ]]; then
  [[ -f "$WEKNORA_ADMIN_TOKEN_FILE" ]] || die "WEKNORA_ADMIN_TOKEN_FILE does not exist"
  command -v curl >/dev/null || die "curl is required for API checks"
  token=$(<"$WEKNORA_ADMIN_TOKEN_FILE")
  api_base=${WEKNORA_BASE_URL%/}/api/v1/system/admin/directory
  curl --fail --silent --show-error -H "Authorization: Bearer ${token}" "$api_base/status" >/dev/null
  curl --fail --silent --show-error -H "Authorization: Bearer ${token}" --get \
    --data-urlencode "q=${AD_TEST_LOGIN}" --data-urlencode "limit=20" "$api_base/users" >/dev/null
  if [[ -n "${AD_TEST_GROUP:-}" ]]; then
    curl --fail --silent --show-error -H "Authorization: Bearer ${token}" --get \
      --data-urlencode "q=${AD_TEST_GROUP}" --data-urlencode "limit=20" "$api_base/groups" >/dev/null
  fi
  curl --fail --silent --show-error -X POST -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json" -d '{}' "$api_base/sync/preview" >/dev/null
  curl --fail --silent --show-error -X POST -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json" -d '{}' "$api_base/sync" >/dev/null
  if [[ -n "${AD_TEST_USER_PASSWORD_FILE:-}" ]] && ! is_true "${AD_EXPECT_LOGIN_ABSENT:-false}"; then
    # Generate the request on stdin so neither the password nor the response
    # token appears in argv, shell traces, command substitution, or output.
    python3 - "$AD_TEST_LOGIN" "$AD_TEST_USER_PASSWORD_FILE" <<'PY' | \
      curl --fail --silent --show-error -H "Content-Type: application/json" \
        --data-binary @- "${WEKNORA_BASE_URL%/}/api/v1/auth/ldap/login" >/dev/null
import json, pathlib, sys
password = pathlib.Path(sys.argv[2]).read_text(encoding="utf-8").rstrip("\r\n")
print(json.dumps({"identifier": sys.argv[1], "password": password}))
PY
    echo "  application LDAP login and live group verification passed"
  fi
  status_json=$(curl --fail --silent --show-error -H "Authorization: Bearer ${token}" "$api_base/status")
  curl --fail --silent --show-error -H "Authorization: Bearer ${token}" "$api_base/sync/runs?limit=5" >/dev/null
  if is_true "${AD_FAILOVER_DRILL:-false}"; then
    healthy_joined=$(IFS=,; printf '%s' "${healthy_urls[*]}")
    STATUS_JSON="$status_json" HEALTHY_URLS="$healthy_joined" python3 - <<'PY'
import json, os
status = json.loads(os.environ["STATUS_JSON"])
active = status.get("active_server", "")
healthy = [v for v in os.environ["HEALTHY_URLS"].split(",") if v]
if active not in healthy:
    raise SystemExit(f"WeKnora active_server {active!r} is not a healthy failover controller: {healthy!r}")
print(f"  application failover selected {active}")
PY
  fi
  unset token status_json
  echo "  API status, queries, preview, sync, and run-history checks passed"
elif is_true "${AD_FAILOVER_DRILL:-false}"; then
  die "AD_FAILOVER_DRILL also requires WEKNORA_BASE_URL and WEKNORA_ADMIN_TOKEN_FILE"
else
  echo "  skipped (set WEKNORA_BASE_URL and WEKNORA_ADMIN_TOKEN_FILE)"
fi

echo "Protocol checks passed. Complete and record the stateful scenarios in docs/LDAP_AD.md:"
echo "nested/primary/cyclic groups; rename and OU move; disabled/out-of-scope users;"
echo "staleness/recovery; role merge; restricted KB/agent paths; revocation; and audit redaction."
