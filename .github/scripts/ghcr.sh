#!/usr/bin/env bash
# Shared GHCR lookup (same logic as Aegis's .github/scripts/ghcr.sh): empty
# means no `${VERSION_BASE}.N` tag is published yet; API errors must stop
# publication rather than silently restarting the patch counter at 0.
ghcr_max_patch() {
  local base="${VERSION_BASE}" org="${REGISTRY##*/}" base_re
  base_re="${base//./\\.}"
  local img="$1" page=1 resp code body page_max latest=""
  while :; do
    resp="$(curl -sS -w $'\n%{http_code}' \
      -H "Authorization: Bearer ${GH_TOKEN}" \
      -H "Accept: application/vnd.github+json" \
      -H "X-GitHub-Api-Version: 2022-11-28" \
      "https://api.github.com/orgs/${org}/packages/container/${img}/versions?per_page=100&page=${page}")" \
      || { echo "GHCR versions API request failed (transport) for ${img}." >&2; return 2; }
    code="$(printf '%s' "$resp" | tail -n1)"
    body="$(printf '%s' "$resp" | sed '$d')"
    case "${code}" in
      200) : ;;
      404) break ;;
      *)   echo "GHCR versions API for ${img} returned HTTP ${code}." >&2; return 2 ;;
    esac
    [ "$(printf '%s' "$body" | tr -d '[:space:]')" = "[]" ] && break
    page_max="$(printf '%s' "$body" \
      | grep -oE "\"${base_re}\\.[0-9]+\"" | tr -d '"' \
      | sed -E "s/^${base_re}\\.//" | sort -n | tail -1 || true)"
    if [ -n "${page_max}" ]; then
      latest="$(printf '%s\n%s\n' "${latest:-0}" "${page_max}" | sort -n | tail -1)"
    fi
    page=$((page + 1))
    [ "${page}" -gt 100 ] && { echo "GHCR pagination exceeded 100 pages for ${img}; aborting." >&2; return 2; }
  done
  printf '%s' "${latest}"
}

next_version() {
  local patch
  patch="$(ghcr_max_patch "$1")" || return $?
  printf '%s.%s\n' "$VERSION_BASE" "$(( ${patch:--1} + 1 ))"
}
