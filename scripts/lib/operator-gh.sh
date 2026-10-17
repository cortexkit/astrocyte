#!/usr/bin/env bash
# Resolve the real GitHub CLI for operator-only administrative scripts.

_operator_gh_resolve_path() {
  local path="$1"
  local target directory
  local hops=0

  # macOS readlink has no -f flag, so resolve the final symlink explicitly and
  # let cd -P resolve symlinked directories.
  while [[ -L "$path" ]]; do
    hops=$((hops + 1))
    if (( hops > 40 )); then
      return 1
    fi
    if ! target=$(readlink "$path" 2>/dev/null); then
      return 1
    fi
    if [[ "$target" == /* ]]; then
      path="$target"
    else
      directory=$(dirname "$path")
      path="$directory/$target"
    fi
  done

  directory=$(cd -P "$(dirname "$path")" 2>/dev/null && pwd -P) || return 1
  printf '%s/%s\n' "$directory" "$(basename "$path")"
}

_operator_gh_is_shim_path() {
  case "$1" in
    */cortexkit/aft/shims/*) return 0 ;;
    *) return 1 ;;
  esac
}

_operator_gh_is_upstream_candidate() {
  local candidate="$1"
  local resolved="$2"

  [[ "$(basename "$resolved")" == "gh" ]] || return 1
  ! _operator_gh_is_shim_path "$candidate" && \
    ! _operator_gh_is_shim_path "$resolved"
}

_operator_gh_find() {
  local path_value="${PATH:-}"
  local path_entry candidate resolved
  local last=0
  local fallback_paths fallback_path_value fallback_last=0 fallback_path

  # Walk PATH ourselves instead of using command -v so an AFT routing shim
  # earlier in PATH can be skipped without losing the later real CLI.
  while :; do
    if [[ "$path_value" == *:* ]]; then
      path_entry="${path_value%%:*}"
      path_value="${path_value#*:}"
    else
      path_entry="$path_value"
      path_value=""
      last=1
    fi

    [[ -n "$path_entry" ]] || path_entry="."
    candidate="$path_entry/gh"
    if [[ -x "$candidate" && ! -d "$candidate" ]] && \
      resolved=$(_operator_gh_resolve_path "$candidate") && \
      _operator_gh_is_upstream_candidate "$candidate" "$resolved"; then
      printf '%s\n' "$candidate"
      return 0
    fi

    if (( last == 1 )); then
      break
    fi
  done

  # These locations cover the supported macOS/Linux package-manager installs.
  # The override keeps isolated tests independent of host-installed binaries.
  if [[ "${OPERATOR_GH_FALLBACK_PATHS+x}" == x ]]; then
    fallback_paths="$OPERATOR_GH_FALLBACK_PATHS"
  else
    fallback_paths="/opt/zerobrew/bin/gh:/opt/homebrew/bin/gh:/usr/local/bin/gh"
  fi
  fallback_path_value="$fallback_paths"
  while :; do
    if [[ "$fallback_path_value" == *:* ]]; then
      fallback_path="${fallback_path_value%%:*}"
      fallback_path_value="${fallback_path_value#*:}"
    else
      fallback_path="$fallback_path_value"
      fallback_path_value=""
      fallback_last=1
    fi

    if [[ -n "$fallback_path" ]]; then
      if [[ -x "$fallback_path" && ! -d "$fallback_path" ]] && \
        resolved=$(_operator_gh_resolve_path "$fallback_path") && \
        _operator_gh_is_upstream_candidate "$fallback_path" "$resolved"; then
        printf '%s\n' "$fallback_path"
        return 0
      fi
    fi

    if (( fallback_last == 1 )); then
      break
    fi
  done

  return 1
}

if ! OPERATOR_GH=$(_operator_gh_find); then
  echo "operator-gh: no upstream gh executable found on PATH or supported install roots; refusing to use the AFT routing shim" >&2
  return 1
fi
export OPERATOR_GH
