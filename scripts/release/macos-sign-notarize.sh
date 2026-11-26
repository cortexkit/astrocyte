#!/usr/bin/env bash
# Sign and notarize the darwin ck-astrocyte archive, in the order the checksum needs.
#
# A browser adds `com.apple.quarantine` to downloads. An ad-hoc signed CLI can
# run after a curl download but hang on launch from a quarantined copy, so an
# installer smoke test alone does not establish that a macOS asset is usable.
#
# ORDER IS LOAD-BEARING AND THE CALLER MUST NOT REARRANGE IT:
#
#     build -> SIGN the binary -> package the zip -> NOTARIZE + staple the zip
#           -> checksum the zip -> upload
#
# Signing mutates the binary and stapling mutates the ZIP, so a checksum taken
# before either covers bytes that are no longer the ones published. That failure
# is silent here and loud for the user: `shasum -c` mismatches on a file we said
# was correct, which is indistinguishable from tampering -- the exact thing a
# checksum exists to detect, manufactured by us.
set -euo pipefail

SOURCE_DIR="${1:?usage: $0 <source-dir> <archive>}"
ARCHIVE="${2:?usage: $0 <source-dir> <archive>}"
BINARY="ck-astrocyte"

fail() {
  echo "$*" >&2
  exit 1
}

# REFUSE ON THE WRONG IDENTITY RATHER THAN SIGNING WITH IT. An Apple Development
# certificate signs successfully and notarizes never; catching that here names
# the cause, where catching it at notarytool names a submission id.
all_identities="$(security find-identity -v -p codesigning)"
developer_id="$(printf '%s\n' "$all_identities" | awk -F'"' '$2 ~ /^Developer ID Application:/ { print $2 }')"
if [[ -z "$developer_id" ]]; then
  if printf '%s\n' "$all_identities" | grep -Fq '"Apple Development:'; then
    fail "refusing release: only an Apple Development identity is present; notarization requires Developer ID Application"
  fi
  fail "refusing release: no Developer ID Application signing identity in the keychain"
fi
identity_count="$(printf '%s\n' "$developer_id" | awk 'NF { c += 1 } END { print c + 0 }')"
[[ "$identity_count" == "1" ]] || fail "refusing release: ${identity_count} Developer ID identities present, cannot choose"
identity="$(printf '%s\n' "$developer_id" | head -n 1)"

binary_path="${SOURCE_DIR}/${BINARY}"
[[ -f "$binary_path" ]] || fail "missing release binary: ${binary_path}"

# `--options runtime` is required for notarization to be accepted at all.
codesign --force --options runtime --timestamp --identifier "$BINARY" --sign "$identity" "$binary_path"
codesign --verify --strict --verbose=2 "$binary_path"

# RUN IT AFTER SIGNING, not only before. `--options runtime` enables the hardened
# runtime, which changes what the process is permitted to do at launch -- a valid
# signature over a binary that no longer starts verifies perfectly and ships. The
# workflow's earlier version check exercised the UNSIGNED artifact, so this is the
# only place the thing actually published is known to execute.
reported="$("$binary_path" --version)" || fail "refusing release: the signed binary does not run"
echo "signed binary reports: ${reported}"

: "${APP_STORE_CONNECT_API_KEY_PATH:?App Store Connect API key path is required}"
: "${APP_STORE_CONNECT_API_KEY_ID:?App Store Connect API key ID is required}"
: "${APP_STORE_CONNECT_API_ISSUER_ID:?App Store Connect API issuer ID is required}"
[[ -r "$APP_STORE_CONNECT_API_KEY_PATH" ]] || fail "App Store Connect API key is not readable"

# Package AFTER signing so the archive carries the signed binary, and notarize
# the archive itself.
#
# The destination is resolved to an ABSOLUTE path before any `cd`. The obvious
# spelling -- zipping to "${OLDPWD}/${ARCHIVE}" from inside a subshell that has
# cd'd -- happens to work, and depends on subshell OLDPWD semantics to put the
# archive where the caller's checksum step will look for it. If it ever resolved
# elsewhere the zip would be created successfully somewhere nobody reads, and the
# failure would surface as a missing asset at upload time rather than here.
archive_path="$(cd "$(dirname "$ARCHIVE")" && pwd)/$(basename "$ARCHIVE")"
rm -f "$archive_path"
( cd "$SOURCE_DIR" && zip -q -j "$archive_path" "$BINARY" )
[[ -f "$archive_path" ]] || fail "packaging produced no archive at ${archive_path}"
ARCHIVE="$archive_path"

notarization=$(xcrun notarytool submit "$ARCHIVE" \
  --key "$APP_STORE_CONNECT_API_KEY_PATH" \
  --key-id "$APP_STORE_CONNECT_API_KEY_ID" \
  --issuer "$APP_STORE_CONNECT_API_ISSUER_ID" \
  --wait --output-format json)
printf '%s\n' "$notarization"
printf '%s\n' "$notarization" | python3 -c 'import json, sys; sys.exit(json.load(sys.stdin)["status"] != "Accepted")' \
  || fail "refusing release: notarization was not accepted"

# A ZIP cannot always take a ticket immediately. Notarization has already
# succeeded at this point, and an unstapled notarized asset still passes
# Gatekeeper via the online check, so this warns rather than failing the release.
if ! xcrun stapler staple "$ARCHIVE"; then
  echo "::warning title=staple-pending::${ARCHIVE} notarized but not stapled; Gatekeeper needs an online check."
fi

echo "signed and notarized with: ${identity}"
