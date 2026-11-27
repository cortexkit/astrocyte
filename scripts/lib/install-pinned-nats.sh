#!/usr/bin/env bash
# Install only the release archive whose exact SHA-256 CI pins.
set -euo pipefail

target=$1
directory=$2
version=${NATS_SERVER_VERSION:?expected the CI release version}
sha=${NATS_SERVER_SHA256:?expected the CI archive checksum}
if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ && "$sha" =~ ^[a-f0-9]{64}$ ]]; then
  echo "invalid NATS release version or archive checksum" >&2
  exit 1
fi

release="nats-server-v$version-$target"
case "$target" in
  linux-amd64|darwin-arm64) suffix=tar.gz ;;
  windows-amd64) suffix=zip ;;
  *) echo "unsupported NATS release target: $target" >&2; exit 1 ;;
esac
mkdir -p "$directory"
archive=$(mktemp "$directory/nats-server.XXXXXX")
trap 'rm -f "$archive"' EXIT
curl --fail --location --retry 3 \
  "https://github.com/nats-io/nats-server/releases/download/v$version/$release.$suffix" \
  --output "$archive"
if [ "$target" = darwin-arm64 ]; then
  printf '%s  %s\n' "$sha" "$archive" | shasum -a 256 --check -
else
  printf '%s  %s\n' "$sha" "$archive" | sha256sum --check -
fi

if [ "$target" = windows-amd64 ]; then
  # Windows ships bsdtar, which can extract ZIP files regardless of suffix.
  "$(cygpath "$SYSTEMROOT")/System32/tar.exe" -xf "$(cygpath -w "$archive")" \
    -C "$(cygpath -w "$directory")" --strip-components=1 "$release/nats-server.exe"
  "$directory/nats-server.exe" --version
else
  tar -xzf "$archive" --strip-components=1 -C "$directory" "$release/nats-server"
  "$directory/nats-server" --version
fi
