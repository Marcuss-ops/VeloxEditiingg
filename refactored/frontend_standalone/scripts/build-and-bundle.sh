#!/usr/bin/env bash
# Build the Velox frontend and produce a versioned tarball + sha256.
# Mirrors the GitHub Actions release job so local builds match CI artifacts.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

VERSION="${VERSION:-$(git describe --tags --always 2>/dev/null || date -u +%Y%m%d-%H%M%S)}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/dist}"
TARBALL_NAME="frontend-$VERSION.tar.gz"

echo "[build] version=$VERSION workspace=web node=$(node --version)"
echo "[build] installing web dependencies"
(cd web && npm ci)

echo "[build] running web production build"
(cd web && npm run build)

DEST="$OUT_DIR/$VERSION"
rm -rf "$DEST"
mkdir -p "$DEST"
# Vite 7 emits to web/dist/ (not web/build/). Anything still under web/build/
# is a stale SvelteKit-era artifact.
cp -R web/dist/. "$DEST/"

TARBALL="$OUT_DIR/$TARBALL_NAME"
tar -C "$DEST" -czf "$TARBALL" .

# sha256sum over the deterministic tarball
SHA="$(sha256sum "$TARBALL" | awk '{print $1}')"
SHA_FILE="$TARBALL.sha256"
{
  echo "$SHA  $TARBALL_NAME"
  echo "# Build version : $VERSION"
  echo "# Built at UTC  : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# Node          : $(node --version)"
} > "$SHA_FILE"

echo "[build] artifact : $TARBALL"
echo "[build] size     : $(du -h "$TARBALL" | awk '{print $1}')"
echo "[build] sha256   : $SHA"
echo "[build] sidecar  : $SHA_FILE"

cat "$SHA_FILE"
