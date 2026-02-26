#!/usr/bin/env bash
set -euo pipefail

REPO_SLUG="${1:-gabry-ts/framekit}"
WIKI_URL="https://github.com/${REPO_SLUG}.wiki.git"
SRC_DIR="wiki"
TMP_DIR="$(mktemp -d)"

cleanup() {
	rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

if [[ ! -d "${SRC_DIR}" ]]; then
	echo "[sync-wiki] source directory '${SRC_DIR}' not found"
	exit 1
fi

if ! git ls-remote "${WIKI_URL}" >/dev/null 2>&1; then
	echo "[sync-wiki] wiki remote not available yet: ${WIKI_URL}"
	echo "[sync-wiki] initialize wiki once from GitHub UI, then rerun"
	exit 0
fi

git clone "${WIKI_URL}" "${TMP_DIR}/wiki-repo" >/dev/null 2>&1
cp -f "${SRC_DIR}"/*.md "${TMP_DIR}/wiki-repo/"

cd "${TMP_DIR}/wiki-repo"

if [[ -z "$(git status --porcelain)" ]]; then
	echo "[sync-wiki] no wiki changes"
	exit 0
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git add .
git commit -m "docs: sync wiki content"
git push

echo "[sync-wiki] wiki updated"
