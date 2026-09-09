#!/usr/bin/env bash
# Hardened Archon runner entrypoint.
#
# The controller seeds a per-run named workspace volume before this container is
# started. This entrypoint only verifies that the volume is mounted at the exact
# workspace path and writable by the non-root agent user, then idles for later
# `docker exec` calls. It never mounts host paths or overlay filesystems.
set -euo pipefail

WS="${ARCHON_WORKSPACE_PATH:?ARCHON_WORKSPACE_PATH must be set}"
READY=/tmp/archon-container-ready

rm -f "$READY"

if [ ! -d "$WS" ]; then
  echo "archon-runner: FATAL — workspace volume missing at ${WS}" >&2
  exit 1
fi

if [ -e "$WS/.git" ]; then
  echo "archon-runner: FATAL — seeded workspace still contains .git metadata" >&2
  exit 1
fi

if ! touch "$WS/.archon-write-test" 2>/tmp/archon-write.err; then
  echo "archon-runner: FATAL — workspace volume is not writable by $(id -un)" >&2
  cat /tmp/archon-write.err >&2 2>/dev/null || true
  exit 1
fi
rm -f "$WS/.archon-write-test"

mkdir -p "${CLAUDE_CONFIG_DIR:-$HOME/.claude}" "$HOME/.cache" "$HOME/.local/state"

if [ -n "${ARCHON_PROXY_SOCKET:-}" ]; then
  bun /usr/local/lib/archon/egress/proxy-shim.ts &
fi

touch "$READY"

echo "archon-runner: hardened workspace ready at ${WS} as $(id -un)"
exec sleep infinity
