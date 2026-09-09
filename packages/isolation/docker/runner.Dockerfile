# Archon hardened container-isolation runner image.
#
# Runs a folder-project workflow from controller-seeded per-run named volumes.
# The agent-facing container never mounts the live worktree, live .git, host
# home, credential files, or Docker socket. Claude, bash: nodes, and script:
# nodes all execute in here as the non-root archon user.
#
# Build (tag with the Archon version, e.g. archon-runner:0.5.0):
#   docker build -t archon-runner:<version> \
#     -f packages/isolation/docker/runner.Dockerfile packages/isolation/docker
# Or: bun run build:runner-image
#
# Supply chain: the base image is pinned by digest and every tool below is
# version-pinned so the image is reproducible and a compromised installer
# endpoint can't silently pull a newer/tampered binary. The vendor installer
# SCRIPTS themselves are still fetched over TLS at build time and are not
# checksum-verified (they aren't published with stable checksums) — an accepted,
# documented residual (see SECURITY.md). Bump the *_VERSION args deliberately.
FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

ENV DEBIAN_FRONTEND=noninteractive

# Pinned tool versions (bump deliberately; keep in sync with the version the
# maintainer validated). CLAUDE_VERSION 'stable' / 'latest' / 'X.Y.Z' accepted.
ARG CLAUDE_VERSION=2.1.211
ARG BUN_VERSION=1.3.14
ARG CODEX_VERSION=0.144.5
ARG UV_VERSION=0.11.29

# Runtime deps: git/bash/rsync for workflow work, ca-certificates+curl for the
# installers, procps for in-container process signalling, nodejs for Codex CLI (the Claude spawn kills
# by pid across `docker exec`), unzip/xz for the bun/claude installers.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    bash \
    rsync \
    procps \
    nodejs \
    unzip \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Claude Code native binary (self-contained), pinned to $CLAUDE_VERSION.
RUN curl -fsSL https://claude.ai/install.sh | bash -s "${CLAUDE_VERSION}" \
    && test -x /root/.local/bin/claude \
    || (echo "FATAL: claude binary not found after install" >&2 && exit 1)

# bun (script: nodes with runtime: bun) → /root/.bun/bin/bun, pinned to $BUN_VERSION.
RUN curl -fsSL https://bun.sh/install | bash -s "bun-v${BUN_VERSION}" \
    && test -x /root/.bun/bin/bun \
    || (echo "FATAL: bun not found after install" >&2 && exit 1)

# Codex CLI for Codex provider container transport.
RUN /root/.bun/bin/bun add --global "@openai/codex@${CODEX_VERSION}" \
    && test -x /root/.bun/bin/codex \
    || (echo "FATAL: codex not found after install" >&2 && exit 1)

# uv (script: nodes with runtime: uv) → /root/.local/bin/uv, pinned via the
# versioned installer URL (https://astral.sh/uv/<version>/install.sh).
RUN curl -LsSf "https://astral.sh/uv/${UV_VERSION}/install.sh" | sh \
    && test -x /root/.local/bin/uv \
    || (echo "FATAL: uv not found after install" >&2 && exit 1)

# Move installed tools out of /root so the non-root runtime user can execute them.
# Codex is ESM and needs its package.json/type + native package, so preserve the
# global node_modules tree rather than copying only the generated bin shim.
RUN mkdir -p /opt/codex \
    && cp -a /root/.bun/install/global/node_modules /opt/codex/node_modules \
    && printf '%s\n' '#!/bin/sh' 'exec node /opt/codex/node_modules/@openai/codex/bin/codex.js "$@"' > /usr/local/bin/codex \
    && cp /root/.local/bin/claude /usr/local/bin/claude \
    && cp /root/.local/bin/uv /usr/local/bin/uv \
    && cp /root/.bun/bin/bun /usr/local/bin/bun \
    && chmod 0755 /usr/local/bin/claude /usr/local/bin/uv /usr/local/bin/bun /usr/local/bin/codex

RUN useradd --create-home --uid 1000 --shell /bin/bash archon \
    && mkdir -p /home/archon/.claude /home/archon/.cache /home/archon/.local/state \
    && chown -R archon:archon /home/archon

# The controller mounts the seeded workspace at the host's absolute cwd inside
# the container. Trust every workspace path (single-purpose run container).
RUN git config --system --add safe.directory '*'

ENV IS_SANDBOX=1
ENV HOME=/home/archon
ENV CLAUDE_CONFIG_DIR=/home/archon/.claude
ENV PATH="/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"

COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY src/egress /usr/local/lib/archon/egress
RUN bun build /usr/local/lib/archon/egress/strict-https-proxy-cli.ts \
      --target=node --outfile /usr/local/lib/archon/egress/strict-https-proxy-cli.mjs \
    && test -f /usr/local/lib/archon/egress/proxy-budget-ledger-cli.ts \
    && /usr/local/bin/bun --version \
    && chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
