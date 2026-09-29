# syntax=docker/dockerfile:1

# ─── Stage 1: build ──────────────────────────────────────────────────────────
FROM node:22-alpine AS builder

WORKDIR /app

# Install dependencies first (layer cache)
COPY package*.json ./
RUN npm ci

# Copy source and build TypeScript
COPY tsconfig.json ./
COPY src/ ./src/
RUN npm run build

# Prune dev dependencies
RUN npm prune --omit=dev

# ─── Stage 2: runtime ────────────────────────────────────────────────────────
FROM node:22-alpine AS runtime

# Non-root user for security
RUN addgroup -S affine && adduser -S affine -G affine

WORKDIR /app

# Copy only what is needed to run
COPY --from=builder --chown=affine:affine /app/node_modules ./node_modules
COPY --from=builder --chown=affine:affine /app/dist ./dist
COPY --chown=affine:affine bin/ ./bin/
COPY --chown=affine:affine package.json ./
COPY --chown=affine:affine tool-manifest.json ./

# Fixed in-container export location for export_workspace. Mount a host
# directory here in docker-compose (e.g. -v /host/backups:/affine-backup) to
# persist exported .affine files outside the container.
RUN mkdir -p /affine-backup && chown affine:affine /affine-backup

USER affine

EXPOSE 3002

ENV MCP_TRANSPORT=http \
    AFFINE_MCP_HTTP_HOST=0.0.0.0 \
    PORT=3002 \
    AFFINE_EXPORT_DIR=/affine-backup

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    # 用 127.0.0.1 而非 localhost: busybox wget 会先试 IPv6 ::1,
    # 而 node 服务只绑定 IPv4 0.0.0.0,导致健康检查 connection refused
    CMD wget -qO- http://127.0.0.1:${PORT}/healthz || exit 1

ENTRYPOINT ["node", "bin/affine-mcp"]
