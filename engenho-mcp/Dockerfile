# Multi-stage Docker build for engenho-mcp.
#
# Build target = the engenho-mcp binary cross-compiled for the
# container's target triple. Runtime = distroless cc-debian12
# (minimal glibc; no shell, no package manager).
#
# Published to ghcr.io/pleme-io/engenho-mcp on every v* tag via
# .github/workflows/release.yml's docker-publish job.

# ============================================================
# Stage 1 — build
# ============================================================
FROM rust:1.89-bookworm AS build

WORKDIR /workspace
COPY . .

# Build the engenho-mcp binary in release mode against the
# target triple. The image stays small because we only copy the
# resulting binary to the runtime stage.
RUN cargo build --release --bin engenho-mcp \
    && cp target/release/engenho-mcp /tmp/engenho-mcp \
    && strip /tmp/engenho-mcp

# ============================================================
# Stage 2 — runtime
# ============================================================
FROM gcr.io/distroless/cc-debian12

LABEL org.opencontainers.image.title="engenho-mcp"
LABEL org.opencontainers.image.description="engenho MCP server — operator + LLM surface for engenho-managed clusters"
LABEL org.opencontainers.image.source="https://github.com/pleme-io/engenho"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.vendor="pleme-io"

COPY --from=build /tmp/engenho-mcp /usr/local/bin/engenho-mcp

ENTRYPOINT ["/usr/local/bin/engenho-mcp"]
