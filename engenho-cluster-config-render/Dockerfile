# Multi-stage Docker build for engenho-cluster-config-render.
#
# A one-shot CLI that renders typed engenho/k3s cluster bootstrap
# configs. Used by FluxCD InitContainers and operator workflows
# that want to materialize the typed config to disk before the
# cluster boots.

FROM rust:1.89-bookworm AS build
WORKDIR /workspace
COPY . .
RUN cargo build --release --bin engenho-cluster-config-render \
    && cp target/release/engenho-cluster-config-render /tmp/render \
    && strip /tmp/render

FROM gcr.io/distroless/cc-debian12

LABEL org.opencontainers.image.title="engenho-cluster-config-render"
LABEL org.opencontainers.image.description="Typed k3s/engenho cluster bootstrap config renderer"
LABEL org.opencontainers.image.source="https://github.com/pleme-io/engenho"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.vendor="pleme-io"

COPY --from=build /tmp/render /usr/local/bin/engenho-cluster-config-render

ENTRYPOINT ["/usr/local/bin/engenho-cluster-config-render"]
