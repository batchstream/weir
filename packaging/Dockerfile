ARG BASE
FROM ${BASE}
ARG TARGETARCH
ARG SOURCE_REVISION
ARG SOURCE_DATE_EPOCH
COPY --chmod=0555 linux-${TARGETARCH}/weir /weir
COPY --chmod=0555 linux-${TARGETARCH}/weir-lua-worker /weir-lua-worker
USER 65532:65532
WORKDIR /
LABEL org.opencontainers.image.title="weir" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.source="https://github.com/batchstream/weir" \
      weir.owner="weir-local-packaging"
ENTRYPOINT ["/weir"]
