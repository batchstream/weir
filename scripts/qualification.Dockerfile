ARG BASE
FROM ${BASE}
ARG TARGETARCH
ARG SOURCE_REVISION
ARG SOURCE_DATE_EPOCH
COPY --chmod=0555 linux-${TARGETARCH}/qualification /qualification
USER 65532:65532
WORKDIR /
LABEL org.opencontainers.image.title="weir-qualification" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.source="https://github.com/batchstream/weir" \
      org.opencontainers.image.description="Internal qualification helper; no production or capacity qualification implied"
ENTRYPOINT ["/qualification"]
