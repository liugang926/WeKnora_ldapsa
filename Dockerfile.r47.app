FROM r47-builder:77ebcaed AS builder
WORKDIR /r47-source
ENV COMMIT_ID=f36413981986916d5359f073cc6815ddb2078bca
COPY . .
RUN mkdir -p /r47-source/third_party/anydoc-go/lib/linux_amd64_gnu \
    && cp /app/third_party/anydoc-go/lib/linux_amd64_gnu/libanydoc_go.a /r47-source/third_party/anydoc-go/lib/linux_amd64_gnu/ \
    && echo "331c050e3fc7541a65298c31169faeec16a1e6914e4295ead0e66e4e36be01ce  /r47-source/third_party/anydoc-go/lib/linux_amd64_gnu/libanydoc_go.a" | sha256sum -c -
RUN --mount=type=cache,target=/go/pkg/mod make build-prod GO_BUILD_TAGS=anydoc
RUN --mount=type=cache,target=/go/pkg/mod go build -tags anydoc -o /r47-source/original-body-retention ./cmd/original-body-retention \
    && go build -tags anydoc -o /r47-source/publication-recovery ./cmd/publication-recovery \
    && go build -tags anydoc -o /r47-source/capacity-reconcile ./cmd/capacity-reconcile \
    && go build -tags anydoc -o /r47-source/dingtalk-wiki-recovery ./cmd/dingtalk-wiki-recovery

FROM r47-runtime:0618ff14
COPY --from=builder /r47-source/WeKnora /app/WeKnora
COPY --from=builder /r47-source/original-body-retention /usr/local/bin/original-body-retention
COPY --from=builder /r47-source/publication-recovery /usr/local/bin/publication-recovery
COPY --from=builder /r47-source/capacity-reconcile /usr/local/bin/capacity-reconcile
COPY --from=builder /r47-source/dingtalk-wiki-recovery /usr/local/bin/dingtalk-wiki-recovery
ENV WEKNORA_BUILD_COMMIT=f36413981986916d5359f073cc6815ddb2078bca
