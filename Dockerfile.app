FROM weknora-notice-builder:1acad0539 AS builder
WORKDIR /app
ENV COMMIT_ID=10b0d9f35a9f2a6df66cbd2bd645c01f9fe5cbc2
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod make build-prod GO_BUILD_TAGS=anydoc
RUN --mount=type=cache,target=/go/pkg/mod go build -o /app/original-body-retention ./cmd/original-body-retention \
    && go build -o /app/publication-recovery ./cmd/publication-recovery \
    && go build -o /app/capacity-reconcile ./cmd/capacity-reconcile \
    && go build -o /app/dingtalk-wiki-recovery ./cmd/dingtalk-wiki-recovery

FROM weknora-production-app:1acad0539ebaad44d2608e4d832a7dd78d29dffa
COPY --from=builder /app/WeKnora /app/WeKnora
COPY --from=builder /app/original-body-retention /usr/local/bin/original-body-retention
COPY --from=builder /app/publication-recovery /usr/local/bin/publication-recovery
COPY --from=builder /app/capacity-reconcile /usr/local/bin/capacity-reconcile
COPY --from=builder /app/dingtalk-wiki-recovery /usr/local/bin/dingtalk-wiki-recovery
ENV WEKNORA_BUILD_COMMIT=10b0d9f35a9f2a6df66cbd2bd645c01f9fe5cbc2
