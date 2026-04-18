FROM golang:1.26-alpine AS build
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/indexer ./cmd/indexer

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata wget && adduser -D -H -u 10001 indexer
WORKDIR /app
COPY --from=build /out/indexer /usr/local/bin/indexer
# Bake a sane default config + the starter aggregate so the container runs
# out of the box with just env-var overrides (LAVA_RPC_ENDPOINT, DB_HOST,
# DB_PASSWORD, etc). Bind-mount over /app if you want to override wholesale
# — the Go binary reads whichever file is at -config.
COPY config.example.yml /app/config.yml
COPY aggregates /app/aggregates
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null || exit 1
USER indexer
ENTRYPOINT ["/usr/local/bin/indexer"]
CMD ["-config", "/app/config.yml"]
