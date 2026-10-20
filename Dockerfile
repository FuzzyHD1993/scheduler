# ──────────────────────────────────────────────────────────────
# Go Scheduler — multi-stage: статическая сборка + минимальный runtime
# Build:   docker build --build-arg VERSION=v1.0.0 -t scheduler .
# ──────────────────────────────────────────────────────────────

# ---------- builder ----------
FROM golang:1.27-alpine AS builder

ARG VERSION=dev

WORKDIR /src

# Модули кэшируются отдельным слоем: пересоздаётся только при изменении
# go.mod/go.sum — copy'им их до исходников.
COPY go.mod go.sum ./
RUN go mod download

# Исходники (migrations/ нужен для go:embed)
COPY . .

RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/scheduler ./cmd/scheduler

# ---------- runtime ----------
FROM alpine:3.20 AS runtime

# ca-certificates: HTTPS для http-задач; tzdata: корректные таймзоны/логи.
# non-root пользователь UID/GID 10001.
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -g 10001 -S scheduler \
    && adduser -u 10001 -S -D -H -s /sbin/nologin -G scheduler scheduler

COPY --from=builder /out/scheduler /scheduler

USER scheduler

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
    CMD wget -qO- http://127.0.0.1:8080/health || exit 1

ENTRYPOINT ["/scheduler"]
