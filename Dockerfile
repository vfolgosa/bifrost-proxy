FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bifrost ./cmd/proxy/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata curl
COPY --from=builder /bifrost /usr/local/bin/bifrost

EXPOSE 8080 9094

HEALTHCHECK --interval=10s --timeout=3s \
    CMD curl -sf http://localhost:8080/health || exit 1

ENTRYPOINT ["bifrost"]
CMD ["-config", "/etc/bifrost/config.yaml"]
