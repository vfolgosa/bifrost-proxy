# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Bifrost, please report it privately by emailing the maintainers or opening a private security advisory on GitHub. Do not open a public issue.

## Production Hardening

Bifrost is designed for internal infrastructure use. For production deployments:

### Network

- **Bind metrics to localhost** — set `bind_address: "127.0.0.1"` in the proxy config if metrics/dashboard should not be publicly accessible.
- **Firewall the metrics port** — port 8080 exposes operational data including cluster topology. Restrict access.
- **Use a reverse proxy with TLS** — the proxy accepts plain TCP from clients. Run it behind a TLS-terminating reverse proxy (nginx, haproxy, Envoy) or within a service mesh.

### Authentication

- **Use strong SASL credentials** — configure `health_check.sasl_username` and `health_check.sasl_password` with strong, unique credentials.
- **Never commit secrets** — `secrets/`, `*.jaas.conf`, and `config.local.yaml` are already in `.gitignore`. Do not override this.
- **Rotate credentials regularly** — the health check credentials should be rotated on the same schedule as your Kafka credentials.

### Operations

- **Monitor failover events** — track `proxy_failover_total` in Prometheus and alert on unexpected failovers.
- **Review the `/status` endpoint** — it exposes cluster topology. Consider restricting access or redacting bootstrap addresses in production.
- **Keep the proxy updated** — subscribe to releases for security patches.

## Known Limitations

- **No TLS on proxy listener** — client-to-proxy traffic is plain TCP. This is a design choice for port-based routing without SNI. Use a sidecar proxy or service mesh for encryption.
- **No built-in authentication on HTTP endpoints** — `/metrics`, `/status`, `/topic-stats`, and the dashboard are unauthenticated. Use a reverse proxy or firewall rules.
- **Health check SASL required** — if your upstream Kafka uses SASL, you must configure `sasl_username` and `sasl_password` in the health check config.

## Dependency Security

Dependencies are managed via `go.mod`. Run `go mod tidy` and review `go.sum` before releases. Consider using `govulncheck` in CI:

```bash
go install golang.org/x/vuln/cmd/govulncheck@latest
govulncheck ./...
```
