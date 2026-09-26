#!/usr/bin/env bash
set -euo pipefail

tls_dir="${RUNNER_TEMP:-/tmp}/asynch-clickhouse-tls"
mkdir -p "$tls_dir"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$tls_dir/server.key" \
  -out "$tls_dir/server.crt" \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
chmod 644 "$tls_dir/server.key"

cat >"$tls_dir/tls.xml" <<'XML'
<clickhouse>
  <tcp_port_secure>9440</tcp_port_secure>
  <openSSL>
    <server>
      <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>
      <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
      <verificationMode>none</verificationMode>
      <loadDefaultCAFile>true</loadDefaultCAFile>
      <cacheSessions>true</cacheSessions>
      <disableProtocols>sslv2,sslv3</disableProtocols>
      <preferServerCiphers>true</preferServerCiphers>
    </server>
  </openSSL>
</clickhouse>
XML

docker run --detach --name asynch-clickhouse-tls \
  --env CLICKHOUSE_SKIP_USER_SETUP=1 \
  --publish 9440:9440 \
  --volume "$tls_dir:/etc/clickhouse-server/certs:ro" \
  --volume "$tls_dir/tls.xml:/etc/clickhouse-server/config.d/tls.xml:ro" \
  clickhouse/clickhouse-server:latest

# Wait for a completed TLS handshake, not an open TCP port: docker-proxy
# accepts on the published port before ClickHouse listens behind it, so a
# plain connect succeeds at once and the test then races server startup.
tls_ready() {
  timeout 2 openssl s_client -connect 127.0.0.1:9440 -servername localhost \
    </dev/null >/dev/null 2>&1
}

for _ in $(seq 1 60); do
  if tls_ready; then
    break
  fi
  sleep 1
done

tls_ready
echo "CLICKHOUSE_TLS_DSN=clickhouse://default:@localhost:9440/default?secure=true&verify=true&ca_certs=$tls_dir/server.crt&server_hostname=localhost" >>"$GITHUB_ENV"
