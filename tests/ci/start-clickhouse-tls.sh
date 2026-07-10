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

for _ in $(seq 1 60); do
  if timeout 1 bash -c "</dev/tcp/127.0.0.1/9440" 2>/dev/null; then
    break
  fi
  sleep 1
done

timeout 1 bash -c "</dev/tcp/127.0.0.1/9440"
echo "CLICKHOUSE_TLS_DSN=clickhouse://default:@localhost:9440/default?secure=true&verify=true&ca_certs=$tls_dir/server.crt&server_hostname=localhost" >>"$GITHUB_ENV"
