#!/usr/bin/env bash
set -euo pipefail

# CDC PoC — Fully containerized setup & execution
# Prerequisites: podman + podman-compose (nothing else)
# Copyright: Yves. Apache 2.0

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

GREEN='\033[0;32m'; BLUE='\033[0;34m'; NC='\033[0m'
info() { printf "${BLUE}[INFO]${NC}  %s\n" "$*"; }
ok()   { printf "${GREEN}[OK]${NC}    %s\n" "$*"; }

# ─── Build app image ────────────────────────────────────────────────────
info "Building Perl app image (downloads Oracle Instant Client inside container)..."
podman build -f docker/Dockerfile.app -t cdc-poc-app .
ok "App image built"

# ─── Start Oracle ───────────────────────────────────────────────────────
if podman ps --format '{{.Names}}' | grep -q '^cdc-oracle$'; then
    ok "Oracle container already running"
else
    info "Starting Oracle (first run pulls ~1.5 GB image, init takes ~2 min)..."
    podman-compose up -d oracle
fi

info "Waiting for Oracle to be healthy..."
until podman inspect --format='{{.State.Health.Status}}' cdc-oracle 2>/dev/null | grep -q healthy; do
    printf "."
    sleep 5
done
echo ""
ok "Oracle is ready"

# ─── Run tests ──────────────────────────────────────────────────────────
info "Verifying Oracle listener accepts connections..."
until podman run --rm --network cdc-poc_default \
    -e ORACLE_DSN="dbi:Oracle:host=oracle;port=1521;service_name=FREEPDB1" \
    -e ORACLE_USER=appuser \
    -e ORACLE_PASS=apppass \
    cdc-poc-app \
    perl -MDBI -e 'DBI->connect($ENV{ORACLE_DSN},$ENV{ORACLE_USER},$ENV{ORACLE_PASS},{RaiseError=>1}) or die' 2>/dev/null
do
    printf "."
    sleep 5
done
echo ""
ok "Oracle listener ready for app connections"

info "Running end-to-end tests..."
podman run --rm \
    --network cdc-poc_default \
    -e ORACLE_DSN="dbi:Oracle:host=oracle;port=1521;service_name=FREEPDB1" \
    -e ORACLE_USER=appuser \
    -e ORACLE_PASS=apppass \
    cdc-poc-app

ok "Done — all tests passed"

cat <<'EOF'

  Useful commands:
    podman run --rm --network cdc-poc_default \
      -e ORACLE_DSN="dbi:Oracle:host=oracle;port=1521;service_name=FREEPDB1" \
      -e ORACLE_USER=appuser -e ORACLE_PASS=apppass cdc-poc-app
                                       — re-run tests
    podman logs cdc-oracle             — Oracle logs
    podman-compose down -v             — tear down everything

EOF
