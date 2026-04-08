#!/usr/bin/env bash
set -euo pipefail

# dev.sh — Developer convenience script
# Copyright: Yves. Apache 2.0

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

usage() {
    cat <<'EOF'
Usage: ./dev.sh <command>

Commands:
  setup     Install CPAN dependencies into ./local/
  test      Run all tests (unit + SQLite e2e)
  clean     Remove ./local/ and build artifacts
EOF
    exit 1
}

cmd_setup() {
    echo "Installing dependencies into ./local/ ..."
    if ! command -v cpanm >/dev/null 2>&1; then
        echo "cpanm not found — installing via cpanmin.us ..."
        curl -fsSL https://cpanmin.us | perl - --local-lib=./local --installdeps .
    else
        cpanm --local-lib=./local --installdeps .
    fi
    echo "Done. Run: ./dev.sh test"
}

cmd_test() {
    export PERL5LIB="${SCRIPT_DIR}/local/lib/perl5:${SCRIPT_DIR}/lib"
    prove -lv t/
}

cmd_clean() {
    rm -rf local/ blib/ pm_to_blib Makefile MYMETA.*
    echo "Cleaned."
}

[[ $# -ge 1 ]] || usage

case "$1" in
    setup) cmd_setup ;;
    test)  cmd_test  ;;
    clean) cmd_clean ;;
    *)     usage     ;;
esac
