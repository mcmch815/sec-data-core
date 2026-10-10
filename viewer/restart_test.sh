#!/usr/bin/env bash
# restart_test.sh — restart viewer against the test DB
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

export MART_DB="$REPO_DIR/test_db/test_annual.db"
exec "$SCRIPT_DIR/restart.sh"
