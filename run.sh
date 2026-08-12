#!/usr/bin/env bash
# Builds the engine (if needed) and starts it alongside the monitoring relay.
# Open monitor/dashboard.html in a browser afterwards.
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -f engine/target/release/mm_engine ]; then
  echo "==> building engine (release, first run only)..."
  (cd engine && cargo build --release)
fi

if ! python3 -c "import websockets" 2>/dev/null; then
  echo "==> installing monitor dependencies..."
  pip3 install -r monitor/requirements.txt --break-system-packages 2>/dev/null \
    || pip3 install -r monitor/requirements.txt
fi

echo "==> starting engine..."
./engine/target/release/mm_engine "$@" &
ENGINE_PID=$!

sleep 1

echo "==> starting relay..."
(cd monitor && python3 relay.py) &
RELAY_PID=$!

echo ""
echo "engine running (pid $ENGINE_PID), relay running (pid $RELAY_PID)."
echo "open monitor/dashboard.html in a browser now."
echo "press Ctrl+C to stop both."

trap "kill $ENGINE_PID $RELAY_PID 2>/dev/null" EXIT
wait
