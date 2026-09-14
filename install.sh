#!/bin/bash
# Install vramble: files under the base dir, a systemd user unit, and the CLIs in ~/.local/bin.
# Paths and endpoints live in config.yaml (see config.example.yaml), never in the code.
set -e
D=$(cd "$(dirname "$0")" && pwd)
read_cfg() { python3 -c "import sys; sys.path.insert(0,'$D'); import config; print(config.C['$1'])"; }
BASE=$(read_cfg base)
# Once the base is known, read the config that vramble will actually use: the one living there. Without
# this, the installer and the daemon can disagree on the port, and the health check checks nothing.
if [ -f "$BASE"/config.yaml ]; then
  export VRAMBLE_CONFIG="$BASE"/config.yaml
  BASE=$(read_cfg base)
fi
PORT=$(read_cfg port)
BIN=${VRAMBLE_BIN:-$HOME/.local/bin}

mkdir -p "$BASE" "$BIN" ~/.config/systemd/user
cp "$D"/vramble.py "$D"/jobs.py "$D"/catalog.py "$D"/config.py "$D"/check_catalog.py "$BASE"/
if [ ! -f "$BASE"/config.yaml ]; then
  cp "$D"/config.example.yaml "$BASE"/config.yaml
  echo "Created $BASE/config.yaml from the example."
  echo "Check it (ports, endpoints, paths) and run this script again."
  exit 0          # going on would install against values nobody has read yet
fi
for f in activities services; do
  [ -f "$BASE"/$f.yaml ] || cp "$D"/$f.example.yaml "$BASE"/$f.yaml
done

python3 "$BASE"/check_catalog.py || echo "⚠️  the catalog needs fixing (see above)"

cp "$D"/gpu-lease "$D"/gpu-job "$BIN"/
chmod +x "$BIN"/gpu-lease "$BIN"/gpu-job

# VRAMBLE_NO_SERVICE=1: install the files and stop. For a machine without systemd, or to try the
# package out without touching a running unit.
if [ -n "$VRAMBLE_NO_SERVICE" ]; then
  echo "Files installed in $BASE, CLIs in $BIN."
  echo "Start it with: python3 $BASE/vramble.py"
  exit 0
fi

loginctl enable-linger "$USER" 2>/dev/null || true
sed -e "s|__BASE__|$BASE|g" -e "s|__PYTHON__|$(command -v python3)|g" \
    "$D"/vramble.service > ~/.config/systemd/user/vramble.service
systemctl --user daemon-reload
systemctl --user enable vramble
systemctl --user restart vramble
for i in $(seq 1 20); do
  curl -sf -m 2 "http://127.0.0.1:$PORT/status?text=1" && exit 0
  sleep 1
done
echo "VRAMBLE DID NOT START"; tail -20 "$BASE"/vramble.log 2>/dev/null
exit 1
