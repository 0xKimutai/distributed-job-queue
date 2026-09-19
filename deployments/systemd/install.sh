#!/bin/bash
# Installs jobqueue binaries and systemd units on a Linux server.
# Run as root: sudo bash deployments/systemd/install.sh
#
# What this does:
#   1. Creates a dedicated non-root system user 'jobqueue'
#   2. Copies binaries to /usr/local/bin/
#   3. Copies migrations to /usr/local/share/jobqueue/
#   4. Installs and enables systemd units
#   5. Prompts to configure /etc/jobqueue/env

set -euo pipefail

BINARY_DIR="/usr/local/bin"
SHARE_DIR="/usr/local/share/jobqueue"
SYSTEMD_DIR="/etc/systemd/system"
ENV_DIR="/etc/jobqueue"

echo "==> Creating jobqueue system user..."
# System user: no home directory, no login shell, no password.
# Services should never run as root — limits damage if compromised.
useradd --system --no-create-home --shell /usr/sbin/nologin jobqueue 2>/dev/null || echo "User already exists."

echo "==> Installing binaries..."
go build -ldflags="-s -w" -o "$BINARY_DIR/jobqueue-api" ./cmd/api/
go build -ldflags="-s -w" -o "$BINARY_DIR/jobqueue-worker" ./cmd/worker/
# -ldflags="-s -w" strips debug symbols — reduces binary size ~30%
chmod 755 "$BINARY_DIR/jobqueue-api" "$BINARY_DIR/jobqueue-worker"

echo "==> Installing migrations..."
mkdir -p "$SHARE_DIR/migrations"
cp migrations/*.sql "$SHARE_DIR/migrations/"
chown -R jobqueue:jobqueue "$SHARE_DIR"

echo "==> Installing systemd units..."
cp deployments/systemd/jobqueue-api.service "$SYSTEMD_DIR/"
cp deployments/systemd/jobqueue-worker.service "$SYSTEMD_DIR/"
systemctl daemon-reload

echo "==> Setting up environment file..."
mkdir -p "$ENV_DIR"
if [ ! -f "$ENV_DIR/env" ]; then
    cp deployments/systemd/jobqueue.env.example "$ENV_DIR/env"
    echo ""
    echo "IMPORTANT: Edit /etc/jobqueue/env and set DATABASE_URL before starting services."
    echo ""
fi
# Only root and jobqueue group can read — protects database credentials.
chown root:jobqueue "$ENV_DIR/env"
chmod 640 "$ENV_DIR/env"

echo "==> Enabling services (will start on next boot)..."
systemctl enable jobqueue-api.service
systemctl enable jobqueue-worker.service

echo ""
echo "Installation complete. Next steps:"
echo "  1. Edit /etc/jobqueue/env — set DATABASE_URL"
echo "  2. sudo systemctl start jobqueue-api"
echo "  3. sudo systemctl start jobqueue-worker"
echo "  4. sudo journalctl -u jobqueue-api -f    # watch logs"
echo "  5. sudo systemctl status jobqueue-api    # check status"
