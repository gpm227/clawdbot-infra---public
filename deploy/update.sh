#!/bin/bash
set -euo pipefail

# =============================================================================
# NicheIndex — Update Script
# Run after each git push to pull latest code and restart services.
# =============================================================================

echo "=== NicheIndex Update ==="
echo ""

# ---------- 1. Update clawdbot-infra ----------
echo "[1/4] Pulling clawdbot-infra..."
cd /home/ubuntu/clawdbot-infra
git pull origin main

# ---------- 2. Update clawdbot-infra pip deps ----------
echo "[2/4] Updating clawdbot-infra pip dependencies..."
/home/ubuntu/clawdbot-infra/.venv/bin/pip install --quiet -r /home/ubuntu/clawdbot-infra/requirements.txt

# ---------- 3. Update nicheindex pipeline ----------
echo "[3/4] Pulling nicheindex..."
if [ -d /home/ubuntu/nicheindex ]; then
  cd /home/ubuntu/nicheindex
  git pull origin main

  PIPELINE_REQ="/home/ubuntu/nicheindex/pipeline/requirements.txt"
  if [ -f "$PIPELINE_REQ" ] && [ -d /home/ubuntu/nicheindex/pipeline/.venv ]; then
    echo "  Updating pipeline pip dependencies..."
    /home/ubuntu/nicheindex/pipeline/.venv/bin/pip install --quiet -r "$PIPELINE_REQ"
  fi
else
  echo "  WARNING: /home/ubuntu/nicheindex not found, skipping."
fi

# ---------- 4. Restart services ----------
echo "[4/4] Restarting orchestrator..."
sudo systemctl restart orchestrator

# ---------- Done ----------
echo ""
echo "=== Update Complete ==="
echo ""
sudo systemctl status orchestrator.service --no-pager -l || true
echo ""
sudo systemctl status watchdog.timer --no-pager -l || true
