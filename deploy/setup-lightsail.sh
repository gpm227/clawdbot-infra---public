#!/bin/bash
set -euo pipefail

# =============================================================================
# NicheIndex — Lightsail One-Time Setup
# Run on a fresh Ubuntu/OpenClaw instance to bootstrap the pipeline environment.
# =============================================================================

echo "=== NicheIndex Lightsail Setup ==="
echo ""

# ---------- 1. System packages ----------
echo "[1/7] Installing system packages..."
sudo apt-get update -qq
sudo apt-get install -y -qq python3-pip python3-venv git postgresql-client

# ---------- 2. Clone repos ----------
echo "[2/7] Cloning repos..."

if [ -d /home/ubuntu/clawdbot-infra ]; then
  echo "  clawdbot-infra already present, skipping clone."
else
  git clone https://github.com/gpm227/clawdbot-infra---public.git /home/ubuntu/clawdbot-infra
fi

if [ -d /home/ubuntu/nicheindex ]; then
  echo "  nicheindex already present, skipping clone."
else
  echo "  WARNING: nicheindex repo is private. Clone it manually:"
  echo "    git clone git@github.com:<org>/nicheindex.git /home/ubuntu/nicheindex"
  echo "  You will need an SSH key or personal access token configured."
fi

# ---------- 3. Python venv — clawdbot-infra ----------
echo "[3/7] Setting up clawdbot-infra Python venv..."
if [ ! -d /home/ubuntu/clawdbot-infra/.venv ]; then
  python3 -m venv /home/ubuntu/clawdbot-infra/.venv
fi
/home/ubuntu/clawdbot-infra/.venv/bin/pip install --quiet --upgrade pip
/home/ubuntu/clawdbot-infra/.venv/bin/pip install --quiet -r /home/ubuntu/clawdbot-infra/requirements.txt

# ---------- 4. Python venv — nicheindex pipeline ----------
echo "[4/7] Setting up nicheindex pipeline Python venv..."
PIPELINE_REQ="/home/ubuntu/nicheindex/pipeline/requirements.txt"
if [ -f "$PIPELINE_REQ" ]; then
  if [ ! -d /home/ubuntu/nicheindex/pipeline/.venv ]; then
    python3 -m venv /home/ubuntu/nicheindex/pipeline/.venv
  fi
  /home/ubuntu/nicheindex/pipeline/.venv/bin/pip install --quiet --upgrade pip
  /home/ubuntu/nicheindex/pipeline/.venv/bin/pip install --quiet -r "$PIPELINE_REQ"
else
  echo "  No pipeline requirements.txt found, skipping."
fi

# ---------- 5. Check .env files ----------
echo "[5/7] Checking .env files..."

CLAWDBOT_ENV="/home/ubuntu/clawdbot-infra/.env"
PIPELINE_ENV="/home/ubuntu/nicheindex/pipeline/.env"

check_env_var() {
  local file="$1"
  local var="$2"
  if grep -q "^${var}=" "$file" 2>/dev/null; then
    echo "    $var = OK"
  else
    echo "    $var = MISSING"
  fi
}

if [ -f "$CLAWDBOT_ENV" ]; then
  echo "  $CLAWDBOT_ENV:"
  for var in DATABASE_URL SUPABASE_URL SUPABASE_SERVICE_KEY RESEND_API_KEY WATCHDOG_EMAIL_TO DISCORD_WEBHOOK_URL; do
    check_env_var "$CLAWDBOT_ENV" "$var"
  done
else
  echo "  MISSING: $CLAWDBOT_ENV"
  echo "  Create it with: DATABASE_URL, SUPABASE_URL, SUPABASE_SERVICE_KEY,"
  echo "  RESEND_API_KEY, WATCHDOG_EMAIL_TO, DISCORD_WEBHOOK_URL"
fi

echo ""

if [ -f "$PIPELINE_ENV" ]; then
  echo "  $PIPELINE_ENV:"
  for var in SUPABASE_URL SUPABASE_SERVICE_ROLE_KEY RESEND_API_KEY DISCORD_WEBHOOK_URL; do
    check_env_var "$PIPELINE_ENV" "$var"
  done
else
  echo "  MISSING: $PIPELINE_ENV"
  echo "  Create it with: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY,"
  echo "  RESEND_API_KEY, DISCORD_WEBHOOK_URL"
fi

# ---------- 6. Install systemd units ----------
echo ""
echo "[6/7] Installing systemd units..."
sudo cp /home/ubuntu/clawdbot-infra/deploy/orchestrator.service /etc/systemd/system/
sudo cp /home/ubuntu/clawdbot-infra/deploy/watchdog.service /etc/systemd/system/
sudo cp /home/ubuntu/clawdbot-infra/deploy/watchdog.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable orchestrator.service
sudo systemctl enable watchdog.timer

# ---------- 7. Start services ----------
echo "[7/7] Starting services..."
sudo systemctl start orchestrator.service
sudo systemctl start watchdog.timer

# ---------- Done ----------
echo ""
echo "=== Setup Complete ==="
echo ""
echo "Service status:"
sudo systemctl status orchestrator.service --no-pager -l || true
echo ""
sudo systemctl status watchdog.timer --no-pager -l || true
echo ""
echo "Useful commands:"
echo "  sudo journalctl -u orchestrator -f          # tail orchestrator logs"
echo "  sudo journalctl -u watchdog -f              # tail watchdog logs"
echo "  sudo systemctl restart orchestrator          # restart orchestrator"
echo "  sudo systemctl status watchdog.timer         # check watchdog timer"
echo "  bash ~/clawdbot-infra/deploy/update.sh       # pull latest + restart"
