#!/usr/bin/env bash
# One command from a fresh Cloud Shell: enable Compute Engine, then provision
# the VMs and start the benchmark inside tmux (so a disconnect does not stop
# it). Progress: tail -f ~/uh-up.log, then bash bench/gcp/run_gcp.sh status.
set -euo pipefail
cd "$(dirname "$0")/../.."
gcloud services enable compute.googleapis.com
tmux kill-session -t uh 2>/dev/null || true
tmux new -d -s uh "bash bench/gcp/run_gcp.sh up > \$HOME/uh-up.log 2>&1 && bash bench/gcp/run_gcp.sh start >> \$HOME/uh-up.log 2>&1; echo EXIT=\$? >> \$HOME/uh-up.log"
echo "started in tmux session 'uh'; log: ~/uh-up.log"
