#!/usr/bin/env bash
# One command from a fresh Cloud Shell: enable Compute Engine, then provision
# the VMs and start the benchmark in the background with nohup. (Not tmux: a
# tmux server keeps the environment it started with, and Cloud Shell keeps
# gcloud's credentials in a per-session CLOUDSDK_CONFIG directory.)
# Progress: tail -f ~/uh-up.log, then bash bench/gcp/run_gcp.sh status.
set -euo pipefail
cd "$(dirname "$0")/../.."
project=$(gcloud config get-value project 2>/dev/null || true)
project=${project:-${GOOGLE_CLOUD_PROJECT:-${DEVSHELL_PROJECT_ID:-}}}
[ -n "$project" ] || { echo "no project: run gcloud config set project ID"; exit 1; }
export CLOUDSDK_CORE_PROJECT=$project
gcloud services enable compute.googleapis.com
nohup bash -c "bash bench/gcp/run_gcp.sh up && bash bench/gcp/run_gcp.sh start; echo EXIT=\$?" \
    > "$HOME/uh-up.log" 2>&1 < /dev/null &
echo "started in the background for project $project; log: ~/uh-up.log"
