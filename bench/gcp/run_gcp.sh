#!/usr/bin/env bash
# Run the paper's benchmarks on Google Compute Engine, from Google Cloud Shell.
#
#   git clone https://github.com/marimuthuvm/uringpy-http && cd uringpy-http
#   bash bench/gcp/run_gcp.sh up       # create the two VMs and provision them (~10 min)
#   bash bench/gcp/run_gcp.sh start    # start the benchmark on the client VM (runs unattended)
#   bash bench/gcp/run_gcp.sh status   # progress and ETA
#   bash bench/gcp/run_gcp.sh fetch    # copy the results here, as a .tgz
#   bash bench/gcp/run_gcp.sh down     # delete both VMs
#
# "all" does up, start, waits, fetches and deletes in one go; run it inside
# tmux, since Cloud Shell closes idle sessions.
#
# The benchmark runs on the client VM under nohup, so closing Cloud Shell
# does not stop it. Testbed as in the uringpy paper: server t2d-standard-4,
# client t2d-standard-8, both in us-east1-b, Ubuntu 24.04. The full profile
# takes about 6 hours (~$3 of VM time); PROFILE=quick takes about 1.5 hours.
set -euo pipefail

# Zones tried in order until both machine types can be created in one of
# them (t2d capacity is often short); the zone obtained is remembered.
ZONES=${ZONES:-${ZONE:-us-east1-b us-east1-c us-east1-d us-central1-a us-central1-b us-central1-c us-central1-f us-east4-a us-east4-b us-east4-c us-west1-a us-west1-b us-west1-c}}
PROFILE=${PROFILE:-full}
SERVER=${SERVER_NAME:-uh-bench-server}
CLIENT=${CLIENT_NAME:-uh-bench-client}
SERVER_TYPE=${SERVER_TYPE:-t2d-standard-4}
CLIENT_TYPE=${CLIENT_TYPE:-t2d-standard-8}
NETWORK=${NETWORK:-default}
REPO_DIR=$(cd "$(dirname "$0")/../.." && pwd)
WORK=${WORK:-$HOME/uh-bench}
mkdir -p "$WORK"
ZONE=$(cat "$WORK/zone" 2>/dev/null || echo "${ZONES%% *}")

g() { gcloud compute "$@" --zone "$ZONE" --quiet; }
on() { local host=$1; shift; g ssh "$host" --command "$*"; }

create() {  # name type
    g instances create "$1" --machine-type "$2" --network "$NETWORK" \
        --image-family ubuntu-2404-lts-amd64 --image-project ubuntu-os-cloud \
        --boot-disk-size 30GB --labels purpose=uringpy-http-bench
}

cmd_up() {
    if ! { g instances describe "$SERVER" && g instances describe "$CLIENT"; } >/dev/null 2>&1; then
        got=""
        for z in $ZONES; do
            ZONE=$z
            echo "trying zone $ZONE"
            if create "$SERVER" "$SERVER_TYPE"; then
                if create "$CLIENT" "$CLIENT_TYPE"; then
                    got=$z; break
                fi
                g instances delete "$SERVER" || true
            fi
        done
        [ -n "$got" ] || { echo "no zone had capacity for both machine types"; exit 1; }
        echo "$got" > "$WORK/zone"
        echo "using zone $got"
    fi
    for host in "$SERVER" "$CLIENT"; do
        for i in $(seq 30); do on "$host" true >/dev/null 2>&1 && break; sleep 10; done
    done
    [ -f "$WORK/uh_bench" ] || ssh-keygen -q -t ed25519 -N '' -C uh-bench -f "$WORK/uh_bench"
    git -C "$REPO_DIR" rev-parse HEAD > "$WORK/COMMIT"
    git -C "$REPO_DIR" archive --prefix=uringpy-http/ -o "$WORK/repo.tar" HEAD
    tar -rf "$WORK/repo.tar" --transform 's,^,uringpy-http/,' -C "$WORK" COMMIT
    gzip -f "$WORK/repo.tar"
    pids=()
    for role in server client; do
        host=$([ $role = server ] && echo "$SERVER" || echo "$CLIENT")
        (
            g scp "$WORK/repo.tar.gz" "$host:/tmp/repo.tgz"
            g scp "$WORK/uh_bench.pub" "$REPO_DIR/bench/gcp/setup_vm.sh" "$host:/tmp/"
            on "$host" "sudo bash /tmp/setup_vm.sh $role" > "$WORK/setup-$role.log" 2>&1
            echo "provisioned $role"
        ) &
        pids+=($!)
    done
    for p in "${pids[@]}"; do wait "$p"; done
    g scp "$WORK/uh_bench" "$CLIENT:/tmp/uh_bench"
    on "$CLIENT" "sudo install -o bench -g bench -m 600 /tmp/uh_bench /home/bench/.ssh/uh_bench && rm /tmp/uh_bench"
    ip=$(server_ip)
    on "$CLIENT" "sudo -u bench ssh -i /home/bench/.ssh/uh_bench -o StrictHostKeyChecking=no bench@$ip true" \
        && echo "client can reach the server at $ip"
    echo "Provisioned. Logs: $WORK/setup-*.log. Next: bash bench/gcp/run_gcp.sh start"
}

server_ip() {
    g instances describe "$SERVER" --format='get(networkInterfaces[0].networkIP)'
}

cmd_start() {
    ip=$(server_ip)
    on "$CLIENT" "sudo -u bench bash -lc 'cd ~/uringpy-http && rm -rf ~/results && nohup python3 bench/run_matrix.py --server-ip $ip --profile $PROFILE --out ~/results > ~/run.log 2>&1 &'"
    echo "Started ($PROFILE profile). Check with: bash bench/gcp/run_gcp.sh status"
}

cmd_status() {
    on "$CLIENT" "sudo -u bench bash -lc 'tail -n 5 ~/run.log; test -f ~/results/DONE && echo FINISHED || echo running'"
}

cmd_fetch() {
    stamp=$(date +%Y%m%d-%H%M)
    on "$CLIENT" "sudo -u bench tar -czf /tmp/uh-results.tgz -C /home/bench results run.log"
    g scp "$CLIENT:/tmp/uh-results.tgz" "$WORK/uh-results-$stamp.tgz"
    cp "$WORK"/setup-*.log "$WORK/" 2>/dev/null || true
    echo "Saved $WORK/uh-results-$stamp.tgz"
    echo "Download it with:  cloudshell download $WORK/uh-results-$stamp.tgz"
}

cmd_down() {
    g instances delete "$SERVER" "$CLIENT" || true
}

cmd_all() {
    cmd_up
    cmd_start
    until on "$CLIENT" "test -f /home/bench/results/DONE" >/dev/null 2>&1; do
        sleep 300
        cmd_status || true
    done
    cmd_fetch
    cmd_down
}

case "${1:-}" in
    up|start|status|fetch|down|all) "cmd_$1" ;;
    *) sed -n '2,20p' "$0"; exit 1 ;;
esac
