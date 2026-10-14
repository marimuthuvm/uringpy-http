#!/usr/bin/env bash
# Provision one benchmark VM (Ubuntu 24.04). Run as root:
#   setup_vm.sh server|client
# Expects /tmp/repo.tgz (this repository) and /tmp/uh_bench.pub (the key the
# client uses to reach the server) to have been copied over first.
set -euxo pipefail
ROLE=$1
export DEBIAN_FRONTEND=noninteractive

apt-get update -q
apt-get install -y -q build-essential pkg-config git curl libssl-dev zlib1g-dev psmisc python3 unzip

# A user that owns the benchmark, reachable from the client with the bench key.
id bench >/dev/null 2>&1 || useradd -m -s /bin/bash bench
install -d -o bench -g bench -m 700 /home/bench/.ssh
install -o bench -g bench -m 600 /tmp/uh_bench.pub /home/bench/.ssh/authorized_keys
rm -rf /home/bench/uringpy-http
tar -xzf /tmp/repo.tgz -C /home/bench
chown -R bench:bench /home/bench/uringpy-http

# Limits and socket queues for 400+ keep-alive connections.
cat > /etc/security/limits.d/90-uh-bench.conf <<'EOF'
bench soft nofile 65536
bench hard nofile 65536
bench soft memlock unlimited
bench hard memlock unlimited
EOF
cat > /etc/sysctl.d/90-uh-bench.conf <<'EOF'
net.core.somaxconn = 4096
net.ipv4.tcp_max_syn_backlog = 4096
net.ipv4.ip_local_port_range = 10000 65000
net.ipv4.tcp_tw_reuse = 1
EOF
sysctl --system >/dev/null

if [ "$ROLE" = client ]; then
    apt-get install -y -q wrk
    if ! command -v wrk2 >/dev/null; then
        rm -rf /tmp/wrk2
        git clone -q https://github.com/giltene/wrk2 /tmp/wrk2
        git -C /tmp/wrk2 checkout -q 44a94c17d8e6a0bac8559b53da76848e430cb7a7
        make -C /tmp/wrk2 -j"$(nproc)" >/dev/null
        install -m 755 /tmp/wrk2/wrk /usr/local/bin/wrk2
    fi
    wrk -v | head -1 || true
    exit 0
fi

# Server: CPython 3.14 with and without the GIL, managed by uv.
if ! command -v uv >/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh
fi
export UV_PYTHON_INSTALL_DIR=/opt/uh/python UV_CACHE_DIR=/opt/uh/cache
mkdir -p /opt/uh
uv python install 3.14 3.14t
uv venv -q --clear -p 3.14 /opt/uh/venv-gil
uv venv -q --clear -p 3.14t /opt/uh/venv-ft
REPO=/home/bench/uringpy-http
# The vendored liburing (2.6) is used on both builds so that the system's
# version does not enter the comparison.
export URINGPY_HTTP_VENDORED_LIBURING=1
for v in gil ft; do
    PY=/opt/uh/venv-$v/bin/python
    uv pip install -q --python $PY cython setuptools wheel
    uv pip install -q --python $PY --no-build-isolation "$REPO"
    uv pip install -q --python $PY uvicorn httptools uvloop starlette fastapi
done
uv pip install -q --python /opt/uh/venv-gil/bin/python granian || echo "granian unavailable"
chmod -R a+rX /opt/uh
for v in gil ft; do
    /opt/uh/venv-$v/bin/python - <<'EOF'
import sys, uringpy_http, fastapi, uvicorn, httptools, uvloop
print(sys.version, "GIL enabled:", getattr(sys, "_is_gil_enabled", lambda: True)())
EOF
done
