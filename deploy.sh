#!/usr/bin/env bash
# deploy.sh — the single tool to build, run, inspect, and benchmark a dito
# cluster. There is no companion script: cluster lifecycle, status, and every
# benchmark in the thesis live here, and ALL benchmark results are printed to
# the terminal (nothing is ever written to a results file).
#
# Default topology: 240 logical nodes (80 per machine) across 3 machines,
# R=3, Kademlia k-bucket size k=20. Each is adjustable from the command line.
#
# Node data lives on each host's RAM-backed /dev/shm tmpfs (never /tmp, never
# NFS — see data_base()). Per-node process stdout is redirected to a log file
# on that same scratch disk (80 background nodes per host cannot stream to your
# terminal); benchmark RESULTS, by contrast, are terminal-only.
#
# Connection model:  Mac → ipvs → {vscalc, vsstorm, taurus}
# Your Mac only authenticates to ipvs. No cluster host keys are stored locally.
#
# Usage:
#   ./deploy.sh [options] <command> [args]
#
# Commands:
#   start             build, copy, and start the cluster (default command)
#   deploy            rebuild + push binaries WITHOUT restarting nodes (picks up
#                     ditoload/client changes for the next bench; no data loss)
#   stop              kill all ditonode processes and wipe data dirs
#   status            print live ditonode process count on each machine
#   bench [scenario]  run benchmarks against the LIVE cluster and print results.
#                       no scenario  → full suite: fill read mixed hammer
#                                      stale churn + membership probe, then a
#                                      consolidated summary table (incl. the
#                                      class-A correctness/divergence column).
#                       a scenario   → run just that one and stream its report.
#                     scenarios: fill read mixed hammer stale churn split
#                                mixedfull (mixed + full all-node replica audit)
#                                rechurn   (mixed + REAL join/decommission churn
#                                           + full audit)
#   membership        probe /debug/membership on every node (O(R)-table claim)
#   saturate [scen]   worker-ramp throughput-ceiling sweep (paper §saturation)
#   churnsweep        split-brain (divergence) vs churn rate — the founding
#                     window probe. Expose divergence first with a narrow routing
#                     table: ./deploy.sh -k 4 churnsweep
#   rechurnsweep      class-A violations vs REAL topology-churn degree, split by
#                     cause: replica-divergence (DHT) vs lost-confirmed (failure
#                     budget). Set TOPO_BATCH>=R to probe the loss mode.
#   ksweep            split-brain vs Kademlia bucket size k (founding window)
#   settlesweep       split-brain vs pre-execution settle time (k fixed, no churn);
#                     run at the edge k from ksweep via BUCKET_SIZE
#   scale             redeploy across a sweep of cluster sizes; bench+membership
#   logs              tail node-1 logs from every machine
#
# Options (may appear before or after the command):
#   -n,  --nodes N      total logical nodes        (default 240; → N/#hosts each)
#        --per-host M    logical nodes per machine  (overrides -n)
#   -r,  --repl R       Paxos replication factor / quorum size   (default 3)
#   -k,  --bucket K     Kademlia k-bucket size (routing-table width) (default 20)
#        --runs N       measurement runs per benchmark            (default 10)
#        --workers W    benchmark worker goroutines (default: 200 for the suite)
#        --dur D        per-run duration, e.g. 90s    (default: 90s for the suite)
#        --hosts a,b,c  physical hosts               (default vscalc,vsstorm,taurus)
#   -h,  --help         show this help
#
# Extra tuning still available via env: BENCH_VALUE_BYTES, BENCH_INGRESS_PER_HOST,
# BUCKET_SIZE (narrow routing table to expose divergence), churn knobs
# CHURN_NODE_LO/CHURN_KILL_HOLD/CHURN_RECOVER_GAP/CHURN_HOLD/CHURN_BATCH/
# CHURN_SWEEP, K_SWEEP, SCALE_SWEEP, SAT_WORKERS/SAT_RUNS/SAT_DUR,
# TOPO_HOLD/TOPO_GAP/TOPO_BATCH/TOPO_MAX_JOINS/RECHURN_SWEEP (rechurn degree),
# START_SETTLE (cluster stabilize window), SETTLE_SWEEP (settlesweep levels),
# TOPO_CHURN=1 (churnsweep uses real joins), CHURN_RESTART=1 (churnsweep fresh
# cluster per level), NO_JUMP=1/SELF_HOST (orchestrate from inside the cluster).
set -euo pipefail

# ── Cluster configuration (defaults; override via the CLI flags above) ──────
JUMP=ipvs
HOSTS=(vscalc vsstorm taurus)
TOTAL_NODES=""                # if set via -n, derives NODES_PER_HOST
NODES_PER_HOST=80             # logical nodes per physical machine (240 total)
R=3                           # Paxos replication factor / quorum size
K=20                          # Kademlia k-bucket size (0 = library default 20)
RUNS=""                       # --runs override; empty = each command's own default

P2P_BASE=7000                 # node i  → p2p  port P2P_BASE+i  (7001–7080)
HTTP_BASE=9000                # node i  → http port HTTP_BASE+i  (9001–9080)

REMOTE_DIR="\$HOME/dito"      # NFS home — binaries only (read after deploy)

# BadgerDB data dir per host: /dev/shm (tmpfs, RAM-backed, no root needed,
# capped at 50% of RAM). Deliberately NOT a disk: taurus's /scratch SSD array
# is pathologically slow at synchronous writes (measured 2026-07-05: 119 kB/s
# on 4k dsync writes, ~34 ms/write on an idle Micron 5200 RAID-1; under bench
# load w_await hit 15 s and froze the whole cluster for 10–15 s at a time).
# RAM-backed data matches the benchmark failure model: churn kills PROCESSES,
# and tmpfs survives process death; host power loss is out of scope (Badger
# ran with SyncWrites=false on disk too, so media durability was never part
# of the measured semantics). Worst observed full-suite footprint was 38 GB —
# well under every host's tmpfs cap (vscalc 128G, vsstorm 256G, taurus 251G).
# NEVER use $HOME (NFS: mmap over NFS corrupts BadgerDB) and never /tmp (a
# tiny 2–5 GB LVM volume: fills → SIGBUS on the mmap'd files). Override with
# DATA_ROOT_<host> env if needed.
data_base() {
    case "$1" in
        vscalc)  echo "${DATA_ROOT_vscalc:-/dev/shm/dito}" ;;
        vsstorm) echo "${DATA_ROOT_vsstorm:-/dev/shm/dito}" ;;
        taurus)  echo "${DATA_ROOT_taurus:-/dev/shm/dito}" ;;
        *)       echo "/dev/shm/dito" ;;
    esac
}
log_base() { echo "$(data_base "$1")/logs"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CODE_DIR="$SCRIPT_DIR/code"

# Parallel indexed arrays (same index as HOSTS).
NODE_IPS=()
SEED_MULTIADDRS=()   # one bootstrap seed per physical machine

# ── Helpers ────────────────────────────────────────────────────────────────

log() { echo "==> $*"; }

host_idx() {
    local h=$1 i
    for i in "${!HOSTS[@]}"; do
        [[ "${HOSTS[$i]}" == "$h" ]] && echo "$i" && return
    done
    echo "-1"
}

# Badger block-cache per *node* — sized so total cache stays under ~40% RAM.
# 80 nodes × cache_MB per machine:
#   vscalc   256 GB:  80 × 256 MB  = 20 GB   (~8 % of RAM)
#   vsstorm  512 GB:  80 × 512 MB  = 40 GB   (~8 % of RAM)
#   taurus     1 TB:  80 × 1024 MB = 80 GB   (~8 % of RAM)
per_node_cache() {
    case "$1" in
        vscalc)  echo 256  ;;
        vsstorm) echo 512  ;;
        taurus)  echo 1024 ;;
        *)       echo 128  ;;
    esac
}

JUMP_SSH=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
          -o BatchMode=yes -o LogLevel=ERROR)

# Direct (no-jump) orchestration. Set NO_JUMP=1 when running this script FROM a
# cluster host (or anything on the cluster's LAN) so per-event churn no longer
# pays the jump-host double-SSH that bottlenecks the injectors. Then:
#   - commands to the host we're ON (SELF_HOST) run LOCALLY, with no SSH at all;
#   - commands to peer hosts use a single direct SSH over the LAN.
# SELF_HOST defaults to this machine's short hostname; override if it doesn't
# match a HOSTS entry. build/deploy are no-ops in this mode — you compile and
# distribute from your Mac first (binaries live on the NFS REMOTE_DIR, visible to
# every node), then run the sweeps here.
NO_JUMP="${NO_JUMP:-}"
SELF_HOST="${SELF_HOST:-$(hostname -s 2>/dev/null || true)}"
DIRECT_SSH=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            -o BatchMode=yes -o LogLevel=ERROR)

remote() {
    local host=$1; shift
    if [[ -n "$NO_JUMP" ]]; then
        if [[ "$host" == "$SELF_HOST" ]]; then
            printf '%s\n' "$@" | bash                       # local, no SSH
        else
            printf '%s\n' "$@" | ssh "${DIRECT_SSH[@]}" "$host" bash   # direct LAN SSH
        fi
        return
    fi
    printf '%s\n' "$@" | \
        ssh "${JUMP_SSH[@]}" "$JUMP" "ssh -o StrictHostKeyChecking=no $host bash"
}

copy_to() {
    local host=$1; shift
    local tmp
    tmp=$(ssh "${JUMP_SSH[@]}" "$JUMP" mktemp -d)
    scp -q "${JUMP_SSH[@]}" "$@" "$JUMP:$tmp/"
    local files="" bases="" f b
    for f in "$@"; do
        b=$(basename "$f")
        files="$files $tmp/$b"
        bases="$bases $REMOTE_DIR/$b"
    done
    # Unlink the destination binaries FIRST. Overwriting an executable that a
    # process still holds open (a just-killed/hung ditoload not yet reaped)
    # fails with scp "dest open Failure" / text-file-busy; rm replaces the inode
    # so the new file is independent of the dying process's mapping.
    ssh "${JUMP_SSH[@]}" "$JUMP" "ssh -o StrictHostKeyChecking=no $host 'mkdir -p $REMOTE_DIR; rm -f $bases'"
    ssh "${JUMP_SSH[@]}" "$JUMP" "scp -o StrictHostKeyChecking=no -o BatchMode=yes $files $host:$REMOTE_DIR/"
    ssh "${JUMP_SSH[@]}" "$JUMP" "rm -rf $tmp"
}

host_ip() { remote "$1" "hostname -I | awk '{print \$1}'"; }

# ── Commands ───────────────────────────────────────────────────────────────

build() {
    if [[ -n "$NO_JUMP" ]]; then
        log "NO_JUMP set — skipping build (compile on your Mac and distribute first)"
        return 0
    fi
    if [[ -n "${SKIP_BUILD:-}" ]]; then
        log "SKIP_BUILD set — using existing $SCRIPT_DIR/ditonode, ditoload (A/B baseline)"
        return 0
    fi
    log "Building linux/amd64 binaries..."
    (cd "$CODE_DIR" &&
        GOOS=linux GOARCH=amd64 go build -trimpath -o "$SCRIPT_DIR/ditonode" ./cmd/ditonode &&
        GOOS=linux GOARCH=amd64 go build -trimpath -o "$SCRIPT_DIR/ditoload"  ./cmd/ditoload)
    log "Built: ditonode  ditoload"
}

deploy() {
    if [[ -n "$NO_JUMP" ]]; then
        log "NO_JUMP set — skipping deploy (binaries already on the NFS REMOTE_DIR from your Mac)"
        return 0
    fi
    log "Distributing binaries..."
    for host in "${HOSTS[@]}"; do
        echo "  -> $host"
        remote "$host" "mkdir -p $REMOTE_DIR"
        # Ship the script itself alongside the binaries so the node always has the
        # latest, ready to run from inside the LAN with NO_JUMP=1 (no jump-host
        # bottleneck on the churn injectors). REMOTE_DIR is NFS-shared, so it lands
        # once for all hosts: run it on any node as `NO_JUMP=1 bash ~/dito/deploy.sh ...`.
        copy_to "$host" "$SCRIPT_DIR/ditonode" "$SCRIPT_DIR/ditoload" "$SCRIPT_DIR/deploy.sh"
    done
}

collect_peer_ids() {
    local total=$(( ${#HOSTS[@]} * NODES_PER_HOST ))
    log "Collecting all $total peer IDs in parallel (all nodes across all hosts)..."
    NODE_IPS=()
    SEED_MULTIADDRS=()

    # Collect all NODES_PER_HOST peer IDs from each host in one SSH call,
    # running all -print-id processes concurrently on the remote machine.
    # Each process generates/reads the node's identity key and prints its
    # multiaddr. Output is collected to per-node temp files to avoid
    # interleaving, then concatenated. All three hosts run in parallel.
    local tmpout
    tmpout=$(mktemp -d)
    trap "rm -rf $tmpout" EXIT

    for host in "${HOSTS[@]}"; do
        local idx ip db
        idx=$(host_idx "$host")
        NODE_IPS[$idx]=$(host_ip "$host")
        ip=${NODE_IPS[$idx]}
        db=$(data_base "$host")
        (
            addrs=$(remote "$host" "
                # Free any still-running cluster first: -print-id opens each node's
                # BadgerDB data dir (and p2p port) to read its identity, which a live
                # node holds locked → 'no multiaddrs'. A plain 'start' over a running
                # cluster would fail here without this (the sweeps stop_cluster first).
                # Then WAIT until they are fully gone: a SIGKILLed node lingers in
                # uninterruptible mmap I/O under load and still holds its QUIC UDP
                # port — -print-id then fails to bind SILENTLY (stderr discarded)
                # and the launch afterwards fails the same way (14 taurus nodes,
                # 2026-07-05, load avg 198).
                pkill -9 -x ditonode 2>/dev/null || true
                for _ in \$(seq 1 60); do
                    pgrep -x ditonode >/dev/null 2>&1 || break
                    sleep 1
                    pkill -9 -x ditonode 2>/dev/null || true
                done
                if pgrep -x ditonode >/dev/null 2>&1; then
                    echo 'WARNING: old ditonode processes still exiting — peer-ID collection may be incomplete' >&2
                fi
                # Scratch dir on this host's LOCAL disk (\$db), never \$HOME — HOME
                # is NFS-shared across all hosts, so a shared path races when the
                # three collectors run in parallel (one host's rm -rf clobbers
                # another's writes → 'Directory not empty' + missing IDs). Clean any
                # stale content first.
                tmpdir=$db/.peer-ids
                rm -rf \$tmpdir
                mkdir -p \$tmpdir
                for i in \$(seq 1 $NODES_PER_HOST); do
                    mkdir -p $db/node-\$i
                    $REMOTE_DIR/ditonode \
                        -bind      $ip \
                        -p2p-port  \$(($P2P_BASE + \$i)) \
                        -data      $db/node-\$i \
                        -transport quic \
                        -print-id 2>/dev/null > \$tmpdir/\$i &
                done
                wait
                # Keep only the external IP (filter loopback), join with commas.
                grep -h -v '127.0.0.1' \$tmpdir/[0-9]* | tr '\n' ',' | sed 's/,\$//'
                rm -rf \$tmpdir
            ")
            if [[ -z "$addrs" ]]; then
                echo "ERROR: no multiaddrs from $host" >&2
                exit 1
            fi
            echo "$addrs" > "$tmpout/$host"
        ) &
    done
    wait

    for host in "${HOSTS[@]}"; do
        local idx
        idx=$(host_idx "$host")
        local f="$tmpout/$host"
        if [[ ! -f "$f" ]] || [[ ! -s "$f" ]]; then
            echo "ERROR: missing peer IDs for $host" >&2
            exit 1
        fi
        SEED_MULTIADDRS[$idx]=$(cat "$f")
        local count
        count=$(tr ',' '\n' < "$f" | wc -l | tr -d ' ')
        echo "  $host (${NODE_IPS[$idx]}): collected $count peer addresses"
    done
}

start_cluster() {
    local total=$(( ${#HOSTS[@]} * NODES_PER_HOST ))
    log "Starting $total nodes ($NODES_PER_HOST per machine)  R=$R  k=${BUCKET_SIZE:-$K}..."

    # Build comma-separated bootstrap peer list from all machine seeds.
    local seeds="" s
    for s in "${SEED_MULTIADDRS[@]}"; do
        [[ -n "$seeds" ]] && seeds="$seeds,"
        seeds="$seeds$s"
    done

    for host in "${HOSTS[@]}"; do
        local idx ip cache db lb
        idx=$(host_idx "$host")
        ip=${NODE_IPS[$idx]}
        cache=$(per_node_cache "$host")
        db=$(data_base "$host")
        lb=$(log_base "$host")

        echo "  $host ($ip): launching $NODES_PER_HOST nodes  data=$db cache=${cache} MB"

        # Per-node launcher script, baked with this host's ip/cache/seeds/data
        # and parameterised by node index ($1). Used both for the initial launch
        # and for restarting an individual node during churn fault injection.
        # MUST be written to the host's LOCAL disk ($db), not $REMOTE_DIR: the
        # latter is NFS-shared across all hosts, so per-host writes collide and
        # the last writer's IP/data-dir leak to every host — restarts then fail
        # with "bind: cannot assign requested address".
        # Routing-table width is baked into the launcher so churn restarts inherit
        # it (default K=20 == library default). A small BUCKET_SIZE narrows each
        # node's k-buckets so ClosestPeers can disagree across nodes — the lever
        # the divergence sweeps override per-iteration to expose membership
        # divergence. See `churnsweep` / `ksweep`.
        local bucket="${BUCKET_SIZE:-$K}"
        local node_extra=" -bucket-size ${bucket}"

        local launch_sh="$db/launch-node.sh"
        local launcher
        launcher=$(cat <<EOF
#!/usr/bin/env bash
i="\$1"
mkdir -p $db/node-\$i
nohup $REMOTE_DIR/ditonode \\
    -bind       $ip \\
    -p2p-port   \$(($P2P_BASE  + \$i)) \\
    -http-port  \$(($HTTP_BASE + \$i)) \\
    -data       $db/node-\$i \\
    -peers      '$seeds' \\
    -r          $R \\
    -badger-mem-mb    64 \\
    -badger-cache-mb  $cache \\
    -badger-compactors 2 \\
    -transport quic$node_extra \\
    > $lb/node-\$i.log 2>&1 </dev/null &
EOF
)
        remote "$host" "
            # SIGKILL and WAIT until zero: a graceful SIGTERM drain of 80 nodes
            # takes >1 s, and launching while old processes still hold the ports
            # double-populates the host (seen twice: 154 and 160 processes).
            pkill -9 -x ditonode 2>/dev/null || true
            for _ in \$(seq 1 60); do
                pgrep -x ditonode >/dev/null 2>&1 || break
                sleep 1
                pkill -9 -x ditonode 2>/dev/null || true
            done
            if pgrep -x ditonode >/dev/null 2>&1; then
                echo '  WARNING: old ditonode processes still exiting — launch may hit busy ports'
            fi
            mkdir -p $lb
            cat > $launch_sh <<'RWLAUNCH'
$launcher
RWLAUNCH
            chmod +x $launch_sh
            for i in \$(seq 1 $NODES_PER_HOST); do
                bash $launch_sh \$i
            done
            sleep 2
            count=\$(pgrep -x -c ditonode 2>/dev/null || true)
            printf '  launched %d ditonode processes\n' \"\${count:-0}\"
            if [ \"\${count:-0}\" -ne \"$NODES_PER_HOST\" ]; then
                echo '  recent node-1 log:'
                tail -n 8 $lb/node-1.log 2>/dev/null | sed 's/^/    /' || true
            fi
        "
    done

    # Settle window before the cluster is declared ready — lets routing tables and
    # dial backoffs converge. 30 s is enough for a single-DC bring-up; raise via
    # START_SETTLE for a conservative paper run. `settlesweep` overrides it per
    # level to measure how much pre-execution convergence the system needs (a short
    # settle leaves ClosestPeers inconsistent → more founding divergence).
    local settle="${START_SETTLE:-30}"
    log "Cluster up — waiting ${settle} s for routing tables and dial backoffs to stabilize..."
    sleep "$settle"
    status
}

stop_cluster() {
    log "Stopping cluster and cleaning data directories..."
    local -a pids=()
    for host in "${HOSTS[@]}"; do
        local db
        db=$(data_base "$host")
        echo "  $host  (data=$db)"
        remote "$host" "
            # SIGKILL is immediate in the common case; only poll briefly for
            # stubborn processes that are still reaping / exiting after the kill.
            pkill -9 -x ditonode 2>/dev/null && echo 'killed' || echo 'not running'
            # Force-kill any stray ditoload (a hung load run ignores SIGTERM and
            # holds the binary open → next deploy's scp fails 'text file busy').
            pkill -9 -x ditoload 2>/dev/null && echo 'killed stray ditoload' || true
            for _ in \$(seq 1 5); do
                pgrep -x ditonode >/dev/null 2>&1 || break
                sleep 1
                pkill -9 -x ditonode 2>/dev/null || true
            done
            rm -rf $db/node-* $db/logs
            # Also clear any stale data left on the tiny /tmp volume by older
            # runs that defaulted DATA_BASE there (keeps /tmp from staying full),
            # and at the pre-2026-07 on-disk scratch locations.
            rm -rf /tmp/dito /scratch/dito /data/scratch/dito 2>/dev/null || true
        " &
        pids+=("$!")
    done
    for pid in "${pids[@]}"; do
        wait "$pid"
    done
}

status() {
    local total=$(( ${#HOSTS[@]} * NODES_PER_HOST ))
    log "Cluster status (expected $total processes across ${#HOSTS[@]} machines):"
    for host in "${HOSTS[@]}"; do
        local ip count
        ip=$(remote    "$host" "hostname -I | awk '{print \$1}'"     2>/dev/null || echo "unreachable")
        count=$(remote "$host" "pgrep -x -c ditonode 2>/dev/null || true" 2>/dev/null || echo "unreachable")
        [[ -z "$count" ]] && count=0
        echo "  $host ($ip): $count / $NODES_PER_HOST ditonode processes"
    done
}

show_logs() {
    log "Last 20 lines from node-1 on each host:"
    for host in "${HOSTS[@]}"; do
        remote "$host" "tail -n 20 $(log_base "$host")/node-1.log 2>/dev/null || echo '(no log yet)'" \
            | sed "s/^/[$host] /"
    done
    echo ""
    log "Live tail of node-1 on each host (Ctrl-C to stop):"
    for host in "${HOSTS[@]}"; do
        remote "$host" "tail -f $(log_base "$host")/node-1.log 2>/dev/null" \
            | sed -u "s/^/[$host] /" &
    done
    wait
}

resolve_node_ips() {
    NODE_IPS=()
    for host in "${HOSTS[@]}"; do
        local idx
        idx=$(host_idx "$host")
        NODE_IPS[$idx]=$(host_ip "$host")
    done
}

# ingress_targets prints a comma-separated list of the first
# BENCH_INGRESS_PER_HOST nodes on every host (client connection fan-out).
ingress_targets() {
    local ingress="${BENCH_INGRESS_PER_HOST:-3}"
    local targets="" hi ni
    for hi in "${!HOSTS[@]}"; do
        for (( ni = 1; ni <= ingress; ni++ )); do
            [[ -n "$targets" ]] && targets="$targets,"
            targets="${targets}http://${NODE_IPS[$hi]}:$((HTTP_BASE + ni))"
        done
    done
    echo "$targets"
}

# all_targets prints every node on every host (used for the membership probe and
# the mixedfull replica audit).
all_targets() {
    local targets="" hi ni
    for hi in "${!HOSTS[@]}"; do
        for (( ni = 1; ni <= NODES_PER_HOST; ni++ )); do
            [[ -n "$targets" ]] && targets="$targets,"
            targets="${targets}http://${NODE_IPS[$hi]}:$((HTTP_BASE + ni))"
        done
    done
    echo "$targets"
}

# audit_targets_wide prints node URLs over a WIDER index range than the initial
# NODES_PER_HOST, so the rechurn audit also reaches nodes that joined at fresh
# indices during the run (ditoload skips any that are unreachable, including the
# decommissioned originals). Range: 1 .. NODES_PER_HOST+TOPO_MAX_JOINS per host.
audit_targets_wide() {
    local extra="${TOPO_MAX_JOINS:-40}"
    local targets="" hi ni
    for hi in "${!HOSTS[@]}"; do
        for (( ni = 1; ni <= NODES_PER_HOST + extra; ni++ )); do
            [[ -n "$targets" ]] && targets="$targets,"
            targets="${targets}http://${NODE_IPS[$hi]}:$((HTTP_BASE + ni))"
        done
    done
    echo "$targets"
}

# restart_node_cmd db idx — remote snippet that restarts one killed node SAFELY.
# The old process must be FULLY gone first: a still-exiting process holds the
# Badger directory flock, and launching over it fails with "Cannot acquire
# directory lock" — the new process exits instantly and the node stays dead
# for the rest of the run (killed 4 taurus nodes in the 2026-07-05 suite).
# So: wait for the pattern to vanish, launch, verify, retry once, and print a
# RESTART FAILED marker if the node is still down so the injector log shows
# the hole instead of silently under-populating the cluster.
restart_node_cmd() {
    local db=$1 idx=$2
    cat <<EOF
for _ in \$(seq 1 20); do
    pgrep -f '$db/node-$idx ' >/dev/null 2>&1 || break
    sleep 0.5
done
bash $db/launch-node.sh $idx
sleep 1
if ! pgrep -f '$db/node-$idx ' >/dev/null 2>&1; then
    sleep 2
    bash $db/launch-node.sh $idx
    sleep 1
    pgrep -f '$db/node-$idx ' >/dev/null 2>&1 || echo "  [churn] RESTART FAILED: node-$idx"
fi
EOF
}

# churn_injector kills and later restarts one random non-ingress node every
# ~(HOLD+GAP) seconds for `$1` seconds total. Ingress nodes (1..CHURN_NODE_LO-1)
# are spared so the load client always has a reachable entry point — the churn
# stresses the Paxos/storage layer (Dead detection → reconfigure → migrate),
# which is what scenario "churn" measures. Runs concurrently with ditoload.
# Kills use SIGKILL: the scenario models crash-stop failure ("node died
# mid-request"), and a SIGTERM drain both soft-pedals the failure model and
# holds the Badger flock long past the hold window (see restart_node_cmd).
churn_injector() {
    local dur_s=$1
    local lo="${CHURN_NODE_LO:-4}"
    local hold="${CHURN_KILL_HOLD:-8}"
    local gap="${CHURN_RECOVER_GAP:-7}"
    local deadline=$(( $(date +%s) + dur_s ))
    log "churn injector: kill+restore one node (idx>=$lo) every ~$((hold+gap))s for ${dur_s}s"
    while [[ $(date +%s) -lt $deadline ]]; do
        local hi="${HOSTS[$((RANDOM % ${#HOSTS[@]}))]}"
        local idx=$(( lo + RANDOM % (NODES_PER_HOST - lo + 1) ))
        local db; db=$(data_base "$hi")
        echo "  [churn] kill    $hi node-$idx"
        remote "$hi" "pkill -9 -f '$db/node-$idx ' 2>/dev/null || true" >/dev/null 2>&1 || true
        sleep "$hold"
        echo "  [churn] restart $hi node-$idx"
        remote "$hi" "$(restart_node_cmd "$db" "$idx")" 2>/dev/null || true
        sleep "$gap"
    done
}

# churn_injector_rate dur gap batch hold — generalised injector for the churn
# sweep. Each event kills `batch` random non-ingress nodes, holds them down for
# `hold`s, restarts them, then waits `gap`s before the next event. Membership-
# change rate ≈ batch/(hold+gap) events/s. batch<=0 disables churn (the control
# level): it simply sleeps, so the sweep measures the baseline violation rate of
# a quiescent cluster. Nodes killed in a batch are tracked and individually
# restarted via the per-host launch-node.sh baked by start_cluster.
#
# A batch's SSH calls are fired in PARALLEL (backgrounded + waited), so all
# `batch` kills — and all `batch` restarts — land together rather than staggered
# by the jump-host round-trip. Serial execution would spread a "simultaneous"
# batch over seconds, defeating the point of batching (dropping several replicas
# / perturbing several keyspace regions at the same instant). Keep batch modest:
# each backgrounded `remote` is a concurrent jump-host SSH session.
churn_injector_rate() {
    local dur_s=$1 gap=$2 batch=$3 hold=$4
    local lo="${CHURN_NODE_LO:-4}"
    local deadline=$(( $(date +%s) + dur_s ))
    # Count membership changes actually delivered (one per kill) into
    # CHURN_COUNT_FILE if set, so churnsweep can report the REALIZED rate rather
    # than the nominal one — they diverge once the injector is SSH-bound.
    local events=0 cf="${CHURN_COUNT_FILE:-}"
    if (( batch <= 0 )); then
        log "churn injector: DISABLED (control level) for ${dur_s}s"
        [[ -n "$cf" ]] && echo 0 > "$cf"
        sleep "$dur_s"
        return 0
    fi
    log "churn injector: kill+restore $batch node(s)/event (parallel), hold=${hold}s gap=${gap}s for ${dur_s}s"
    while [[ $(date +%s) -lt $deadline ]]; do
        local hosts=() idxs=() pids=() b
        for (( b=0; b<batch; b++ )); do
            local hi="${HOSTS[$((RANDOM % ${#HOSTS[@]}))]}"
            local idx=$(( lo + RANDOM % (NODES_PER_HOST - lo + 1) ))
            local db; db=$(data_base "$hi")
            hosts+=("$hi"); idxs+=("$idx")
            echo "  [churn] kill    $hi node-$idx"
            remote "$hi" "pkill -9 -f '$db/node-$idx ' 2>/dev/null || true" >/dev/null 2>&1 &
            pids+=($!)
            events=$(( events + 1 ))
        done
        wait "${pids[@]}" 2>/dev/null || true
        [[ -n "$cf" ]] && echo "$events" > "$cf"
        sleep "$hold"
        pids=()
        for b in "${!hosts[@]}"; do
            local hi="${hosts[$b]}" idx="${idxs[$b]}" db
            db=$(data_base "$hi")
            echo "  [churn] restart $hi node-$idx"
            remote "$hi" "$(restart_node_cmd "$db" "$idx")" 2>/dev/null &
            pids+=($!)
        done
        wait "${pids[@]}" 2>/dev/null || true
        sleep "$gap"
    done
}

# topology_injector dur — REAL topology churn for the `rechurn` scenario. Unlike
# churn_injector (kill+restart the SAME node → same identity, same keyspace
# position), each event PERMANENTLY decommissions one random original node (killed,
# never restarted) AND joins one brand-new node at a fresh index with a fresh data
# dir → a NEW libp2p identity, hence a new position in the XOR keyspace. This
# reshapes per-object replica rankings, exercising genuine joins + permanent
# departures (the join-time handoff and sustained migration), not transient
# failure. Net cluster size stays ~constant. Ingress nodes (idx < CHURN_NODE_LO)
# are spared so the load client keeps an entry point.
#   Env: TOPO_HOLD (settle after each event, default 12s), TOPO_GAP (default 6s),
#        TOPO_MAX_JOINS (index headroom for joiners, default 40),
#        TOPO_BATCH (decommissions+joins per event, default 1).
#
# TOPO_BATCH controls SIMULTANEITY, the second axis of churn degree. With the
# default 1, at most one node is ever absent at a time, so the failure budget is
# never exceeded and the only class-A signal is DHT/membership divergence. Set
# TOPO_BATCH >= R (the replication factor) to decommission several nodes back to
# back within one event: if they were the only replicas of a key, the committed
# value is LOST before it re-replicates (failure budget exceeded → ditoload's
# `A lost confirmed write`). Combine a large batch with a SHORT hold/gap to keep
# them down together. Joins still replace departures 1:1, so cluster size is
# stable across the event.
topology_injector() {
    local dur_s=$1
    local hold="${TOPO_HOLD:-12}"
    local gap="${TOPO_GAP:-6}"
    local batch="${TOPO_BATCH:-1}"
    (( batch < 1 )) && batch=1
    local lo="${CHURN_NODE_LO:-4}"
    local deadline=$(( $(date +%s) + dur_s ))
    # Live churnable pool of "<hostIdx>:<nodeIdx>" (indexed arrays only — macOS
    # bash 3.2 has no associative arrays). Seeded with all non-ingress originals.
    # `free` holds slots vacated by decommissions; a join REUSES a freed slot
    # rather than allocating an ever-higher index. Reuse is sound because the
    # joiner's identity/keyspace position comes from the WIPED data dir's freshly
    # generated key, not from the index/port — so a recycled slot still yields a
    # new peer ID and a new XOR position. Crucially it keeps every live node
    # within the originals' index range (1..NODES_PER_HOST), which is what the
    # post-run audit polls: an unbounded index could place a live joiner beyond
    # audit_targets_wide's reach and be miscounted as a lost write. nextidx is a
    # fallback only if frees run dry (cannot happen at steady state, since each
    # event frees `batch` slots before it consumes `batch`).
    # Count membership changes actually delivered (one per decommission) into
    # CHURN_COUNT_FILE if set, so churnsweep reports the REALIZED rate.
    local events=0 cf="${CHURN_COUNT_FILE:-}"
    local -a pool=()
    local -a free=()
    local -a nextidx=()
    local hi ni
    for hi in "${!HOSTS[@]}"; do
        nextidx[$hi]=$(( NODES_PER_HOST + 1 ))
        for (( ni = lo; ni <= NODES_PER_HOST; ni++ )); do pool+=("$hi:$ni"); done
    done
    log "topology injector: rolling decommission+join ${batch}/event (parallel) every ~$((hold+gap))s for ${dur_s}s, pool=${#pool[@]} (REAL churn, size-stable)"
    while [[ $(date +%s) -lt $deadline ]]; do
        local b pids=()
        # Decommission `batch` random live pool members. Array bookkeeping stays
        # serial, but the SSH kills are fired in PARALLEL (backgrounded + waited)
        # so the nodes are absent SIMULTANEOUSLY — the only way to drop all R
        # replicas of a key before re-replication, instead of seconds apart via the
        # jump host. Each is permanent; its slot is freed for a joiner to reuse.
        for (( b = 0; b < batch; b++ )); do
            (( ${#pool[@]} == 0 )) && break
            local pi=$(( RANDOM % ${#pool[@]} ))
            local entry="${pool[$pi]}"
            pool=("${pool[@]:0:$pi}" "${pool[@]:$((pi+1))}")
            local dhi="${entry%%:*}" didx="${entry##*:}"
            local dh="${HOSTS[$dhi]}" ddb; ddb=$(data_base "$dh")
            echo "  [topo] decommission $dh node-$didx (permanent departure)"
            remote "$dh" "pkill -f '$ddb/node-$didx ' 2>/dev/null || true" >/dev/null 2>&1 &
            pids+=($!)
            free+=("$dhi:$didx")
            events=$(( events + 1 ))
        done
        wait "${pids[@]}" 2>/dev/null || true
        [[ -n "$cf" ]] && echo "$events" > "$cf"
        # Join `batch` brand-new nodes, reusing freed slots, also in parallel.
        # Wiping the data dir first forces a fresh identity key → new peer ID → new
        # keyspace position. Each bootstraps off the seeds baked into launch-node.sh
        # and is discovered by the rest via the Kademlia join; add it to the pool so
        # the live cluster size stays constant across the event.
        pids=()
        for (( b = 0; b < batch; b++ )); do
            local jhi jidx
            if (( ${#free[@]} > 0 )); then
                local fe="${free[0]}"; free=("${free[@]:1}")
                jhi="${fe%%:*}"; jidx="${fe##*:}"
            else
                jhi=$(( RANDOM % ${#HOSTS[@]} ))
                jidx=${nextidx[$jhi]}; nextidx[$jhi]=$(( jidx + 1 ))
            fi
            local jh="${HOSTS[$jhi]}"
            local jdb; jdb=$(data_base "$jh")
            echo "  [topo] join         $jh node-$jidx (fresh identity)"
            remote "$jh" "rm -rf $jdb/node-$jidx; bash $jdb/launch-node.sh $jidx" >/dev/null 2>&1 &
            pids+=($!)
            pool+=("$jhi:$jidx")
        done
        wait "${pids[@]}" 2>/dev/null || true
        sleep "$hold"
        sleep "$gap"
    done
}

# churnsweep — measure split-brain (class-A) violations vs churn intensity.
#
# Runs the `split` correctness probe (ditoload) — concurrent double-PUT of fresh
# keys from far-apart ingress nodes — while churn_injector_rate churns the
# cluster at a sweep of rates, tabulating violations to a CSV. This isolates the
# system's one genuine correctness assumption (consistent replica-set membership)
# and finds the churn rate at which it breaks.
#
# Parameters that matter, and why:
#   CHURN_SWEEP   gap-seconds per level; "control"/0 = no churn baseline.
#                 Smaller gap = more membership-change events/s = wider/more
#                 frequent routing-table flux = more divergence opportunities.
#                 (default "control 30 15 8 4 2")
#   CHURN_BATCH   nodes killed+restarted per event (default 2). Larger batches
#                 perturb more of the keyspace per event and raise the odds two
#                 ingress disagree on a fresh key's quorum.
#   CHURN_HOLD    seconds a node stays down (default 8); longer hold = more nodes
#                 simultaneously absent from routing tables.
#   BENCH_WORKERS concurrent fresh-key probes (default 64). More probes during
#                 flux = more first-writes-to-new-keys that can race divergence.
#   BENCH_DUR     per-level measured duration (default 120s); BENCH_RUNS (1).
#
# DECISIVE PREREQUISITE — the routing-table width. At the default k-bucket size
# (k=20) the DHT lookups converge and violations stay ~0 at every churn rate (a
# real, reportable result: dito tolerates churn when routing is consistent). To
# find the breaking point, bring the cluster up with a narrow routing table FIRST:
#   BUCKET_SIZE=4 ./deploy.sh start
# then run ./deploy.sh churnsweep. Sweep N (NODES_PER_HOST) as a third axis by
# restarting at each size, as `scale` does.
churnsweep() {
    log "Resolving node IPs..."
    resolve_node_ips
    local targets; targets=$(ingress_targets)
    local runs="${RUNS:-${BENCH_RUNS:-1}}"
    local dur="${BENCH_DUR:-120s}"
    local workers="${BENCH_WORKERS:-64}"
    local valb="${BENCH_VALUE_BYTES:-64}"
    local hold="${CHURN_HOLD:-8}"
    local batch="${CHURN_BATCH:-2}"
    local sweep; read -r -a sweep <<< "${CHURN_SWEEP:-control 30 15 8 4 2}"

    # Churn type. Default = churn_injector_rate (kill+restart: same identity, same
    # keyspace position — a transient-failure trigger). TOPO_CHURN=1 = real joins
    # via topology_injector (fresh identity reshapes the XOR ranking — the stronger
    # founding-window trigger).
    local mode="kill+restart"; [[ "${TOPO_CHURN:-0}" == "1" ]] && mode="real join/leave"
    # CHURN_RESTART=1: bring up a FRESH cluster (at BUCKET_SIZE) for EACH level,
    # instead of churning one shared cluster across all levels. This removes two
    # confounds seen in the shared-cluster mode: (a) order effects — topology_injector
    # permanently reshapes the cluster, so later levels run on a more-churned cluster
    # than earlier ones; (b) each level samples an INDEPENDENT random topology, so a
    # single split-prone boot can't dominate the whole sweep. It also makes the k
    # label authoritative (we start the cluster). Cost: a stop+start per level.
    local restart=""; [[ "${CHURN_RESTART:-0}" == "1" ]] && restart=1
    log "churn sweep: gaps={${sweep[*]}} batch=$batch hold=${hold}s dur=$dur workers=$workers churn=$mode restart=${restart:-0} (terminal-only)"
    if [[ -n "$restart" ]]; then
        log "  per-level fresh cluster at k=${BUCKET_SIZE:-$K} (authoritative)"
    else
        log "  shared cluster; k=${BUCKET_SIZE:-$K} is a LABEL only (real k = whatever start used)"
    fi
    local -a rows=()
    printf '%10s  %6s  %14s  %14s  %14s  %10s  %10s  %10s  %10s\n' \
        gap batch ev/min'(real)' split-create split-read keys/run C-live% D-cong% minAvail%
    local g
    for g in "${sweep[@]}"; do
        local gap b
        if [[ "$g" == "control" || "$g" == "0" ]]; then gap=0; b=0; else gap="$g"; b="$batch"; fi

        # Fresh independent cluster per level (CHURN_RESTART=1).
        if [[ -n "$restart" ]]; then
            log "──── level gap=$g: fresh cluster at k=${BUCKET_SIZE:-$K} ────"
            stop_cluster
            collect_peer_ids
            start_cluster
            resolve_node_ips
            targets=$(ingress_targets)
        fi

        local per total
        per="${dur%s}"
        total=$(( per * runs + 30 ))
        local cf; cf=$(mktemp)
        local inj="" t0 t1
        t0=$(date +%s)
        if (( b > 0 )); then
            if [[ "${TOPO_CHURN:-0}" == "1" ]]; then
                CHURN_COUNT_FILE="$cf" TOPO_HOLD="$hold" TOPO_GAP="$gap" TOPO_BATCH="$b" topology_injector "$total" &
            else
                CHURN_COUNT_FILE="$cf" churn_injector_rate "$total" "$gap" "$b" "$hold" &
            fi
            inj=$!
        fi  # b==0 is the control level: no churn

        local out
        out=$(remote taurus "$REMOTE_DIR/ditoload -scenario split -workers $workers -runs $runs -dur $dur -targets '$targets' -value-bytes $valb" 2>&1) || true
        t1=$(date +%s)
        if [[ -n "$inj" ]]; then kill "$inj" 2>/dev/null || true; wait "$inj" 2>/dev/null || true; fi

        # REALIZED rate: membership changes the injector actually delivered (from
        # CHURN_COUNT_FILE) over the wall-time it ran — the honest x-axis, since the
        # nominal 60*batch/(hold+gap) overstates it once the injector is SSH-bound.
        local cnt elapsed realized
        cnt=$(cat "$cf" 2>/dev/null || echo 0); rm -f "$cf"
        [[ -z "$cnt" ]] && cnt=0
        elapsed=$(( t1 - t0 )); (( elapsed < 1 )) && elapsed=1
        realized=$(awk "BEGIN{printf \"%.1f\", 60.0*$cnt/$elapsed}")

        # Pull each KPI's cross-run mean (first number after the colon).
        local sc sr kp cliv dcon mav
        sc=$(  echo "$out" | grep -E 'A split-brain double-create'  | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        sr=$(  echo "$out" | grep -E 'A split-brain divergent-read' | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        kp=$(  echo "$out" | grep -E 'split keys probed'            | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        cliv=$(echo "$out" | grep -E 'C liveness'                   | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        dcon=$(echo "$out" | grep -E 'D congestion'                 | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        mav=$( echo "$out" | grep -E 'min 1s availability'          | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        rows+=("$(printf '%10s  %6s  %14s  %14s  %14s  %10s  %10s  %10s  %10s' \
            "$g" "$b" "$realized" "${sc:-ERR}" "${sr:-?}" "${kp:-?}" "${cliv:-?}" "${dcon:-?}" "${mav:-?}")")
        printf '%10s  %6s  %14s  %14s  %14s  %10s  %10s  %10s  %10s\n' \
            "$g" "$b" "$realized" "${sc:-ERR}" "${sr:-?}" "${kp:-?}" "${cliv:-?}" "${dcon:-?}" "${mav:-?}"
    done

    echo
    echo "═════════════ CHURNSWEEP SUMMARY  N=$(( ${#HOSTS[@]} * NODES_PER_HOST ))  R=$R  k=${BUCKET_SIZE:-$K}  runs=$runs  restart=${restart:-0} ═════════════"
    printf '%10s  %6s  %14s  %14s  %14s  %10s  %10s  %10s  %10s\n' \
        gap batch ev/min'(real)' split-create split-read keys/run C-live% D-cong% minAvail%
    local row; for row in "${rows[@]}"; do echo "$row"; done
    echo "keys/run = fresh keys the split probe attempted (the denominator). split-create/-read are class-A — MUST be 0."
    echo "════════════════════════════════════════════════════════════════════════════════════════"
    log "churn sweep complete."
}

# rechurnsweep — class-A violations vs degree of REAL topology churn, ATTRIBUTED
# to root cause. Drives the `rechurn` scenario (mixed load + full all-node audit)
# under topology_injector at a sweep of event intervals, and tabulates the two
# distinct class-A signals the audit emits so each can be charted against churn:
#
#   replica divergence   DHT / membership inconsistency — two quorums committed
#                        different values for one key (split-brain at rest).
#   lost confirmed write FAILURE BUDGET EXCEEDED — a value the client saw commit
#                        has no replica left, i.e. all R replicas departed before
#                        re-replication. Requires SIMULTANEOUS departures, so set
#                        TOPO_BATCH >= R to make this mode reachable; at the
#                        default batch=1 it stays 0 and only divergence can show.
#
# Two degree axes, both via env so one sweep isolates one cause:
#   RECHURN_SWEEP  event-interval gaps (sec); "control"/0 = no churn baseline.
#                  Smaller gap = more join/leave events/min = more routing flux =
#                  more divergence opportunities. (default "control 18 12 6 3")
#   TOPO_BATCH     simultaneous decommissions+joins per event (default 1). Raise
#                  to >= R to probe the failure-budget (lost-write) mode.
#   TOPO_HOLD      settle seconds a batch stays down (default 6); keep SHORT with
#                  a large batch so all R replicas are absent together.
# Like churnsweep, run -runs 1 (default) so the post-run audit sees the settled
# cluster, not mid-migration churn. Same routing-table caveat applies: at the
# default k=20 divergence stays ~0 (a real result); re-`start` with a smaller -k /
# BUCKET_SIZE to expose it. To exercise the loss mode instead, churn the default
# routing table with TOPO_BATCH>=R.
rechurnsweep() {
    log "Resolving node IPs..."
    resolve_node_ips
    local targets; targets=$(ingress_targets)
    local atw; atw=$(audit_targets_wide)
    local runs="${RUNS:-${BENCH_RUNS:-1}}"
    local dur="${BENCH_DUR:-60s}"
    local workers="${BENCH_WORKERS:-200}"
    local valb="${BENCH_VALUE_BYTES:-64}"
    local settle="${AUDIT_SETTLE:-45s}"
    local hold="${TOPO_HOLD:-6}"
    local batch="${TOPO_BATCH:-1}"
    local sweep; read -r -a sweep <<< "${RECHURN_SWEEP:-control 18 12 6 3}"

    log "rechurn sweep: gaps={${sweep[*]}} batch=$batch hold=${hold}s dur=$dur runs=$runs workers=$workers settle=$settle (terminal-only)"
    log "  routing regime: k=${BUCKET_SIZE:-$K}  R=$R"
    if (( batch < R )); then
        log "  NOTE: TOPO_BATCH=$batch < R=$R — lost-confirmed (failure-budget) mode unreachable; set TOPO_BATCH>=$R to probe it."
    fi
    local -a rows=()
    printf '%10s  %6s  %12s  %16s  %18s  %14s  %10s\n' \
        gap batch events/min replica-divergence lost-confirmed-write lin-violations minAvail%
    local g
    for g in "${sweep[@]}"; do
        local gap epm
        if [[ "$g" == "control" || "$g" == "0" ]]; then gap=0; else gap="$g"; fi
        if [[ "$gap" == "0" ]]; then epm="0"; else epm=$(awk "BEGIN{printf \"%.1f\", 60.0*$batch/($hold+$gap)}"); fi

        # The injector covers the WORKLOAD only (per*runs + small pad); ditoload's
        # -audit-settle then drains migrations so the audit sees the settled state.
        local per total
        per="${dur%s}"
        total=$(( per * runs + 15 ))
        local inj=""
        if [[ "$gap" != "0" ]]; then
            TOPO_HOLD="$hold" TOPO_GAP="$gap" TOPO_BATCH="$batch" topology_injector "$total" &
            inj=$!
        fi

        local out
        out=$(remote taurus "$REMOTE_DIR/ditoload -scenario rechurn -workers $workers -runs $runs -dur $dur -targets '$targets' -value-bytes $valb -audit-targets '$atw' -per-key-cap 0 -audit-settle $settle" 2>&1) || true
        if [[ -n "$inj" ]]; then kill "$inj" 2>/dev/null || true; wait "$inj" 2>/dev/null || true; fi

        # Pull each KPI's cross-run mean (first number after the colon).
        local rd lc lv mav
        rd=$( echo "$out" | grep -E 'A replica divergence'    | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        lc=$( echo "$out" | grep -E 'A lost confirmed write'  | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        lv=$( echo "$out" | grep -E 'A lin violations'        | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        mav=$(echo "$out" | grep -E 'min 1s availability'     | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        rows+=("$(printf '%10s  %6s  %12s  %16s  %18s  %14s  %10s' \
            "$g" "$batch" "$epm" "${rd:-ERR}" "${lc:-?}" "${lv:-?}" "${mav:-?}")")
        printf '%10s  %6s  %12s  %16s  %18s  %14s  %10s\n' \
            "$g" "$batch" "$epm" "${rd:-ERR}" "${lc:-?}" "${lv:-?}" "${mav:-?}"
        # Surface ditoload's per-key forensics (audit summary + class-A samples:
        # which keys diverged / were lost, their values+seqs, and holder nodes),
        # which the KPI grep above would otherwise swallow.
        local forensics
        forensics=$(echo "$out" | grep -E 'CLASS-A|audit: [0-9]+/|^[[:space:]]+(key=|seq=)' || true)
        [[ -n "$forensics" ]] && echo "$forensics"
    done

    echo
    echo "═════════════ RECHURNSWEEP SUMMARY  N=$(( ${#HOSTS[@]} * NODES_PER_HOST ))  R=$R  k=${BUCKET_SIZE:-$K}  batch=$batch  runs=$runs ═════════════"
    printf '%10s  %6s  %12s  %16s  %18s  %14s  %10s\n' \
        gap batch events/min replica-divergence lost-confirmed-write lin-violations minAvail%
    local row; for row in "${rows[@]}"; do echo "$row"; done
    echo "replica-divergence = DHT/membership inconsistency; lost-confirmed-write = failure budget exceeded. Both class-A, MUST be 0."
    echo "════════════════════════════════════════════════════════════════════════════════════════"
    log "rechurn sweep complete."
}

# ksweep — Kademlia bucket-size dose-response for founding divergence, N fixed.
#
# For each k in K_SWEEP, bring up a FRESH N-node cluster with that bucket size
# (narrow k-buckets are the divergence lever), then run the `split` correctness
# probe BENCH_RUNS times at control (no churn) and at each CHURN_SWEEP level,
# tabulating the cross-run-mean class-A split counts into one CSV with a k column.
# This yields two curves at once: the steady-state (control) violation rate vs k,
# and a churn dose-response (little -> more) at each k. N is held fixed; only k
# and the churn rate vary.
#
# Env: K_SWEEP (default "4 8 12 16 20"), CHURN_SWEEP (default "control 30 2"),
#      BENCH_RUNS (10), BENCH_DUR (30s), BENCH_WORKERS (64), CHURN_BATCH (2),
#      CHURN_HOLD (8).
ksweep() {
    local kvals; read -r -a kvals <<< "${K_SWEEP:-4 8 12 16 20}"
    local sweep; read -r -a sweep <<< "${CHURN_SWEEP:-control 30 2}"
    local runs="${RUNS:-${BENCH_RUNS:-10}}"
    local dur="${BENCH_DUR:-30s}"
    local workers="${BENCH_WORKERS:-64}"
    local valb="${BENCH_VALUE_BYTES:-64}"
    local hold="${CHURN_HOLD:-8}"
    local batch="${CHURN_BATCH:-2}"

    log "k-sweep: k={${kvals[*]}} churn={${sweep[*]}} runs=$runs dur=$dur N=$(( ${#HOSTS[@]} * NODES_PER_HOST )) (terminal-only)"
    local -a rows=()
    build; deploy
    local k
    for k in "${kvals[@]}"; do
        export BUCKET_SIZE="$k"
        log "════ k=$k (bucket-size=$k) ════"
        stop_cluster
        collect_peer_ids
        start_cluster
        resolve_node_ips
        local targets; targets=$(ingress_targets)
        printf '%6s  %12s  %14s  %14s\n' k events/min split-create split-read
        local g
        for g in "${sweep[@]}"; do
            local gap b
            if [[ "$g" == "control" || "$g" == "0" ]]; then gap=0; b=0; else gap="$g"; b="$batch"; fi
            local epm="0"
            (( b > 0 )) && epm=$(awk "BEGIN{printf \"%.1f\", 60.0*$b/($hold+$gap)}")
            local per total
            per="${dur%s}"
            total=$(( per * runs + 30 ))
            churn_injector_rate "$total" "$gap" "$b" "$hold" &
            local inj=$!
            local out
            out=$(remote taurus "$REMOTE_DIR/ditoload -scenario split -workers $workers -runs $runs -dur $dur -targets '$targets' -value-bytes $valb" 2>&1) || true
            kill "$inj" 2>/dev/null || true; wait "$inj" 2>/dev/null || true
            local sc sr cliv dcon mav
            sc=$(  echo "$out" | grep -E 'A split-brain double-create'  | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
            sr=$(  echo "$out" | grep -E 'A split-brain divergent-read' | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
            cliv=$(echo "$out" | grep -E 'C liveness'                   | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
            dcon=$(echo "$out" | grep -E 'D congestion'                 | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
            mav=$( echo "$out" | grep -E 'min 1s availability'          | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
            printf '%6s  %12s  %14s  %14s\n' "$k" "$epm" "${sc:-ERR}" "${sr:-?}"
            rows+=("$(printf '%6s  %12s  %14s  %14s  %10s  %10s  %10s' \
                "$k" "$epm" "${sc:-ERR}" "${sr:-?}" "${cliv:-?}" "${dcon:-?}" "${mav:-?}")")
        done
    done

    echo
    echo "═════════════ KSWEEP SUMMARY  N=$(( ${#HOSTS[@]} * NODES_PER_HOST ))  R=$R  runs=$runs ═════════════"
    printf '%6s  %12s  %14s  %14s  %10s  %10s  %10s\n' \
        k events/min split-create split-read C-live% D-cong% minAvail%
    local row; for row in "${rows[@]}"; do echo "$row"; done
    echo "split-create / split-read are class-A divergence (founding split-brain) — MUST be 0."
    echo "════════════════════════════════════════════════════════════════════════════════════════"
    log "k-sweep complete."
}

# settlesweep — split-brain vs PRE-EXECUTION settle time, k fixed, no churn.
#
# Axis 3 of THRESHOLD_SWEEP_PLAN.md. For each T in SETTLE_SWEEP (descending),
# bring up a FRESH cluster, wait exactly T s for routing to converge (via
# START_SETTLE → start_cluster's stabilize window), then run the `split` probe
# with NO concurrent churn and tabulate the cross-run-mean class-A split counts.
# At T→0 routing tables are still inconsistent (ClosestPeers disagree) → more
# founding divergence; as T grows, splits decay to the k-dependent steady-state
# floor. The smallest T that still yields ~0 split-create is the minimum safe
# pre-execution settle.
#
# PREREQUISITE — run at the EDGE k (BUCKET_SIZE) found by ksweep. At the default
# k=20 routing converges fast and splits stay ~0 at every T (a real result, but
# not a threshold). Set BUCKET_SIZE to the sensitive k first.
#
# Env: SETTLE_SWEEP (default "120 60 30 15 8 0"), BUCKET_SIZE (edge k; required to
#      see a threshold), BENCH_RUNS (10), BENCH_DUR (60s), BENCH_WORKERS (128).
settlesweep() {
    local sweep; read -r -a sweep <<< "${SETTLE_SWEEP:-120 60 30 15 8 0}"
    local runs="${RUNS:-${BENCH_RUNS:-10}}"
    local dur="${BENCH_DUR:-60s}"
    local workers="${BENCH_WORKERS:-128}"
    local valb="${BENCH_VALUE_BYTES:-64}"

    log "settle sweep: T={${sweep[*]}}s runs=$runs dur=$dur workers=$workers k=${BUCKET_SIZE:-$K} N=$(( ${#HOSTS[@]} * NODES_PER_HOST )) (terminal-only)"
    if [[ -z "${BUCKET_SIZE:-}" ]]; then
        log "  WARNING: default routing table (k=$K) — splits likely ~0 at every T."
        log "           Re-run with BUCKET_SIZE=<edge k from ksweep> to see a threshold."
    fi
    local -a rows=()
    build; deploy
    printf '%10s  %14s  %14s  %10s  %10s  %10s\n' \
        settle_s split-create split-read C-live% D-cong% minAvail%
    local t
    for t in "${sweep[@]}"; do
        log "════ settle=${t}s (k=${BUCKET_SIZE:-$K}) ════"
        stop_cluster
        collect_peer_ids
        START_SETTLE="$t" start_cluster
        resolve_node_ips
        local targets; targets=$(ingress_targets)

        local out
        out=$(remote taurus "$REMOTE_DIR/ditoload -scenario split -workers $workers -runs $runs -dur $dur -targets '$targets' -value-bytes $valb" 2>&1) || true

        local sc sr cliv dcon mav
        sc=$(  echo "$out" | grep -E 'A split-brain double-create'  | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        sr=$(  echo "$out" | grep -E 'A split-brain divergent-read' | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        cliv=$(echo "$out" | grep -E 'C liveness'                   | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        dcon=$(echo "$out" | grep -E 'D congestion'                 | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        mav=$( echo "$out" | grep -E 'min 1s availability'          | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        printf '%10s  %14s  %14s  %10s  %10s  %10s\n' \
            "$t" "${sc:-ERR}" "${sr:-?}" "${cliv:-?}" "${dcon:-?}" "${mav:-?}"
        rows+=("$(printf '%10s  %14s  %14s  %10s  %10s  %10s' \
            "$t" "${sc:-ERR}" "${sr:-?}" "${cliv:-?}" "${dcon:-?}" "${mav:-?}")")
    done

    echo
    echo "═════════════ SETTLESWEEP SUMMARY  N=$(( ${#HOSTS[@]} * NODES_PER_HOST ))  R=$R  k=${BUCKET_SIZE:-$K}  runs=$runs ═════════════"
    printf '%10s  %14s  %14s  %10s  %10s  %10s\n' \
        settle_s split-create split-read C-live% D-cong% minAvail%
    local row; for row in "${rows[@]}"; do echo "$row"; done
    echo "split-create / split-read are class-A divergence (founding split-brain) — MUST be 0."
    echo "Smallest settle_s with ~0 split-create = minimum safe pre-execution settle."
    echo "════════════════════════════════════════════════════════════════════════════════════════"
    log "settle sweep complete."
}

# cluster_health prints "<live> <expected>" by summing the status table. Used as
# a gate so a benchmark is never run against a dead cluster (where every op is a
# class-C liveness failure and the run would report a meaningless pass).
cluster_health() {
    local total live
    total=$(( ${#HOSTS[@]} * NODES_PER_HOST ))
    live=$(status 2>/dev/null | grep -oE '[0-9]+ / [0-9]+' | awk '{l+=$1} END{print l+0}')
    echo "${live:-0} ${total}"
}

# bench_suite — run every thesis workload scenario + the membership probe against
# the LIVE cluster and print ONE consolidated summary table to the terminal.
# Nothing is written to disk. Each scenario is porcupine-checked inside ditoload;
# the summary's ClassA column is the correctness/divergence verdict (must be 0).
# Matches the paper's setup: W=200, 90 s, RUNS runs (override with --workers/--dur).
bench_suite() {
    log "Resolving node IPs..."
    resolve_node_ips
    local runs="${RUNS:-10}"

    local live total
    read -r live total < <(cluster_health)
    if (( total == 0 || live * 10 < total * 9 )); then
        log "FATAL: cluster not healthy ($live/$total nodes live) — aborting suite."
        exit 1
    fi
    log "Cluster healthy: $live/$total nodes live."

    # Paper setup: fixed concurrency W=200, 90 s runs. CLI --workers/--dur win.
    export BENCH_WORKERS="${BENCH_WORKERS:-200}"
    export BENCH_DUR="${BENCH_DUR:-90s}"
    log "Suite: scenarios at W=$BENCH_WORKERS dur=$BENCH_DUR runs=$runs (terminal-only)"

    local scenarios=(fill read mixed hammer stale churn)
    local -a rows=()
    local sc
    for sc in "${scenarios[@]}"; do
        log "════ bench $sc ════"
        local out
        out=$(run_bench "$sc" 2>&1) || true
        echo "$out"
        # Cross-run means from the aggregate report (first number after the colon).
        local tp sr p50 p99 ca
        tp=$( echo "$out" | grep -E '^[[:space:]]*throughput' | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        sr=$( echo "$out" | grep -E 'success rate'           | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        p50=$(echo "$out" | grep -E 'latency p50'            | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        p99=$(echo "$out" | grep -E 'latency p99 '           | sed -E 's/.*: *([0-9.]+).*/\1/' | head -1)
        # Class-A correctness/divergence = lin + ghost + stale (+ split-brain).
        ca=$(echo "$out" | grep -E 'A (lin violations|ghost values|stale reads|split-brain)' \
             | sed -E 's/.*: *([0-9.]+).*/\1/' | awk '{s+=$1} END{printf "%g", s+0}')
        rows+=("$(printf '%-9s %12s %9s %9s %9s %8s' "$sc" "${tp:-ERR}" "${sr:-?}" "${p50:-?}" "${p99:-?}" "${ca:-?}")")
    done

    log "════ membership probe (O(R)-per-node table) ════"
    bench_membership || true

    # ── Consolidated terminal summary ───────────────────────────────────────
    echo
    echo "═════════════ BENCH SUMMARY  N=$total  R=$R  k=${BUCKET_SIZE:-$K}  W=$BENCH_WORKERS  dur=$BENCH_DUR  runs=$runs ═════════════"
    printf '%-9s %12s %9s %9s %9s %8s\n' scenario thr_op/s succ% p50_ms p99_ms ClassA
    local row
    for row in "${rows[@]}"; do echo "$row"; done
    echo "ClassA = lin+ghost+stale(+split-brain) violations summed over all runs — MUST be 0."
    echo "═══════════════════════════════════════════════════════════════════════════════════════════"
}

# run_bench [scenario]  — drive a single benchmark scenario from taurus. The
# porcupine linearizability check runs inside ditoload for every scenario, so a
# non-zero class-A KPI means actually-wrong data (exit 1). See `ditoload
# -scenario list` for the per-scenario definition of "unsuccessful".
run_bench() {
    local scenario="${1:-mixed}"
    log "Resolving node IPs..."
    resolve_node_ips
    local targets
    targets=$(ingress_targets)

    local runs="${RUNS:-${BENCH_RUNS:-10}}"
    local valb="${BENCH_VALUE_BYTES:-256}"

    # Optional overrides — empty means "use ditoload's per-scenario default".
    local extra=""
    [[ -n "${BENCH_WORKERS:-}" ]] && extra="$extra -workers ${BENCH_WORKERS}"
    [[ -n "${BENCH_KEYS:-}"    ]] && extra="$extra -keys ${BENCH_KEYS}"
    [[ -n "${BENCH_DUR:-}"     ]] && extra="$extra -dur ${BENCH_DUR}"

    # Full-audit scenarios poll EVERY node once (/debug/dump) after the run, so
    # pass the all-node URL list and uncap porcupine (-per-key-cap 0 = check every
    # key's full history). rechurn uses the wide range to also reach joiners.
    case "$scenario" in
        mixedfull) extra="$extra -audit-targets '$(all_targets)' -per-key-cap 0" ;;
        rechurn)   extra="$extra -audit-targets '$(audit_targets_wide)' -per-key-cap 0 -audit-settle ${AUDIT_SETTLE:-45s}" ;;
    esac

    local cmd="$REMOTE_DIR/ditoload -scenario $scenario -runs $runs -targets '$targets' -value-bytes $valb$extra"

    if [[ "$scenario" == "churn" ]]; then
        # Cover all runs with the fault injector. Default per-run churn dur=90s.
        local per="${BENCH_DUR:-90s}"; per="${per%s}"
        local total=$(( (per + 5) * runs + 30 ))
        log "Running churn bench from taurus ($runs runs) with concurrent fault injection..."
        churn_injector "$total" &
        local inj=$!
        remote taurus "$cmd" || true
        kill "$inj" 2>/dev/null || true
        wait "$inj" 2>/dev/null || true
    elif [[ "$scenario" == "rechurn" ]]; then
        # REAL topology churn: decommission + join concurrently with the load. The
        # injector covers the WORKLOAD only (per*runs + small pad) and then stops,
        # so ditoload's -audit-settle pause lets migrations drain and the final
        # audit sees the SETTLED state, not mid-migration churn. Use -runs 1 for a
        # fully clean post-churn audit (multi-run only settles the last run).
        local per="${BENCH_DUR:-90s}"; per="${per%s}"
        local total=$(( per * runs + 15 ))
        log "Running rechurn bench from taurus ($runs runs) with REAL topology churn (join + decommission)..."
        topology_injector "$total" &
        local inj=$!
        remote taurus "$cmd" || true
        kill "$inj" 2>/dev/null || true
        wait "$inj" 2>/dev/null || true
    elif [[ "$scenario" == "split" && "${TOPO_CHURN:-0}" == "1" ]]; then
        # Authoritative split-brain probe (client-observed splitCreate/splitRead)
        # under REAL topology churn. No settle/audit needed — split detects via the
        # client path in real time, so ditoload's native -runs N (mean±stddev) is
        # valid; the injector simply churns throughout all runs.
        local per="${BENCH_DUR:-120s}"; per="${per%s}"
        local total=$(( (per + 5) * runs + 30 ))
        log "Running split bench from taurus ($runs runs) under REAL topology churn (join + decommission)..."
        topology_injector "$total" &
        local inj=$!
        remote taurus "$cmd" || true
        kill "$inj" 2>/dev/null || true
        wait "$inj" 2>/dev/null || true
    else
        log "Running $scenario bench from taurus ($runs runs)..."
        remote taurus "$cmd"
    fi
}

# bench_membership probes /debug/membership on EVERY node and reports the
# tracked-peer distribution — empirical validation of the O(R)-per-node claim.
bench_membership() {
    log "Resolving node IPs..."
    resolve_node_ips
    local targets
    targets=$(all_targets)
    log "Probing /debug/membership on $(( ${#HOSTS[@]} * NODES_PER_HOST )) nodes..."
    remote taurus "$REMOTE_DIR/ditoload -membership -targets '$targets'"
}

# scale — redeploy the cluster at a sweep of sizes, running the mixed bench and
# the membership probe at each. Validates throughput/latency scaling and the
# bounded membership table vs N. Set SCALE_SWEEP="10 20 40 80" to customise.
scale() {
    local sweep
    read -r -a sweep <<< "${SCALE_SWEEP:-10 20 40 80}"
    log "Scale sweep over NODES_PER_HOST = ${sweep[*]} (×${#HOSTS[@]} hosts)"
    build; deploy
    for nph in "${sweep[@]}"; do
        NODES_PER_HOST="$nph"
        log "════ N=$(( ${#HOSTS[@]} * nph )) ($nph nodes/host) ════"
        stop_cluster
        collect_peer_ids
        start_cluster
        run_bench mixed
        bench_membership
    done
    log "Scale sweep complete."
}

# saturate [scenario] — closed-loop worker sweep to find the TRUE throughput
# ceiling. ditoload is closed-loop (each worker blocks for its reply), so
# throughput ≈ workers ÷ mean-latency: a fixed worker count measures
# throughput-at-that-concurrency, NOT capacity. This ramps -workers over
# SAT_WORKERS and tabulates throughput + latency vs workers. The KNEE — where
# throughput plateaus while latency climbs — is the real system limit; if
# throughput is still rising at the top level, raise SAT_WORKERS. Runs against
# the LIVE cluster (no restart). Results + per-point logs land in a CSV.
#   Env: SAT_WORKERS (default "50 100 200 400 800 1600"), SAT_RUNS (default 3),
#        SAT_DUR (default 30s), BENCH_VALUE_BYTES.
saturate() {
    local scenario="${1:-mixed}"
    log "Resolving node IPs..."
    resolve_node_ips
    local targets; targets=$(ingress_targets)
    local runs="${RUNS:-${SAT_RUNS:-3}}"
    local dur="${SAT_DUR:-30s}"
    local valb="${BENCH_VALUE_BYTES:-256}"
    local levels; read -r -a levels <<< "${SAT_WORKERS:-50 100 200 400 800 1600}"

    log "Saturation sweep: scenario=$scenario workers={${levels[*]}} runs=$runs dur=$dur restart=${SAT_RESTART:-0} (terminal-only)"
    local -a rows=()
    printf '%8s  %14s  %8s  %10s  %10s  %10s\n' workers throughput "succ%" p50_ms p99_ms p999_ms
    local w out tp sr p50 p99 p999
    for w in "${levels[@]}"; do
        # SAT_RESTART=1 → measure each worker level on a FRESH cluster, so the
        # throughput-vs-workers curve is not confounded by accumulated Paxos
        # groups/data from earlier levels (binaries already deployed; no rebuild).
        # Run the restart in a SUBSHELL so collect_peer_ids' internal `exit` only
        # kills the subshell (not the sweep) and '|| true' defuses set -e; the
        # subshell still launches the nodes (SEED_MULTIADDRS is used within it).
        if [[ "${SAT_RESTART:-0}" == "1" ]]; then
            log "  [w=$w] restarting cluster for a clean measurement..."
            ( stop_cluster; collect_peer_ids; start_cluster ) >/dev/null 2>&1 || true
            local lc total
            total=$(( ${#HOSTS[@]} * NODES_PER_HOST ))
            lc=$(status 2>/dev/null | grep -oE '[0-9]+ / [0-9]+' | awk '{l+=$1} END{print l+0}')
            if (( lc * 10 < total * 9 )); then
                log "  [w=$w] UNHEALTHY after restart ($lc/$total live) — skipping level"
                rows+=("$(printf '%8s  %14s  %8s  %10s  %10s  %10s' "$w" "unhealthy_${lc}" "-" "-" "-" "-")")
                continue
            fi
            log "  [w=$w] cluster healthy ($lc/$total live)"
        fi
        # '|| true' so a nonzero ditoload exit (e.g. it flags class-A, or a
        # transient ssh hiccup) is shown and the sweep continues, instead of
        # set -e aborting the whole loop mid-sweep.
        out=$(remote taurus "$REMOTE_DIR/ditoload -scenario $scenario -workers $w -runs $runs -dur $dur -targets '$targets' -value-bytes $valb" 2>&1) || true
        # Pull the mean (first number after the colon) from each report line.
        tp=$(  echo "$out" | grep -E '^[[:space:]]*throughput' | sed -E 's/.*: *([0-9.]+).*/\1/')
        sr=$(  echo "$out" | grep -E 'success rate'            | sed -E 's/.*: *([0-9.]+).*/\1/')
        p50=$( echo "$out" | grep -E 'latency p50'             | sed -E 's/.*: *([0-9.]+).*/\1/')
        p99=$( echo "$out" | grep -E 'latency p99 '            | sed -E 's/.*: *([0-9.]+).*/\1/')
        p999=$(echo "$out" | grep -E 'latency p99\.9'          | sed -E 's/.*: *([0-9.]+).*/\1/')
        printf '%8s  %14s  %8s  %10s  %10s  %10s\n' "$w" "${tp:-ERR}" "${sr:-?}" "${p50:-?}" "${p99:-?}" "${p999:-?}"
        rows+=("$(printf '%8s  %14s  %8s  %10s  %10s  %10s' "$w" "${tp:-ERR}" "${sr:-?}" "${p50:-?}" "${p99:-?}" "${p999:-?}")")
    done

    echo
    echo "═════════════ SATURATION SUMMARY  scenario=$scenario  N=$(( ${#HOSTS[@]} * NODES_PER_HOST ))  R=$R  k=${BUCKET_SIZE:-$K}  runs=$runs ═════════════"
    printf '%8s  %14s  %8s  %10s  %10s  %10s\n' workers throughput "succ%" p50_ms p99_ms p999_ms
    local row; for row in "${rows[@]}"; do echo "$row"; done
    echo "The knee — throughput plateaus while latency climbs — is the system's true ceiling."
    echo "════════════════════════════════════════════════════════════════════════════════════════"
    log "Saturation sweep complete."
}

# ── Entry point ────────────────────────────────────────────────────────────

usage() {
    sed -n '2,55p' "$0" | sed 's/^# \{0,1\}//'
}

# Flags may appear before or after the command; positionals collect the command
# and its scenario argument.
POSITIONAL=()
while (( $# )); do
    case "$1" in
        -n|--nodes)   TOTAL_NODES="$2";     shift 2 ;;
        --per-host)   NODES_PER_HOST="$2";  TOTAL_NODES=""; shift 2 ;;
        -r|--repl)    R="$2";               shift 2 ;;
        -k|--bucket)  K="$2";               shift 2 ;;
        --runs)       RUNS="$2";            shift 2 ;;
        --workers)    export BENCH_WORKERS="$2"; shift 2 ;;
        --dur)        export BENCH_DUR="$2";     shift 2 ;;
        --hosts)      IFS=',' read -r -a HOSTS <<< "$2"; shift 2 ;;
        -h|--help)    usage; exit 0 ;;
        --)           shift; while (( $# )); do POSITIONAL+=("$1"); shift; done ;;
        -*)           echo "unknown flag: $1" >&2; usage >&2; exit 2 ;;
        *)            POSITIONAL+=("$1"); shift ;;
    esac
done

# -n (total) takes precedence over the per-host default: divide across hosts,
# rounding up so the cluster has at least the requested node count.
if [[ -n "$TOTAL_NODES" ]]; then
    NODES_PER_HOST=$(( (TOTAL_NODES + ${#HOSTS[@]} - 1) / ${#HOSTS[@]} ))
fi

CMD="${POSITIONAL[0]:-start}"
SCEN="${POSITIONAL[1]:-}"

case "$CMD" in
    start)      build; deploy; collect_peer_ids; start_cluster ;;
    deploy)     build; deploy ;;
    stop)       stop_cluster ;;
    bench)      if [[ -n "$SCEN" ]]; then run_bench "$SCEN"; else bench_suite; fi ;;
    membership) bench_membership ;;
    scale)      scale ;;
    saturate)   saturate "${SCEN:-mixed}" ;;
    churnsweep) churnsweep ;;
    rechurnsweep) rechurnsweep ;;
    ksweep)     ksweep ;;
    settlesweep) settlesweep ;;
    logs)       show_logs ;;
    status)     status ;;
    *)          usage >&2; exit 1 ;;
esac
