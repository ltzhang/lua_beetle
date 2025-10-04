#!/bin/bash
# Comprehensive benchmark script for Lua Beetle vs TigerBeetle
# Tests various workloads with different hot account ratios and thread counts
#
# The script AUTOMATICALLY starts and stops all backend services:
# - Redis: Port 6379 (auto-started)
# - DragonflyDB: Port 6380 (auto-started)
# - EloqKV: Port 6379 (auto-started, conflicts with Redis - automatically managed)
# - TigerBeetle: Port 3000 (auto-started)
#
# All database files are stored in /mnt/ramdisk/tests/ for performance
# Results are saved in ./benchmark_results/run_YYYYMMDD_HHMMSS/

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration
ACCOUNTS=100000
DURATION=30
BATCH_SIZE=200

# Results directory (under script location)
RESULTS_BASE_DIR="$SCRIPT_DIR/benchmark_results"
OUTPUT_DIR="$RESULTS_BASE_DIR/run_$(date +%Y%m%d_%H%M%S)"

# Database/execution directory (ramdisk for performance)
RAMDISK_DIR="/mnt/ramdisk/tests"

# Create directories
mkdir -p "$OUTPUT_DIR"
mkdir -p "$RAMDISK_DIR"

# Log file
LOGFILE="$OUTPUT_DIR/benchmark.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOGFILE"
}

# Function to start Redis
start_redis() {
    log "Starting Redis..."
    killall redis-server 2>/dev/null || true
    killall eloqkv 2>/dev/null || true
    sleep 1
    cd "$RAMDISK_DIR"
    redis-server --dir "$RAMDISK_DIR" --daemonize yes > /dev/null 2>&1
    sleep 2
    cd - > /dev/null
    log "Redis started (data in $RAMDISK_DIR)"
}

# Function to reset Redis
reset_redis() {
    log "Resetting Redis..."
    redis-cli FLUSHDB > /dev/null 2>&1 || true
    rm -f "$RAMDISK_DIR/dump.rdb" 2>/dev/null || true
    sleep 1
}

# Function to start DragonflyDB
start_dragonfly() {
    log "Starting DragonflyDB..."
    killall dragonfly-x86_64 2>/dev/null || true
    sleep 1
    cd "$RAMDISK_DIR"
    "$SCRIPT_DIR/../third_party/dragonfly-x86_64" \
        --logtostderr --port=6380 --dir="$RAMDISK_DIR" --dbfilename=dragonfly \
        --default_lua_flags=allow-undeclared-keys > dragonfly.log 2>&1 &
    sleep 3
    cd - > /dev/null
    log "DragonflyDB started (data in $RAMDISK_DIR)"
}

# Function to reset DragonflyDB
reset_dragonfly() {
    log "Resetting DragonflyDB..."
    redis-cli -p 6380 FLUSHDB > /dev/null 2>&1 || true
    rm -f "$RAMDISK_DIR/dragonfly-"* 2>/dev/null || true
    sleep 1
}

# Function to start EloqKV
start_eloqkv() {
    log "Starting EloqKV..."
    killall redis-server 2>/dev/null || true
    killall eloqkv 2>/dev/null || true
    sleep 1
    cd "$RAMDISK_DIR"
    "$SCRIPT_DIR/../third_party/eloqkv" > eloqkv.log 2>&1 &
    sleep 3
    cd - > /dev/null
    log "EloqKV started (data in $RAMDISK_DIR)"
}

# Function to reset EloqKV
reset_eloqkv() {
    log "Resetting EloqKV..."
    redis-cli FLUSHDB > /dev/null 2>&1 || true
    sleep 1
}

# Function to reset TigerBeetle
reset_tigerbeetle() {
    log "Resetting TigerBeetle..."
    killall -9 tigerbeetle 2>/dev/null || true
    sleep 2
    cd "$RAMDISK_DIR"
    rm -f 0_0.tigerbeetle tigerbeetle.log
    "$SCRIPT_DIR/../third_party/tigerbeetle" format --cluster=0 --replica=0 --replica-count=1 --development ./0_0.tigerbeetle > /dev/null 2>&1
    "$SCRIPT_DIR/../third_party/tigerbeetle" start --addresses=3000 --development ./0_0.tigerbeetle > tigerbeetle.log 2>&1 &
    sleep 3
    cd - > /dev/null
    log "TigerBeetle restarted (data in $RAMDISK_DIR)"
}

# Function to run a single benchmark
run_benchmark() {
    local mode=$1
    local workload=$2
    local hot_accounts=$3
    local workers=$4

    local test_name="${mode}_${workload}_hot${hot_accounts}_workers${workers}"
    local output_file="$OUTPUT_DIR/${test_name}.txt"

    log "Running: $test_name"

    # Build command (must run from script directory for relative script paths)
    local cmd="./stress_test -mode=$mode -workload=$workload -accounts=$ACCOUNTS -hot-accounts=$hot_accounts -workers=$workers -duration=$DURATION -batch=$BATCH_SIZE -no-cleanup"

    # Add mixed workload parameters
    if [ "$workload" = "mixed" ]; then
        cmd="$cmd -transfer-ratio=0.7 -twophase-ratio=0.2"
    fi

    # Run benchmark from script directory and save output to results directory
    cd "$SCRIPT_DIR"
    if timeout 120 $cmd > "$output_file" 2>&1; then
        log "  ✓ Completed: $test_name"
    else
        log "  ✗ Failed: $test_name"
        echo "FAILED" >> "$output_file"
    fi
    cd - > /dev/null

    # Reset database for next test
    case "$mode" in
        redis)
            reset_redis
            ;;
        dragonfly)
            reset_dragonfly
            ;;
        eloqkv)
            reset_eloqkv
            ;;
        tigerbeetle)
            reset_tigerbeetle
            ;;
    esac
}

# Test configurations
HOT_ACCOUNTS=(1 10 50 100 1000 10000 100000)
WORKLOADS=(transfer lookup twophase mixed)
WORKERS=(1 2 4 8)
MODES=(redis dragonfly eloqkv tigerbeetle)

log "==================================================="
log "Starting Comprehensive Benchmark Suite"
log "==================================================="
log "Script Directory: $SCRIPT_DIR"
log "Results Directory: $OUTPUT_DIR"
log "Execution/DB Directory: $RAMDISK_DIR"
log ""
log "Total Accounts: $ACCOUNTS"
log "Duration per test: ${DURATION}s"
log "Batch Size: $BATCH_SIZE"
log ""
log "Hot Account Configs: ${HOT_ACCOUNTS[*]}"
log "Workloads: ${WORKLOADS[*]}"
log "Worker Counts: ${WORKERS[*]}"
log "Modes: ${MODES[*]}"
log ""

total_tests=$((${#MODES[@]} * ${#WORKLOADS[@]} * ${#HOT_ACCOUNTS[@]} * ${#WORKERS[@]}))
current_test=0

log "Total tests to run: $total_tests"
log "Estimated time: $((total_tests * (DURATION + 10) / 60)) minutes"
log "==================================================="
log ""

start_time=$(date +%s)

# Run all benchmarks
for mode in "${MODES[@]}"; do
    log "======================================="
    log "Testing: $mode"
    log "======================================="

    # Start/reset backend
    case "$mode" in
        redis)
            start_redis
            reset_redis
            ;;
        dragonfly)
            start_dragonfly
            reset_dragonfly
            ;;
        eloqkv)
            start_eloqkv
            reset_eloqkv
            ;;
        tigerbeetle)
            reset_tigerbeetle
            ;;
    esac

    for workload in "${WORKLOADS[@]}"; do
        for hot_accounts in "${HOT_ACCOUNTS[@]}"; do
            for workers in "${WORKERS[@]}"; do
                current_test=$((current_test + 1))
                log "Progress: $current_test / $total_tests"
                run_benchmark "$mode" "$workload" "$hot_accounts" "$workers"
            done
        done
    done
done

end_time=$(date +%s)
elapsed=$((end_time - start_time))

log ""
log "==================================================="
log "Benchmark Suite Completed!"
log "==================================================="
log "Total time: $((elapsed / 60))m $((elapsed % 60))s"
log "Results saved in: $OUTPUT_DIR"
log ""
log "Generating summary..."

# Generate summary CSV
SUMMARY_FILE="$OUTPUT_DIR/summary.csv"
echo "mode,workload,hot_accounts,workers,throughput,avg_latency_ms,transfers,lookups,twophase" > "$SUMMARY_FILE"

for file in "$OUTPUT_DIR"/*.txt; do
    if [ -f "$file" ] && ! grep -q "FAILED" "$file"; then
        # Parse filename
        basename=$(basename "$file" .txt)
        IFS='_' read -ra PARTS <<< "$basename"
        mode="${PARTS[0]}"
        workload="${PARTS[1]}"
        hot_accounts="${PARTS[2]#hot}"
        workers="${PARTS[3]#workers}"

        # Extract metrics
        throughput=$(grep "Throughput:" "$file" | awk '{print $2}')
        latency=$(grep "Average Latency:" "$file" | awk '{print $3}')
        transfers=$(grep "Transfers Created:" "$file" | awk '{print $3}')
        lookups=$(grep "Accounts Looked Up:" "$file" | awk '{print $4}')
        twophase=$(grep "Two-Phase Transfers:" "$file" | awk '{print $3}' | head -1)

        # Default empty values
        transfers=${transfers:-0}
        lookups=${lookups:-0}
        twophase=${twophase:-0}

        echo "$mode,$workload,$hot_accounts,$workers,$throughput,$latency,$transfers,$lookups,$twophase" >> "$SUMMARY_FILE"
    fi
done

log "Summary CSV created: $SUMMARY_FILE"
log ""
log "To view summary:"
log "  cat $SUMMARY_FILE | column -t -s','"
log ""
log "To analyze results:"
log "  python3 analyze_results.py $OUTPUT_DIR"

# Cleanup - stop all services
log "Cleaning up services..."
killall -9 tigerbeetle 2>/dev/null || true
killall -9 dragonfly-x86_64 2>/dev/null || true
killall eloqkv 2>/dev/null || true
killall redis-server 2>/dev/null || true

log "All done!"
