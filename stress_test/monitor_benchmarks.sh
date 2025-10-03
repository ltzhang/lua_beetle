#!/bin/bash
# Monitor benchmark progress

LOGFILE="benchmark_run.log"

if [ ! -f "$LOGFILE" ]; then
    echo "No benchmark running (log file not found)"
    exit 1
fi

echo "=== Benchmark Monitor ==="
echo ""

# Get total tests
total=$(grep "Total tests to run:" "$LOGFILE" | awk '{print $5}')

# Get current progress
current=$(grep "Progress:" "$LOGFILE" | tail -1 | awk '{print $2}')

if [ -n "$total" ] && [ -n "$current" ]; then
    percent=$((current * 100 / total))
    echo "Progress: $current / $total ($percent%)"
else
    echo "Progress: Unknown"
fi

echo ""
echo "Latest entries:"
tail -10 "$LOGFILE"

echo ""
echo "Estimated completion:"
start_time=$(grep "Starting Comprehensive" "$LOGFILE" | head -1 | awk '{print $1, $2}' | tr -d '[]')
if [ -n "$start_time" ]; then
    start_epoch=$(date -d "$start_time" +%s 2>/dev/null || echo "0")
    current_epoch=$(date +%s)
    elapsed=$((current_epoch - start_epoch))

    if [ "$start_epoch" != "0" ] && [ -n "$current" ] && [ "$current" -gt 0 ]; then
        time_per_test=$((elapsed / current))
        remaining=$((total - current))
        eta=$((remaining * time_per_test))

        echo "  Time elapsed: $((elapsed / 60))m $((elapsed % 60))s"
        echo "  Time per test: ${time_per_test}s"
        echo "  ETA: $((eta / 60))m $((eta % 60))s"
    fi
fi

# Check if benchmark is still running
if ps aux | grep -v grep | grep -q "run_benchmarks.sh"; then
    echo ""
    echo "Status: RUNNING"
else
    echo ""
    echo "Status: COMPLETED or STOPPED"

    # Check if completed successfully
    if grep -q "All done!" "$LOGFILE"; then
        echo "Result: COMPLETED SUCCESSFULLY"

        # Show results directory
        results_dir=$(grep "Output Directory:" "$LOGFILE" | awk '{print $3}')
        if [ -n "$results_dir" ]; then
            echo ""
            echo "Results directory: $results_dir"

            if [ -f "$results_dir/summary.csv" ]; then
                num_results=$(wc -l < "$results_dir/summary.csv")
                echo "Results collected: $((num_results - 1))"  # Subtract header
            fi
        fi
    fi
fi
