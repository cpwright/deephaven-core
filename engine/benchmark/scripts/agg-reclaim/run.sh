#!/usr/bin/env bash
#
# Run the aggregation state reclaiming benchmarks against the baseline (upstream main with the fresh-block change) and
# against the pull request, one at a time.
#
# Usage: run.sh <results-dir> [phase ...]
#
# Phases, run in the order given (default: tables):
#   tables  the design document's tables: 1M live rows, 10k rows added and removed per cycle, 1 and 100 rows per group
#   large   10M live rows, 100k rows per cycle, 1 row per group; needs about 64 GB of RAM
#
# The worktrees default to siblings of the results directory: bench-agg-main and bench-agg-pr. Override them with
# WT_MAIN and WT_PR. A log that already ends in "# Run complete" is skipped, so the script can be rerun to resume. Set
# DRY_RUN=1 to print the commands without running them.
set -uo pipefail

if [ $# -lt 1 ]; then
    sed -n '3,15p' "$0"
    exit 1
fi
mkdir -p "$1"
OUT=$(cd "$1" && pwd)
shift
PHASES=("$@")
if [ ${#PHASES[@]} -eq 0 ]; then
    PHASES=(tables)
fi

BASE=$(cd "$OUT/.." && pwd)
WT_MAIN=${WT_MAIN:-$BASE/bench-agg-main}
WT_PR=${WT_PR:-$BASE/bench-agg-pr}
mkdir -p "$OUT/logs" "$OUT/jmh"

# two forks per configuration, three warmup and five measured batches of 900 cycles each; each phase adds its JVM
# arguments in a single -jvmArgsAppend, since JMH keeps only one
JMH_COMMON="-f 2 -wi 3 -i 5"
VALIDATE="-DBaseTable.validateUpdateIndices=false"

machine_info() {
    {
        echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "uname: $(uname -a)"
        lscpu 2> /dev/null | grep -E 'Model name|^CPU\(s\)|Thread|Core|Socket|MHz|NUMA node\(s\)'
        free -g 2> /dev/null | head -2
        for gov in /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor; do
            [ -r "$gov" ] && echo "governor: $(cat "$gov")"
        done
        [ -r /sys/devices/system/cpu/intel_pstate/no_turbo ] && echo "no_turbo: $(cat /sys/devices/system/cpu/intel_pstate/no_turbo)"
        [ -r /sys/devices/system/cpu/cpufreq/boost ] && echo "boost: $(cat /sys/devices/system/cpu/cpufreq/boost)"
        echo "load: $(cat /proc/loadavg 2> /dev/null)"
        for wt in "$WT_MAIN" "$WT_PR"; do
            echo "worktree $wt: $(git -C "$wt" rev-parse --short=10 HEAD) $(git -C "$wt" status --porcelain | wc -l | tr -d ' ') uncommitted"
        done
    } > "$OUT/machine.txt" 2>&1
}

# run_one <worktree> <log name> <jmh args>
run_one() {
    local wt=$1 name=$2 args=$3
    local log="$OUT/logs/$name.log"
    if [ -f "$log" ] && grep -q '^# Run complete' "$log"; then
        echo "skip $name (done)"
        return
    fi
    if [ -n "${DRY_RUN:-}" ]; then
        echo "(cd $wt && ./gradlew --no-daemon :engine-benchmark:jmhRunAggregationIncremental --args=\"$args -rf csv -rff $OUT/jmh/$name.csv\")"
        return
    fi
    echo "$(date -u +%H:%M:%S) start $name"
    (cd "$wt" && ./gradlew --no-daemon :engine-benchmark:jmhRunAggregationIncremental \
        --args="$args -rf csv -rff $OUT/jmh/$name.csv") > "$log" 2>&1
    local rc=$?
    echo "$(date -u +%H:%M:%S) end $name exit=$rc"
}

# config <phase> <size args> <build> <worktree> <config label> <mode args>
config() {
    local phase=$1 sizeArgs=$2 build=$3 wt=$4 label=$5 mode=$6
    run_one "$wt" "${phase}__${build}__${label}__add" \
        "AggregationIncrementalBenchmark.addOnly $JMH_COMMON $sizeArgs -p keysReturn=false $mode"
    run_one "$wt" "${phase}__${build}__${label}__churn" \
        "AggregationIncrementalBenchmark.(slidingWindow|randomChurn) $JMH_COMMON $sizeArgs -p keysReturn=false,true $mode"
}

# the configurations of one phase, with the baseline run first and again last to show any drift over the run. The
# baseline's benchmark takes the collapse fraction as its own parameter; the pull request's names it in the reclaim mode.
phase_configs() {
    local phase=$1 sizeArgs=$2
    config "$phase" "$sizeArgs" main "$WT_MAIN" none "-p reclaim=none -p collapse=1"
    config "$phase" "$sizeArgs" pr "$WT_PR" none "-p reclaim=none"
    config "$phase" "$sizeArgs" pr "$WT_PR" blocks "-p reclaim=blocks"
    config "$phase" "$sizeArgs" pr "$WT_PR" collapse075 "-p reclaim=collapse0.75"
    config "$phase" "$sizeArgs" pr "$WT_PR" collapse05 "-p reclaim=collapse0.5"
    config "$phase" "$sizeArgs" mainagain "$WT_MAIN" none "-p reclaim=none -p collapse=1"
}

machine_info
for phase in "${PHASES[@]}"; do
    case "$phase" in
        tables)
            phase_configs tables "-p windowSize=1000000 -p rowsPerCycle=10000 -p rowsPerKey=1,100 -jvmArgsAppend $VALIDATE"
            ;;
        large)
            phase_configs large "-p windowSize=10000000 -p rowsPerCycle=100000 -p rowsPerKey=1 -jvmArgsAppend '$VALIDATE -Xmx48g'"
            ;;
        *)
            echo "unknown phase $phase"
            exit 1
            ;;
    esac
done
machine_info
echo "done; now run: python3 $(dirname "$0")/parse_logs.py $OUT"
