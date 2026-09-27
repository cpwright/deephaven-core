#!/usr/bin/env bash
#
# Run the aggregation state reclaiming benchmark matrix against four builds, one at a time.
#
# Usage: run.sh <results-dir> [phase ...]
#
# Phases, run in the order given (default: s1 s10 s100 build):
#   s1         1M distinct keys per batch: 100k live rows, 1k rows added and removed per cycle
#   s10        10M distinct keys per batch: 1M live rows, 10k rows per cycle
#   s100       100M distinct keys per batch: 10M live rows, 100k rows per cycle (needs about 64 GB of RAM)
#   s10rpk100  the s10 sizes with 100 rows per group
#   build      the initial-build benchmark, int versus long positions
#
# The worktrees default to siblings of the results directory's parent; override with WT_MAIN, WT_MAIN_8676, WT_PR and
# WT_LONG. A log that already ends in "# Run complete" is skipped, so the script can be rerun to resume. Set DRY_RUN=1 to
# print the commands without running them.
set -uo pipefail

if [ $# -lt 1 ]; then
    sed -n '3,18p' "$0"
    exit 1
fi
mkdir -p "$1"
OUT=$(cd "$1" && pwd)
shift
PHASES=("$@")
if [ ${#PHASES[@]} -eq 0 ]; then
    PHASES=(s1 s10 s100 build)
fi

BASE=$(cd "$OUT/.." && pwd)
WT_MAIN=${WT_MAIN:-$BASE/bench-agg-main}
WT_MAIN_8676=${WT_MAIN_8676:-$BASE/bench-agg-main-8676}
WT_PR=${WT_PR:-$BASE/bench-agg-pr}
WT_LONG=${WT_LONG:-$BASE/bench-agg-long}
mkdir -p "$OUT/logs" "$OUT/jmh"

machine_info() {
    {
        echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "uname: $(uname -a)"
        if command -v lscpu > /dev/null; then
            lscpu | grep -E 'Model name|^CPU\(s\)|Thread|Core|Socket|MHz'
            free -g | head -2
        else
            sysctl -n machdep.cpu.brand_string hw.ncpu hw.memsize 2> /dev/null
        fi
        for wt in "$WT_MAIN" "$WT_MAIN_8676" "$WT_PR" "$WT_LONG"; do
            echo "worktree $wt: $(git -C "$wt" rev-parse --short=10 HEAD) $(git -C "$wt" status --porcelain | wc -l | tr -d ' ') uncommitted"
        done
    } > "$OUT/machine.txt" 2>&1
}

# run_one <worktree> <gradle task> <log name> <jmh args>
run_one() {
    local wt=$1 task=$2 name=$3 args=$4
    local log="$OUT/logs/$name.log"
    if [ -f "$log" ] && grep -q '^# Run complete' "$log"; then
        echo "skip $name (done)"
        return
    fi
    if [ -n "${DRY_RUN:-}" ]; then
        echo "(cd $wt && ./gradlew --no-daemon :engine-benchmark:$task --args=\"$args -rf csv -rff $OUT/jmh/$name.csv\")"
        return
    fi
    echo "$(date -u +%H:%M:%S) start $name"
    (cd "$wt" && ./gradlew --no-daemon ":engine-benchmark:$task" \
        --args="$args -rf csv -rff $OUT/jmh/$name.csv") > "$log" 2>&1
    local rc=$?
    echo "$(date -u +%H:%M:%S) end $name exit=$rc"
}

# incremental <size label> <size args> <build label> <worktree> <config label> <mode args>
incremental() {
    local size=$1 sizeArgs=$2 build=$3 wt=$4 config=$5 mode=$6
    run_one "$wt" jmhRunAggregationIncremental "${size}__${build}__${config}__add" \
        "AggregationIncrementalBenchmark.addOnly $sizeArgs -p keysReturn=false $mode"
    run_one "$wt" jmhRunAggregationIncremental "${size}__${build}__${config}__churn" \
        "AggregationIncrementalBenchmark.(slidingWindow|randomChurn) $sizeArgs -p keysReturn=false,true $mode"
}

NONE="-p reclaim=none -p collapse=1 -p blockShift=-1"
# the release mode configurations, as collapseFreeFraction blockShiftFraction bulkShift
declare -a CONFIGS=(
    "default 1 -1 false"
    "sweep075 0.75 0 false"
    "bulk 1 0 true"
    "bulk075 0.75 0 true"
    "bulk05 0.5 0 true"
)
mode() {
    echo "-p reclaim=blocks -p collapse=$1 -p blockShift=$2 -p bulkShift=$3"
}

# matrix <size label> <size args>
matrix() {
    local size=$1 sizeArgs=$2
    incremental "$size" "$sizeArgs" main "$WT_MAIN" none "$NONE"
    incremental "$size" "$sizeArgs" main8676 "$WT_MAIN_8676" none "$NONE"
    incremental "$size" "$sizeArgs" pr "$WT_PR" none "$NONE -p bulkShift=false"
    for c in "${CONFIGS[@]}"; do
        set -- $c
        incremental "$size" "$sizeArgs" pr "$WT_PR" "$1" "$(mode "$2" "$3" "$4")"
        # the long-positions build right after the int build, for the configurations it is measured in
        if [ "$1" = default ] || [ "$1" = bulk05 ]; then
            incremental "$size" "$sizeArgs" long "$WT_LONG" "$1" "$(mode "$2" "$3" "$4")"
        fi
    done
}

VALIDATE="-DBaseTable.validateUpdateIndices=false"
machine_info
for phase in "${PHASES[@]}"; do
    case $phase in
        s1)
            matrix s1 "-f 1 -wi 3 -i 5 -p rowsPerKey=1 -p windowSize=100000 -p rowsPerCycle=1000 -jvmArgsAppend $VALIDATE"
            ;;
        s10)
            matrix s10 "-f 1 -wi 3 -i 5 -p rowsPerKey=1 -p windowSize=1000000 -p rowsPerCycle=10000 -jvmArgsAppend $VALIDATE"
            ;;
        s10rpk100)
            matrix s10rpk100 "-f 1 -wi 3 -i 5 -p rowsPerKey=100 -p windowSize=1000000 -p rowsPerCycle=10000 -jvmArgsAppend $VALIDATE"
            ;;
        s100)
            matrix s100 "-f 1 -wi 1 -i 3 -p rowsPerKey=1 -p windowSize=10000000 -p rowsPerCycle=100000 -jvmArgsAppend '$VALIDATE -Xmx48g'"
            ;;
        build)
            for b in pr long; do
                if [ $b = pr ]; then wt=$WT_PR; else wt=$WT_LONG; fi
                run_one "$wt" jmhRunAggregationBuild "build__${b}__blocks__build" \
                    "AggregationBuildBenchmark -f 1 -p reclaim=blocks -p keyCount=100000,5000000 -p keyType=long,String -jvmArgsAppend $VALIDATE"
            done
            ;;
        *)
            echo "unknown phase $phase" >&2
            exit 1
            ;;
    esac
done
[ -n "${DRY_RUN:-}" ] && exit 0
python3 "$(dirname "$0")/parse_logs.py" "$OUT"
echo "done: $OUT/results.csv"
