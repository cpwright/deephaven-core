# Aggregation state reclaiming benchmarks: instructions for a benchmark agent

You are running a fixed set of JMH benchmarks for deephaven-core PR #8631 (DH-23798, reclaiming aggregation states) on
this machine, and packaging the results so that they can be analyzed elsewhere. Do not change any code, benchmark
parameters, or iteration counts. If something fails, record it (see [Reporting](#reporting)) rather than working around
it.

## What is being measured

`AggregationIncrementalBenchmark` runs batches of 900 update cycles of a keyed `sumBy` over a refreshing table, and
`AggregationBuildBenchmark` measures the initial build of one. They run against four builds:

| Build | Branch | Expected HEAD | Contents |
| --- | --- | --- | --- |
| `main` | `cpw/bench-agg-main` | `0ed788fcc0` | main at `a62858ee0c`, plus the benchmark sources |
| `main8676` | `cpw/bench-agg-main-8676` | `4cc16e3790` | main with PR #8676, plus the benchmark sources |
| `pr` | `cpw/bench-agg-pr` | see below | PR #8631 at `8ee9474a9b`, plus these scripts |
| `long` | `cpw/bench-agg-long` | `271bda4038` | PR #8631 with output positions stored as `long` (an experiment) |

`cpw/bench-agg-pr` is `8ee9474a9b` plus one commit that adds this directory; its HEAD is whatever that commit is.

The phases, each a size of the incremental benchmark or the build benchmark:

| Phase | Live rows | Rows added and removed per cycle | Distinct keys per batch | Iterations |
| --- | --- | --- | --- | --- |
| `s1` | 100,000 | 1,000 | 1M | 3 warmup, 5 measured |
| `s10` | 1,000,000 | 10,000 | 10M | 3 warmup, 5 measured |
| `s100` | 10,000,000 | 100,000 | 100M | 1 warmup, 3 measured, 48 GB heap |
| `build` | 10,000,000 | — | — | the benchmark's defaults |
| `s10rpk100` (optional) | 1,000,000 | 10,000 | 100k groups | 3 warmup, 5 measured |

## Machine requirements

- macOS or Linux, with `git`, `bash`, and `python3`.
- At least 64 GB of RAM for `s100`. With less, skip `s100` and say so in `notes.txt`.
- About 20 GB of free disk space.
- Network access for the first build: Gradle downloads its dependencies and a JDK.
- Nothing else running. The results are timings; other load, sleep, or thermal throttling distorts them. Keep a laptop
  on power and stop it from sleeping (for example, `caffeinate -dims` on macOS, or `systemd-inhibit` on Linux).

## Setup

Use one clone of the fork and a worktree per build, all as siblings of the results directory:

```bash
mkdir agg-bench && cd agg-bench
git clone --no-checkout https://github.com/cpwright/deephaven-core.git repo
cd repo
git fetch origin cpw/bench-agg-main cpw/bench-agg-main-8676 cpw/bench-agg-pr cpw/bench-agg-long
git worktree add --detach ../bench-agg-main origin/cpw/bench-agg-main
git worktree add --detach ../bench-agg-main-8676 origin/cpw/bench-agg-main-8676
git worktree add --detach ../bench-agg-pr origin/cpw/bench-agg-pr
git worktree add --detach ../bench-agg-long origin/cpw/bench-agg-long
cd ..
for wt in bench-agg-main bench-agg-main-8676 bench-agg-pr bench-agg-long; do
    echo "$wt $(git -C $wt rev-parse --short=10 HEAD)"
done
```

Check the HEADs against the table above. Then build each worktree once, so that the benchmark runs do not include
compiling (the first build takes a while):

```bash
for wt in bench-agg-main bench-agg-main-8676 bench-agg-pr bench-agg-long; do
    (cd $wt && ./gradlew --no-daemon :engine-benchmark:compileJava) || echo "BUILD FAILED: $wt"
done
```

## Running

Run the whole matrix from the `agg-bench` directory, in `tmux`, `screen`, or with `nohup`, since it takes hours:

```bash
nohup bash bench-agg-pr/engine/benchmark/scripts/agg-reclaim/run.sh results s1 s10 s100 build > run.out 2>&1 &
tail -f run.out
```

- Leave out `s100` if the machine has less than 64 GB of RAM.
- If time allows, run `s10rpk100` afterward: `bash bench-agg-pr/engine/benchmark/scripts/agg-reclaim/run.sh results
  s10rpk100`.
- `run.sh` runs one benchmark at a time and writes one log per run to `results/logs/`. Running the same command again
  skips every run whose log is complete, so after an interruption, rerun it to resume.
- `DRY_RUN=1` prints the commands without running them.

Rough durations, which depend on the machine: `s1` 30 minutes, `s10` 1 to 2 hours, `s100` 3 to 5 hours, `build` 15
minutes.

## Checking

When `run.sh` finishes, it writes `results/results.csv`. Check:

- Every log in `results/logs/` contains `# Run complete`:
  `grep -L '# Run complete' results/logs/*.log` should print nothing.
- No log contains an exception or out-of-memory error:
  `grep -l -E 'OutOfMemoryError|Exception in thread|FAILED' results/logs/*.log` should print nothing.
- `results/results.csv` has the expected number of rows (not counting the header):

  | Phases run | Rows |
  | --- | --- |
  | `s1` | 400 |
  | `s10` | 400 |
  | `s100` | 200 |
  | `build` | 64 |
  | `s10rpk100` | 400 |

If a run failed, rerun `run.sh` once with the same phases. If it fails again, delete nothing; describe it in
`notes.txt`.

## Reporting

Package the results directory and return the archive:

```bash
cd agg-bench
tar czf agg-reclaim-results-$(hostname -s).tar.gz results run.out
```

The archive must contain:

- `results/results.csv`: one row per iteration, described below.
- `results/machine.txt`: the machine and the worktrees' HEADs, written by `run.sh`.
- `results/jmh/*.csv`: JMH's own summary for each run.
- `results/logs/*.log`: the raw logs.
- `results/notes.txt`: anything that went wrong or was skipped, and anything else about the machine or the run that
  could affect timings. Create it even if it only says "no problems".

`results.csv` columns:

| Column | Meaning |
| --- | --- |
| `size` | the phase: `s1`, `s10`, `s100`, `s10rpk100`, or `build` |
| `build` | `main`, `main8676`, `pr`, or `long` |
| `config` | the reclaim configuration: `none`, `default`, `sweep075`, `bulk`, `bulk075`, `bulk05`, or `blocks` for the build benchmark |
| `part` | `add` (the add-only benchmark), `churn` (sliding window and random churn), or `build` |
| `benchmark` | `addOnly`, `slidingWindow`, `randomChurn`, or the build benchmark's method |
| `windowSize`, `rowsPerCycle`, `rowsPerKey`, `keysReturn` | the incremental benchmark's parameters |
| `reclaim`, `collapse`, `blockShift`, `bulkShift` | the reclaim parameters; `bulkShift` is empty for the main builds |
| `tableSize`, `keyCount`, `keyType` | the build benchmark's parameters |
| `fork`, `phase`, `iteration` | the JMH fork, `warmup` or `measure`, and the iteration number |
| `time_ms` | the iteration's time in milliseconds: the whole batch of 900 cycles, or one build |
| `liveStates`, `positionsAssigned`, `lastLivePosition`, `rehashes`, `maxCycleMillis`, `retainedHeapMB` | what the incremental benchmark reports after the iteration; empty for the build benchmark |

`rehashes` is `0` on the main builds, which do not count them, and for the `none` configuration.
