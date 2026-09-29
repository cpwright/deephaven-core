# Aggregation state reclaiming benchmarks: the new build of the pull request versus the previous one

These instructions are for an agent running on a quiet x86-64 Linux machine. They measure the incremental aggregation
benchmark on three builds, in each of the pull request's modes:

- **main:** current upstream main (f93afcf3d4), the baseline.
- **prev:** the build of deephaven-core pull request #8631 that the 2026-09-29b run measured, unchanged
  (`cpw/bench-agg-pr-2026-09-29b`).
- **new:** the pull request with current upstream main merged in, plus the adaptive recycler (#8698) at its latest
  version.

The question is how much faster the new build is than the previous one, measured side by side on one machine. The new
build differs from the previous one by:

- recording no previous values for blocks that an array source allocates during an update cycle (the fresh-block fix,
  also in draft #8713);
- the final version of the adaptive recycler (#8698); the previous build had an earlier version of it;
- tombstoning removed states a chunk at a time;
- 16 upstream main commits.

Follow the steps in order and report back as described in [Reporting the results](#reporting-the-results).

## What is measured

`AggregationIncrementalBenchmark` runs batches of 900 update cycles of a keyed `sumBy` over a refreshing table of
1,000,000 rows, with 10,000 rows added and removed each cycle, and reports the time per batch, the heap retained
afterward, the output positions assigned, and the longest cycle. It runs three workloads (add only, a sliding window, and
random churn), with groups of 1 and of 100 rows, and with and without groups returning after they empty.

Ten configurations run, one after another. Each mode runs on the previous build and then on the new one, so the two
are measured close together:

| Log label | Checkout | Mode |
| --- | --- | --- |
| `main__none` | `bench-agg-main` | no reclaiming (main has no other mode) |
| `prev__none`, `new__none` | `bench-agg-prev`, `bench-agg-pr` | `StateReclaimMode.none()` |
| `prev__blocks`, `new__blocks` | `bench-agg-prev`, `bench-agg-pr` | `releaseBlocks(1)`: release blocks, move nothing |
| `prev__collapse075`, `new__collapse075` | `bench-agg-prev`, `bench-agg-pr` | `releaseBlocks(0.75)` |
| `prev__collapse05`, `new__collapse05` | `bench-agg-prev`, `bench-agg-pr` | `releaseBlocks(0.5)` |
| `mainagain__none` | `bench-agg-main` | no reclaiming, again, to show any drift over the run |

The measured benchmark code is the same in all three checkouts. Only the way a mode is named differs: the previous
build takes the collapse fraction as a `collapse` parameter, and the new build names it in `reclaim` (`collapse0.75`,
`collapse0.5`). `run.sh` passes each build its own form.

Each configuration uses two JVM forks, with three warmup and five measured batches per fork. The `tables` phase takes
about 50 minutes after the first build. The `large` phase repeats the matrix with 10,000,000 rows and 100,000 per cycle,
one row per group, and needs about 64 GB of RAM and about seven more hours. Run both.

## 1. Check the machine

- x86-64 Linux, at least 8 cores and 16 GB of RAM for the `tables` phase.
- Nothing else running: no builds, browsers, containers, or other benchmarks. Do not use the machine during the run.
- A bare-metal host is best. In a container or VM, note it in `notes.txt` (see step 6), including any CPU limits.
- JDK: nothing to install. Gradle downloads the JDK the build requires. `git`, `git-lfs` (optional), `python3`, and
  `curl` or network access to GitHub and Maven Central are needed.

If you have root, make the CPU frequency steady for the run, and record what you did in `notes.txt`:

```bash
# performance governor on every core
for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo performance | sudo tee "$g" > /dev/null; done
# disable turbo boost: Intel pstate, or the generic cpufreq boost switch
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo 2> /dev/null
echo 0 | sudo tee /sys/devices/system/cpu/cpufreq/boost 2> /dev/null
```

If you cannot, run anyway and say so in `notes.txt`; `run.sh` records the governor and turbo settings it finds.

## 2. Check out the three branches

If this machine already ran an earlier version of this kit in `~/agg-bench`, reuse that directory as described in
[Rerunning after an earlier run](#rerunning-after-an-earlier-run) instead, then continue with step 3.

Otherwise, work in a new directory. The repository stores documentation images in Git LFS; they are not needed, so skip
them.

```bash
mkdir -p ~/agg-bench && cd ~/agg-bench
export GIT_LFS_SKIP_SMUDGE=1
git clone --filter=blob:none https://github.com/cpwright/deephaven-core.git bench-agg-main
cd bench-agg-main
git fetch origin cpw/bench-agg-main-2026-09-30 cpw/bench-agg-pr-2026-09-29b cpw/bench-agg-pr-2026-09-30
git checkout -B bench-agg-main origin/cpw/bench-agg-main-2026-09-30
git worktree add --detach ../bench-agg-prev origin/cpw/bench-agg-pr-2026-09-29b
git worktree add --detach ../bench-agg-pr origin/cpw/bench-agg-pr-2026-09-30
cd ..
git -C bench-agg-main rev-parse --short=10 HEAD~1   # expect f93afcf3d4, upstream main
git -C bench-agg-prev rev-parse --short=10 HEAD~1   # expect a4c6fa12c8, the previously measured pull request
git -C bench-agg-pr rev-parse --short=10 HEAD~1     # expect 9d6e29c349, the pull request with main and #8698
```

Each branch is one commit, holding the benchmark and these scripts, on top of the commit being measured. The directory
now holds `bench-agg-main`, `bench-agg-prev`, and `bench-agg-pr` side by side. If any parent differs from the one
expected, stop and report it.

### Rerunning after an earlier run

Keep the earlier results, move the two existing checkouts to the new branches, and add the third:

```bash
cd ~/agg-bench
mv results results-2026-09-29b
mv run-tables.log results-2026-09-29b/ 2> /dev/null
export GIT_LFS_SKIP_SMUDGE=1
git -C bench-agg-main fetch origin cpw/bench-agg-main-2026-09-30 cpw/bench-agg-pr-2026-09-29b cpw/bench-agg-pr-2026-09-30
git -C bench-agg-main checkout -B bench-agg-main origin/cpw/bench-agg-main-2026-09-30
git -C bench-agg-pr checkout --detach origin/cpw/bench-agg-pr-2026-09-30
git -C bench-agg-main worktree add --detach ../bench-agg-prev origin/cpw/bench-agg-pr-2026-09-29b
git -C bench-agg-main status --short                # must print nothing
git -C bench-agg-prev status --short                # must print nothing
git -C bench-agg-pr status --short                  # must print nothing
git -C bench-agg-main rev-parse --short=10 HEAD~1   # expect f93afcf3d4, upstream main
git -C bench-agg-prev rev-parse --short=10 HEAD~1   # expect a4c6fa12c8, the previously measured pull request
git -C bench-agg-pr rev-parse --short=10 HEAD~1     # expect 9d6e29c349, the pull request with main and #8698
```

If `bench-agg-prev` already exists from another run, check it out with
`git -C bench-agg-prev checkout --detach origin/cpw/bench-agg-pr-2026-09-29b` instead of adding it.

`results` must not exist before the run: `run.sh` skips any configuration whose log is already complete, so a leftover
`results` directory would silently reuse the earlier numbers.

## 3. Build each checkout once

The first build downloads Gradle, the JDK, and the dependencies, which takes a while. Build all three before starting,
so that the benchmark run itself measures only the benchmarks:

```bash
(cd bench-agg-main && ./gradlew --no-daemon :engine-benchmark:compileJava)
(cd bench-agg-prev && ./gradlew --no-daemon :engine-benchmark:compileJava)
(cd bench-agg-pr && ./gradlew --no-daemon :engine-benchmark:compileJava)
```

All three must end in `BUILD SUCCESSFUL`. If not, stop and report the error.

## 4. Run

```bash
cd ~/agg-bench
nohup bash bench-agg-pr/engine/benchmark/scripts/agg-reclaim/run.sh results tables large > run-tables.log 2>&1 &
```

`run-tables.log` gets a line as each configuration starts and ends. The JMH output of each goes to `results/logs/`. If
the run is interrupted, run the same command again: configurations whose logs are complete are skipped.

## 5. Check the output

When `run-tables.log` ends with `done`:

```bash
cd ~/agg-bench
grep -L '^# Run complete' results/logs/*.log            # must print nothing
grep -l 'Exception\|OutOfMemoryError' results/logs/*.log # must print nothing
python3 bench-agg-pr/engine/benchmark/scripts/agg-reclaim/parse_logs.py results
```

`parse_logs.py` writes `results/results.csv` and should report 2400 rows: 1600 for the `tables` phase (ten
configurations, each with 10 benchmark and parameter combinations, two forks, and eight batches) and 800 for `large`
(five combinations). If a check fails, do not rerun anything; report it.

## 6. Write notes

Create `results/notes.txt` with anything a reader should know: whether the machine is bare metal, a VM, or a container;
whether the governor and turbo could be set; anything else that ran during the benchmarks; interruptions or reruns; and
the output of the checks in step 5 if any failed.

## Reporting the results

Package everything into one archive named for the host:

```bash
cd ~/agg-bench
tar czf agg-reclaim-results-$(hostname)-2026-09-30.tar.gz results run-tables.log
ls -l agg-reclaim-results-*-2026-09-30.tar.gz
```

Give that archive to the person who asked for the run; they will pass it back. It holds:

- `results/results.csv`: one row per batch, warmup batches included and marked. The columns are the phase, build and
  configuration from the log name; the benchmark; its parameters (`windowSize`, `rowsPerCycle`, `rowsPerKey`,
  `keysReturn`, `reclaim`, and `collapse`, which is empty for the new build); the fork, `warmup` or `measure`, and batch
  number; the batch time in milliseconds; and the benchmark's own metrics (`liveStates`, `positionsAssigned`, `lastLivePosition`, `rehashes`,
  `maxCycleMillis`, `retainedHeapMB`).
- `results/jmh/*.csv`: JMH's own summary of each run.
- `results/logs/*.log`: the full JMH output, from which the CSV was made.
- `results/machine.txt`: the CPU, memory, frequency settings, load, and the commit of each checkout, before and after.
- `results/notes.txt` and `run-tables.log`.

Also reply with a short summary: the contents of `results/machine.txt`, the row count `parse_logs.py` reported, and
anything in `notes.txt`.
