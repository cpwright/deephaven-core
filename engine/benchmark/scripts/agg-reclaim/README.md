# Aggregation state reclaiming benchmarks: the baseline versus the pull request

These instructions are for an agent running on a quiet x86-64 Linux machine. They measure the incremental aggregation
benchmark on a baseline and on deephaven-core pull request #8631 in each of its modes, so that the design document's
and the pull request's performance tables can be regenerated from one consistent run.

- **Baseline** (`main` in the logs): upstream main (f93afcf3d4) with the fresh-block change of draft #8713, which
  records no previous values for blocks that an array source allocates during an update cycle.
- **Pull request** (`pr`): the head of #8631 (82d1ff9aa1), which merges that same upstream main and contains the same
  fresh-block change.

The change is in both builds, since it speeds up aggregations whether or not they reclaim states. Leaving it out of the
baseline would credit its gain to the pull request. Follow the steps in
order and report back as described in [Reporting the results](#reporting-the-results).

## What is measured

`AggregationIncrementalBenchmark` runs batches of 900 update cycles of a keyed `sumBy` over a refreshing table of
1,000,000 rows, with 10,000 rows added and removed each cycle, and reports the time per batch, the heap retained
afterward, the output positions assigned, and the longest cycle. It runs three workloads (add only, a sliding window, and
random churn), with groups of 1 and of 100 rows, and with and without groups returning after they empty.

Six configurations run, one after another:

| Log label | Branch | Mode |
| --- | --- | --- |
| `main__none` | baseline | no reclaiming (the baseline has no other mode) |
| `pr__none` | pull request | `StateReclaimMode.none()` |
| `pr__blocks` | pull request | `releaseBlocks(1)`: release blocks, move nothing |
| `pr__collapse075` | pull request | `releaseBlocks(0.75)` |
| `pr__collapse05` | pull request | `releaseBlocks(0.5)` |
| `mainagain__none` | baseline | no reclaiming, again, to show any drift over the run |

Each configuration uses two JVM forks, with three warmup and five measured batches per fork. The `tables` phase takes
about half an hour after the first build. The `large` phase repeats the matrix with 10,000,000 rows and 100,000 per
cycle, one row per group, and needs about 64 GB of RAM and about four more hours. Run both.

The measured benchmark code is the same in both checkouts. Only the way a mode is named differs: the baseline takes the
collapse fraction as a `collapse` parameter, and the pull request names it in `reclaim` (`collapse0.75`,
`collapse0.5`). `run.sh` passes each build its own form.

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

## 2. Check out the two branches

If this machine already ran an earlier version of this kit in `~/agg-bench`, reuse that directory as described in
[Rerunning after an earlier run](#rerunning-after-an-earlier-run) instead, then continue with step 3.

Otherwise, work in a new directory. The repository stores documentation images in Git LFS; they are not needed, so skip them.

```bash
mkdir -p ~/agg-bench && cd ~/agg-bench
export GIT_LFS_SKIP_SMUDGE=1
git clone --filter=blob:none https://github.com/cpwright/deephaven-core.git bench-agg-main
cd bench-agg-main
git fetch origin cpw/bench-agg-main-2026-09-30b cpw/bench-agg-pr-2026-09-30b
git checkout -B bench-agg-main origin/cpw/bench-agg-main-2026-09-30b
git worktree add ../bench-agg-pr origin/cpw/bench-agg-pr-2026-09-30b
cd ..
git -C bench-agg-main rev-parse --short=10 HEAD~1   # expect cf035e319f, upstream main with the fresh-block change
git -C bench-agg-pr rev-parse --short=10 HEAD~1     # expect 82d1ff9aa1, the pull request
```

Each branch is one commit, holding the benchmark and these scripts, on top of the commit being measured. The pull
request already contains that upstream main and the fresh-block change, so the two differ only by the pull request's
other changes. Neither contains the adaptive recycler (#8698). The directory now holds `bench-agg-main` and
`bench-agg-pr` side by side. If either parent differs from the one expected, stop and report it.

### Rerunning after an earlier run

Keep the earlier results, and move both checkouts to the new branches:

```bash
cd ~/agg-bench
mv results results-2026-09-29b
mv run-tables.log results-2026-09-29b/ 2> /dev/null
export GIT_LFS_SKIP_SMUDGE=1
git -C bench-agg-main fetch origin cpw/bench-agg-main-2026-09-30b cpw/bench-agg-pr-2026-09-30b
git -C bench-agg-main checkout -B bench-agg-main origin/cpw/bench-agg-main-2026-09-30b
git -C bench-agg-pr checkout --detach origin/cpw/bench-agg-pr-2026-09-30b
git -C bench-agg-main status --short                # must print nothing
git -C bench-agg-pr status --short                  # must print nothing
git -C bench-agg-main rev-parse --short=10 HEAD~1   # expect cf035e319f, upstream main with the fresh-block change
git -C bench-agg-pr rev-parse --short=10 HEAD~1     # expect 82d1ff9aa1, the pull request
```

`results` must not exist before the run: `run.sh` skips any configuration whose log is already complete, so a leftover
`results` directory would silently reuse the earlier numbers.

## 3. Build each checkout once

The first build downloads Gradle, the JDK, and the dependencies, which takes a while. Build both before starting, so that
the benchmark run itself measures only the benchmarks:

```bash
(cd bench-agg-main && ./gradlew --no-daemon :engine-benchmark:compileJava)
(cd bench-agg-pr && ./gradlew --no-daemon :engine-benchmark:compileJava)
```

Both must end in `BUILD SUCCESSFUL`. If not, stop and report the error.

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

`parse_logs.py` writes `results/results.csv` and should report 1440 rows: 960 for the `tables` phase (six
configurations, each with 10 benchmark and parameter combinations, two forks, and eight batches) and 480 for `large`
(five combinations). If a check fails, do not rerun anything;
report it.

## 6. Write notes

Create `results/notes.txt` with anything a reader should know: whether the machine is bare metal, a VM, or a container;
whether the governor and turbo could be set; anything else that ran during the benchmarks; interruptions or reruns; and
the output of the checks in step 5 if any failed.

## Reporting the results

Package everything into one archive named for the host:

```bash
cd ~/agg-bench
tar czf agg-reclaim-results-$(hostname)-2026-09-30b.tar.gz results run-tables.log
ls -l agg-reclaim-results-*-2026-09-30b.tar.gz
```

Give that archive to the person who asked for the run; they will pass it back. It holds:

- `results/results.csv`: one row per batch, warmup batches included and marked. The columns are the phase, build and
  configuration from the log name; the benchmark; its parameters (`windowSize`, `rowsPerCycle`, `rowsPerKey`,
  `keysReturn`, `reclaim`, and `collapse`, which is empty for the pull request); the fork, `warmup` or `measure`, and
  batch number; the batch time in milliseconds; and the benchmark's own metrics (`liveStates`, `positionsAssigned`, `lastLivePosition`, `rehashes`,
  `maxCycleMillis`, `retainedHeapMB`).
- `results/jmh/*.csv`: JMH's own summary of each run.
- `results/logs/*.log`: the full JMH output, from which the CSV was made.
- `results/machine.txt`: the CPU, memory, frequency settings, load, and the commit of each checkout, before and after.
- `results/notes.txt` and `run-tables.log`.

Also reply with a short summary: the contents of `results/machine.txt`, the row count `parse_logs.py` reported, and
anything in `notes.txt`.
