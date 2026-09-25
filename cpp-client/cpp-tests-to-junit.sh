#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 out.xml out.log" 1>&2
    exit 1
fi

if [ -z "${DH_PREFIX}" ]; then
    echo "$0: Environment variable DH_PREFIX is not set, aborting." 1>&2
    exit 1
fi

# The whole suite normally takes about a minute. Abort a hung run well before the enclosing docker
# task gives up, so it leaves a core dump and a stack trace instead of being killed silently.
TEST_TIMEOUT_SECONDS=${DH_TEST_TIMEOUT_SECONDS:-180}
# Stress settings: run the suite this many times, stopping at the first failure, with this many
# busy-looping processes competing for the CPU.
TEST_ITERATIONS=${DH_TEST_ITERATIONS:-1}
CPU_HOGS=${DH_TEST_CPU_HOGS:-0}
TEST_BINARY="${DH_PREFIX}/bin/dhclient_tests"
OUT_DIR=$(dirname "$1")

ulimit -c unlimited || true
cat /proc/sys/kernel/core_pattern || true
echo "$0: nproc=$(nproc) iterations=${TEST_ITERATIONS} cpu_hogs=${CPU_HOGS}" | tee "$2"

hog_pids=()
for ((hh = 0; hh < CPU_HOGS; ++hh)); do
    (set +o xtrace; while :; do :; done) &
    hog_pids+=($!)
done

status=0
for ((ii = 1; ii <= TEST_ITERATIONS; ++ii)); do
    echo "$0: iteration ${ii} of ${TEST_ITERATIONS} starting at $(date -u +%FT%TZ)" | tee -a "$2"
    xml="${OUT_DIR}/cpp-test-${ii}.xml"
    set +o errexit
    timeout --signal=ABRT --kill-after=30 "${TEST_TIMEOUT_SECONDS}" \
        "${TEST_BINARY}" --reporter XML --durations yes --out "${xml}" 2>&1 | tee -a "$2"
    status=${PIPESTATUS[0]}
    set -o errexit
    cp "${xml}" "$1" || true

    if [ "${status}" -eq 124 ]; then
        reason="timed out after ${TEST_TIMEOUT_SECONDS} seconds and was sent SIGABRT"
    elif [ "${status}" -gt 128 ]; then
        reason="was killed by signal SIG$(kill -l $((status - 128)))"
    else
        reason="exited with status ${status}"
    fi
    echo "$0: iteration ${ii}: dhclient_tests ${reason}" | tee -a "$2"
    if [ "${status}" -ne 0 ]; then
        break
    fi
    rm -f "${xml}"
done

if [ "${#hog_pids[@]}" -gt 0 ]; then
    kill "${hog_pids[@]}" || true
fi

shopt -s nullglob
for core in "${OUT_DIR}"/core* /core* ./core*; do
    echo "$0: found core file ${core}" | tee -a "$2"
    if command -v gdb > /dev/null; then
        gdb --batch --quiet -ex 'info threads' -ex 'thread apply all bt' \
            "${TEST_BINARY}" "${core}" 2>&1 | tee -a "$2" "${OUT_DIR}/cpp-test-backtrace.txt" || true
    fi
done

exit "${status}"
