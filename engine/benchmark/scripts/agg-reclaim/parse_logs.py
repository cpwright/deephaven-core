#!/usr/bin/env python3
"""
Convert the JMH logs written by run.sh into one CSV row per iteration.

Usage: parse_logs.py <results-dir>

Reads <results-dir>/logs/*.log and writes <results-dir>/results.csv. A log's name is
<size>__<build>__<config>__<part>.log, which supplies the first columns; the rest come from the log itself.
"""
import csv
import os
import re
import sys

PARAMS = ['windowSize', 'rowsPerCycle', 'rowsPerKey', 'keysReturn', 'reclaim', 'collapse', 'blockShift',
          'bulkShift', 'tableSize', 'keyCount', 'keyType']
METRICS = ['liveStates', 'positionsAssigned', 'lastLivePosition', 'rehashes', 'maxCycleMillis', 'retainedHeapMB']
COLUMNS = ['size', 'build', 'config', 'part', 'benchmark'] + PARAMS + ['fork', 'phase', 'iteration', 'time_ms'] \
    + METRICS


def parse(path):
    size, build, config, part = os.path.basename(path)[:-len('.log')].split('__')
    rows = []
    benchmark = None
    params = {}
    fork = None
    current = None
    with open(path, errors='replace') as f:
        for line in f:
            if line.startswith('# Run complete'):
                break
            m = re.match(r'# Benchmark: \S+\.(\w+)$', line.strip())
            if m:
                benchmark = m.group(1)
                continue
            m = re.match(r'# Parameters: \((.*)\)', line.strip())
            if m:
                params = dict(kv.split(' = ', 1) for kv in m.group(1).split(', '))
                continue
            m = re.match(r'# Fork: (\d+) of', line.strip())
            if m:
                fork = int(m.group(1))
                continue
            m = re.search(r'(# Warmup )?Iteration\s+(\d+):', line)
            if m:
                current = {'size': size, 'build': build, 'config': config, 'part': part, 'benchmark': benchmark,
                           'fork': fork, 'phase': 'warmup' if m.group(1) else 'measure',
                           'iteration': int(m.group(2))}
                for p in PARAMS:
                    current[p] = params.get(p, '')
            if current is None:
                continue
            m = re.search(r'result: (.*)', line)
            if m:
                for kv in m.group(1).split(', '):
                    k, _, v = kv.partition('=')
                    if k in METRICS:
                        current[k] = v.strip()
            m = re.search(r'([\d.]+) ms/op', line)
            if m and 'time_ms' not in current:
                current['time_ms'] = m.group(1)
                rows.append(current)
                current = None
    return rows


def main():
    results = sys.argv[1]
    logs = os.path.join(results, 'logs')
    rows = []
    for name in sorted(os.listdir(logs)):
        if name.endswith('.log') and name.count('__') == 3:
            rows.extend(parse(os.path.join(logs, name)))
    with open(os.path.join(results, 'results.csv'), 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, restval='')
        writer.writeheader()
        writer.writerows(rows)
    print(f'{len(rows)} rows written to {os.path.join(results, "results.csv")}')


if __name__ == '__main__':
    main()
