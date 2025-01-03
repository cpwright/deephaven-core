//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.stats;

import java.text.DecimalFormat;

public abstract class Value {

    protected long n = 0;
    protected long last = 0;
    protected long sum = 0;
    protected long sum2 = 0;
    protected long max = Long.MIN_VALUE;
    protected long min = Long.MAX_VALUE;

    private boolean alwaysUpdated = false;

    // NOTE: If you are looking in here to determine what the output data columns mean:
    // n,sum,last,min,max,ave,sum2,stdev

    public long getN() {
        return n;
    }

    public long getLast() {
        return last;
    }

    public long getSum() {
        return sum;
    }

    public long getSum2() {
        return sum2;
    }

    public long getMax() {
        return max;
    }

    public long getMin() {
        return min;
    }

    protected final History history;

    public Value(long now) {
        history = new History(now);
    }

    protected Value(History history) {
        this.history = history;
    }

    public void sample(long x) {
        n++;
        last = x;
        sum += x;
        sum2 += x * x;
        if (x > max) {
            max = x;
        }
        if (x < min) {
            min = x;
        }
    }

    public void increment(long x) {
        sample(x);
    }

    public abstract char getTypeTag();

    public History getHistory() {
        return history;
    }

    public Value alwaysUpdated(boolean b) {
        alwaysUpdated = b;
        return this;
    }

    public void reset() {
        n = sum = sum2 = 0;
        max = Long.MIN_VALUE;
        min = Long.MAX_VALUE;
    }

    public void update(Item item, ItemUpdateListener listener, long logInterval, long now, long appNow) {
        int topInterval = history.update(this, now);
        reset();
        if (History.INTERVALS[topInterval] >= logInterval) {
            for (int i = 0; i <= topInterval; ++i) {
                if (History.INTERVALS[i] >= logInterval && history.getN(i, 1) > 0 || alwaysUpdated) {
                    if (listener != null) {
                        listener.handleItemUpdated(item, now, appNow, i, History.INTERVALS[i],
                                History.INTERVAL_NAMES[i]);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        final DecimalFormat format = new DecimalFormat("#,###");
        final DecimalFormat avgFormat = new DecimalFormat("#,###.###");

        final double variance = n > 1 ? (sum2 - ((double)sum * sum / (double)n)) / (n - 1) : Double.NaN;

        return "Value{" +
                "n=" + format.format(n) +
                (n > 0 ?
                ", sum=" + format.format(sum) +
                ", sum2=" + format.format(sum2) +
                ", max=" + format.format(max) +
                ", min=" + format.format(min) +
                ", avg=" + avgFormat.format((n > 0 ? (double)sum / n : Double.NaN)) +
                ", std=" + avgFormat.format(Math.sqrt(variance))
                : "") +
                '}';
    }
}
