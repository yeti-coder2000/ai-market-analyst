package com.aimarketanalyst.research.jforex;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/** Exact values received from a native JForex {@code IBar}. */
public record NativeBar(
        Instant timestampUtc,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume) {

    public NativeBar {
        Objects.requireNonNull(timestampUtc, "timestampUtc");
        Objects.requireNonNull(open, "open");
        Objects.requireNonNull(high, "high");
        Objects.requireNonNull(low, "low");
        Objects.requireNonNull(close, "close");
        Objects.requireNonNull(volume, "volume");
        if (timestampUtc.toEpochMilli() % Contracts.M5_MILLIS != 0L) {
            throw new IllegalArgumentException("bar timestamp is not M5 aligned");
        }
        if (high.compareTo(open.max(close)) < 0
                || low.compareTo(open.min(close)) > 0
                || high.compareTo(low) < 0
                || volume.signum() < 0) {
            throw new IllegalArgumentException("invalid native OHLC envelope");
        }
    }

    public static NativeBar fromProvider(
            long timestampMillis,
            double open,
            double high,
            double low,
            double close,
            double volume) {
        return new NativeBar(
                Instant.ofEpochMilli(timestampMillis),
                BigDecimal.valueOf(open),
                BigDecimal.valueOf(high),
                BigDecimal.valueOf(low),
                BigDecimal.valueOf(close),
                BigDecimal.valueOf(volume));
    }

    public String csvRow() {
        return timestampUtc + ","
                + open.toPlainString() + ","
                + high.toPlainString() + ","
                + low.toPlainString() + ","
                + close.toPlainString() + ","
                + volume.toPlainString();
    }
}
