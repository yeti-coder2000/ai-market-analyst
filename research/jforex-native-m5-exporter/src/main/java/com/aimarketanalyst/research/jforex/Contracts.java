package com.aimarketanalyst.research.jforex;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Immutable source contracts shared by the network adapter and offline core. */
public final class Contracts {
    public static final String PROVIDER_PROFILE = "JFOREX_NATIVE_M5_BID";
    public static final String PERIOD = "FIVE_MINS";
    public static final String OFFER_SIDE = "BID";
    public static final String SCHEMA_VERSION = "jforex-native-m5-source-manifest-v1";
    public static final String EXPORTER_ID = "ai-market-analyst-jforex-native-m5-exporter-v1";
    public static final long M5_MILLIS = 5L * 60L * 1000L;

    public static final Map<String, String> SYMBOL_MAPPING;
    public static final Map<String, Instant> FIRST_NATIVE_M5_UTC;
    public static final List<String> CANONICAL_SYMBOLS;

    static {
        LinkedHashMap<String, String> symbols = new LinkedHashMap<>();
        symbols.put("XAUUSD", "XAU/USD");
        symbols.put("EURUSD", "EUR/USD");
        symbols.put("GBPUSD", "GBP/USD");
        symbols.put("USDJPY", "USD/JPY");
        symbols.put("USDCHF", "USD/CHF");
        symbols.put("USDCAD", "USD/CAD");
        symbols.put("AUDUSD", "AUD/USD");
        symbols.put("BTCUSD", "BTC/USD");
        symbols.put("ETHUSD", "ETH/USD");
        symbols.put("GER40", "DEU.IDX/EUR");
        symbols.put("NAS100", "USATECH.IDX/USD");
        symbols.put("SPX500", "USA500.IDX/USD");
        symbols.put("UKOIL", "BRENT.CMD/USD");
        SYMBOL_MAPPING = Map.copyOf(symbols);
        CANONICAL_SYMBOLS = List.copyOf(symbols.keySet());

        LinkedHashMap<String, Instant> starts = new LinkedHashMap<>();
        starts.put("XAUUSD", Instant.parse("2003-05-05T00:00:00Z"));
        starts.put("BTCUSD", Instant.parse("2017-05-07T23:55:00Z"));
        starts.put("ETHUSD", Instant.parse("2017-12-11T23:50:00Z"));
        starts.put("EURUSD", Instant.parse("2003-05-04T21:00:00Z"));
        starts.put("GBPUSD", Instant.parse("2003-05-04T21:00:00Z"));
        starts.put("USDJPY", Instant.parse("2003-05-04T21:00:00Z"));
        starts.put("USDCHF", Instant.parse("2003-05-04T21:00:00Z"));
        starts.put("USDCAD", Instant.parse("2003-08-03T21:00:00Z"));
        starts.put("AUDUSD", Instant.parse("2003-08-03T21:00:00Z"));
        starts.put("GER40", Instant.parse("2013-09-30T15:10:00Z"));
        starts.put("NAS100", Instant.parse("2011-09-19T13:30:00Z"));
        starts.put("SPX500", Instant.parse("2011-09-19T06:30:00Z"));
        starts.put("UKOIL", Instant.parse("2010-12-02T01:00:00Z"));
        FIRST_NATIVE_M5_UTC = Map.copyOf(starts);
    }

    private Contracts() {}

    public static void requireCanonicalSymbol(String symbol) {
        if (!SYMBOL_MAPPING.containsKey(symbol)) {
            throw new IllegalArgumentException("unsupported canonical symbol");
        }
    }

    public static void requireExactFullUniverse(List<String> symbols) {
        if (!CANONICAL_SYMBOLS.equals(symbols)) {
            throw new IllegalArgumentException(
                    "full universe requires exact canonical 13-symbol order");
        }
    }
}
