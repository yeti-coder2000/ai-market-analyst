package com.aimarketanalyst.research.jforex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ContractsTest {
    @TempDir Path root;

    @Test
    void exactProviderContractAndSymbolOrderAreStable() {
        assertEquals("JFOREX_NATIVE_M5_BID", Contracts.PROVIDER_PROFILE);
        assertEquals("FIVE_MINS", Contracts.PERIOD);
        assertEquals("BID", Contracts.OFFER_SIDE);
        assertEquals(
                List.of(
                        "XAUUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCHF",
                        "USDCAD", "AUDUSD", "BTCUSD", "ETHUSD", "GER40",
                        "NAS100", "SPX500", "UKOIL"),
                Contracts.CANONICAL_SYMBOLS);
        assertEquals("2017-12-11T23:50:00Z", Contracts.FIRST_NATIVE_M5_UTC.get("ETHUSD").toString());
    }

    @Test
    void writerPromotesDeterministicChunkAndVerifiedResumeReusesIt() throws Exception {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        Instant end = Instant.parse("2026-02-01T00:00:00Z");
        NativeBar bar = new NativeBar(
                start,
                new BigDecimal("1.10000"),
                new BigDecimal("1.20000"),
                new BigDecimal("1.00000"),
                new BigDecimal("1.15000"),
                new BigDecimal("2.5"));
        SourceManifest.Chunk first = ChunkWriter.write(root, "EURUSD", start, end, List.of(bar), false);
        SourceManifest.Chunk resumed = ChunkWriter.write(root, "EURUSD", start, end, List.of(bar), true);
        assertEquals(first, resumed);
        Path completed = root.resolve("EURUSD/completed/EURUSD_2026-01.csv");
        assertEquals(
                ChunkWriter.HEADER + "\n2026-01-01T00:00:00Z,1.10000,1.20000,1.00000,1.15000,2.5\n",
                Files.readString(completed, StandardCharsets.UTF_8));
        assertTrue(Files.isRegularFile(completed.resolveSibling("EURUSD_2026-01.csv.manifest.json")));
    }

    @Test
    void emptyAndInvalidBarsFailClosed() {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        Instant end = Instant.parse("2026-02-01T00:00:00Z");
        assertThrows(Exception.class, () -> ChunkWriter.write(root, "EURUSD", start, end, List.of(), false));
        assertThrows(
                IllegalArgumentException.class,
                () -> new NativeBar(
                        start,
                        BigDecimal.ONE,
                        new BigDecimal("0.9"),
                        BigDecimal.ONE,
                        BigDecimal.ONE,
                        BigDecimal.ONE));
    }
}
