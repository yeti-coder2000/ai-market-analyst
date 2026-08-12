package com.aimarketanalyst.research.jforex;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;

/** Validate, hash, and atomically promote one deterministic source chunk. */
public final class ChunkWriter {
    public static final String HEADER = "timestamp_utc,open,high,low,close,volume";

    private ChunkWriter() {}

    public static SourceManifest.Chunk write(
            Path cacheRoot,
            String canonicalSymbol,
            Instant startUtc,
            Instant endExclusiveUtc,
            List<NativeBar> bars,
            boolean resume) throws IOException {
        Contracts.requireCanonicalSymbol(canonicalSymbol);
        if (bars.isEmpty()) {
            throw new IOException(
                    "empty native history partition; retry in a new JForex process/session");
        }
        String month = startUtc.toString().substring(0, 7);
        String chunkId = canonicalSymbol + "/" + month;
        Path symbolRoot = cacheRoot.resolve(canonicalSymbol);
        Path completed = symbolRoot.resolve("completed").resolve(canonicalSymbol + "_" + month + ".csv");
        Path sidecar = completed.resolveSibling(completed.getFileName() + ".manifest.json");
        if (resume && Files.isRegularFile(completed) && Files.isRegularFile(sidecar)) {
            SourceManifest.Chunk existing = SourceManifest.readChunkSidecar(sidecar);
            validateExisting(completed, existing, chunkId, canonicalSymbol, startUtc, endExclusiveUtc);
            return existing;
        }
        if (Files.exists(completed) || Files.exists(sidecar)) {
            throw new IOException("unverified completed namespace artifact");
        }
        Path partialRoot = symbolRoot.resolve(".partial");
        Files.createDirectories(partialRoot);
        Path partial = partialRoot.resolve(canonicalSymbol + "_" + month + ".csv.part");
        Files.deleteIfExists(partial);
        Instant previous = null;
        try (BufferedWriter writer = Files.newBufferedWriter(
                partial, StandardCharsets.UTF_8)) {
            writer.write(HEADER);
            writer.write('\n');
            for (NativeBar bar : bars) {
                Instant timestamp = bar.timestampUtc();
                if (timestamp.isBefore(startUtc) || !timestamp.isBefore(endExclusiveUtc)) {
                    throw new IOException("native bar outside chunk range");
                }
                if (previous != null && !timestamp.isAfter(previous)) {
                    throw new IOException("duplicate or non-monotonic native bar");
                }
                previous = timestamp;
                writer.write(bar.csvRow());
                writer.write('\n');
            }
        }
        Files.createDirectories(completed.getParent());
        try {
            Files.move(partial, completed, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException exception) {
            throw new IOException("atomic chunk promotion is required", exception);
        }
        SourceManifest.Chunk chunk = new SourceManifest.Chunk(
                chunkId,
                canonicalSymbol,
                Contracts.SYMBOL_MAPPING.get(canonicalSymbol),
                "completed/" + completed.getFileName(),
                startUtc,
                endExclusiveUtc,
                bars.get(0).timestampUtc(),
                bars.get(bars.size() - 1).timestampUtc(),
                bars.size(),
                sha256(completed));
        SourceManifest.writeChunkSidecar(sidecar, chunk);
        return chunk;
    }

    private static void validateExisting(
            Path completed,
            SourceManifest.Chunk chunk,
            String chunkId,
            String canonicalSymbol,
            Instant startUtc,
            Instant endExclusiveUtc) throws IOException {
        if (!chunk.chunkId().equals(chunkId)
                || !chunk.canonicalSymbol().equals(canonicalSymbol)
                || !chunk.startUtc().equals(startUtc)
                || !chunk.endExclusiveUtc().equals(endExclusiveUtc)
                || !chunk.sha256().equals(sha256(completed))) {
            throw new IOException("completed chunk failed resume validation");
        }
        long rows = 0;
        Instant first = null;
        Instant previous = null;
        try (var reader = Files.newBufferedReader(completed, StandardCharsets.UTF_8)) {
            if (!HEADER.equals(reader.readLine())) {
                throw new IOException("completed chunk header mismatch");
            }
            String line;
            while ((line = reader.readLine()) != null) {
                NativeBar bar = parseCompletedRow(line);
                if (bar.timestampUtc().isBefore(startUtc)
                        || !bar.timestampUtc().isBefore(endExclusiveUtc)
                        || (previous != null && !bar.timestampUtc().isAfter(previous))) {
                    throw new IOException("completed chunk timestamp contract mismatch");
                }
                if (first == null) {
                    first = bar.timestampUtc();
                }
                previous = bar.timestampUtc();
                rows++;
            }
        }
        if (rows != chunk.rowCount()
                || first == null
                || !first.equals(chunk.firstBarUtc())
                || previous == null
                || !previous.equals(chunk.lastBarUtc())) {
            throw new IOException("completed chunk row count or range mismatch");
        }
    }

    private static NativeBar parseCompletedRow(String line) throws IOException {
        String[] values = line.split(",", -1);
        if (values.length != 6) {
            throw new IOException("completed chunk row schema mismatch");
        }
        try {
            return new NativeBar(
                    Instant.parse(values[0]),
                    new BigDecimal(values[1]),
                    new BigDecimal(values[2]),
                    new BigDecimal(values[3]),
                    new BigDecimal(values[4]),
                    new BigDecimal(values[5]));
        } catch (RuntimeException exception) {
            throw new IOException("completed chunk row validation failed", exception);
        }
    }

    public static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(path)) {
                byte[] buffer = new byte[1024 * 1024];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required", exception);
        }
    }
}
