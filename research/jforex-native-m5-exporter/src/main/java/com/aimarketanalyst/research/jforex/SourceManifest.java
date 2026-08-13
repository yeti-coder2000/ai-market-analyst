package com.aimarketanalyst.research.jforex;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Deterministic JSON provenance for promoted source chunks. */
public final class SourceManifest {
    private static final Pattern STRING_FIELD =
            Pattern.compile("\\\"([a-z0-9_]+)\\\"\\s*:\\s*\\\"([^\\\"]*)\\\"");
    private static final Pattern INTEGER_FIELD =
            Pattern.compile("\\\"row_count\\\"\\s*:\\s*([0-9]+)");

    public record Chunk(
            String chunkId,
            String canonicalSymbol,
            String providerSymbol,
            String relativePath,
            Instant startUtc,
            Instant endExclusiveUtc,
            Instant firstBarUtc,
            Instant lastBarUtc,
            long rowCount,
            String sha256) {
        public Chunk {
            Contracts.requireCanonicalSymbol(canonicalSymbol);
            if (!Contracts.SYMBOL_MAPPING.get(canonicalSymbol).equals(providerSymbol)
                    || !chunkId.startsWith(canonicalSymbol + "/")
                    || !relativePath.startsWith("completed/")
                    || relativePath.contains("..")
                    || rowCount <= 0
                    || !sha256.matches("[0-9a-f]{64}")
                    || startUtc.compareTo(firstBarUtc) > 0
                    || lastBarUtc.compareTo(endExclusiveUtc) >= 0
                    || firstBarUtc.compareTo(lastBarUtc) > 0) {
                throw new IllegalArgumentException("invalid completed chunk provenance");
            }
        }
    }

    private SourceManifest() {}

    public static void write(
            Path path,
            String canonicalSymbol,
            Instant requestedStartUtc,
            Instant requestedEndExclusiveUtc,
            boolean complete,
            List<Chunk> chunks) throws IOException {
        Contracts.requireCanonicalSymbol(canonicalSymbol);
        List<Chunk> ordered = new ArrayList<>(chunks);
        ordered.sort(Comparator.comparing(Chunk::startUtc).thenComparing(Chunk::chunkId));
        StringBuilder json = new StringBuilder();
        json.append("{\n")
                .append(field("schema_version", Contracts.SCHEMA_VERSION, true))
                .append(field("exporter_id", Contracts.EXPORTER_ID, true))
                .append(field("provider_profile", Contracts.PROVIDER_PROFILE, true))
                .append(field("canonical_symbol", canonicalSymbol, true))
                .append(field("provider_symbol", Contracts.SYMBOL_MAPPING.get(canonicalSymbol), true))
                .append(field("period", Contracts.PERIOD, true))
                .append(field("offer_side", Contracts.OFFER_SIDE, true))
                .append(field("requested_start_utc", requestedStartUtc.toString(), true))
                .append(field("requested_end_exclusive_utc", requestedEndExclusiveUtc.toString(), true))
                .append("  \"complete\": ").append(complete).append(",\n")
                .append("  \"chunks\": [\n");
        for (int index = 0; index < ordered.size(); index++) {
            json.append(chunkJson(ordered.get(index), "    "));
            json.append(index + 1 == ordered.size() ? "\n" : ",\n");
        }
        json.append("  ]\n}\n");
        writeAtomically(path, json.toString());
    }

    public static void writeChunkSidecar(Path path, Chunk chunk) throws IOException {
        writeAtomically(path, chunkJson(chunk, "") + "\n");
    }

    public static Chunk readChunkSidecar(Path path) throws IOException {
        String json = Files.readString(path, StandardCharsets.UTF_8);
        if (!json.contains("\"status\": \"COMPLETED\"")
                || !json.contains("\"complete\": true")) {
            throw new IOException("incomplete chunk sidecar");
        }
        java.util.Map<String, String> strings = new java.util.HashMap<>();
        Matcher stringsMatcher = STRING_FIELD.matcher(json);
        while (stringsMatcher.find()) {
            strings.put(stringsMatcher.group(1), stringsMatcher.group(2));
        }
        Matcher integerMatcher = INTEGER_FIELD.matcher(json);
        if (!integerMatcher.find()) {
            throw new IOException("invalid completed chunk sidecar");
        }
        try {
            return new Chunk(
                    required(strings, "chunk_id"),
                    required(strings, "canonical_symbol"),
                    required(strings, "provider_symbol"),
                    required(strings, "relative_path"),
                    Instant.parse(required(strings, "start_utc")),
                    Instant.parse(required(strings, "end_exclusive_utc")),
                    Instant.parse(required(strings, "first_bar_utc")),
                    Instant.parse(required(strings, "last_bar_utc")),
                    Long.parseLong(integerMatcher.group(1)),
                    required(strings, "sha256"));
        } catch (RuntimeException exception) {
            throw new IOException("invalid completed chunk sidecar", exception);
        }
    }

    private static String required(java.util.Map<String, String> fields, String key)
            throws IOException {
        String value = fields.get(key);
        if (value == null) {
            throw new IOException("invalid completed chunk sidecar");
        }
        return value;
    }

    private static String chunkJson(Chunk chunk, String indent) {
        return indent + "{\n"
                + indent + field("chunk_id", chunk.chunkId(), true).stripLeading()
                + indent + field("canonical_symbol", chunk.canonicalSymbol(), true).stripLeading()
                + indent + field("provider_symbol", chunk.providerSymbol(), true).stripLeading()
                + indent + field("relative_path", chunk.relativePath(), true).stripLeading()
                + indent + field("start_utc", chunk.startUtc().toString(), true).stripLeading()
                + indent + field("end_exclusive_utc", chunk.endExclusiveUtc().toString(), true).stripLeading()
                + indent + field("first_bar_utc", chunk.firstBarUtc().toString(), true).stripLeading()
                + indent + field("last_bar_utc", chunk.lastBarUtc().toString(), true).stripLeading()
                + indent + "  \"row_count\": " + chunk.rowCount() + ",\n"
                + indent + field("sha256", chunk.sha256(), true).stripLeading()
                + indent + "  \"status\": \"COMPLETED\",\n"
                + indent + "  \"complete\": true\n"
                + indent + "}";
    }

    private static String field(String key, String value, boolean comma) {
        return "  \"" + key + "\": \"" + escape(value) + "\"" + (comma ? "," : "") + "\n";
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static void writeAtomically(Path path, String text) throws IOException {
        Files.createDirectories(path.getParent());
        Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
        Files.writeString(temporary, text, StandardCharsets.UTF_8);
        try {
            Files.move(
                    temporary,
                    path,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException exception) {
            Files.deleteIfExists(temporary);
            throw new IOException("atomic manifest promotion is required", exception);
        }
    }
}
