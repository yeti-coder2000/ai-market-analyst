package com.aimarketanalyst.research.jforex;

import com.dukascopy.api.Filter;
import com.dukascopy.api.IAccount;
import com.dukascopy.api.IBar;
import com.dukascopy.api.IContext;
import com.dukascopy.api.IHistory;
import com.dukascopy.api.IMessage;
import com.dukascopy.api.IStrategy;
import com.dukascopy.api.ITick;
import com.dukascopy.api.Instrument;
import com.dukascopy.api.instrument.IFinancialInstrument;
import com.dukascopy.api.JFException;
import com.dukascopy.api.OfferSide;
import com.dukascopy.api.Period;
import com.dukascopy.api.system.ClientFactory;
import com.dukascopy.api.system.IClient;
import com.dukascopy.api.system.ISystemListener;
import java.nio.file.Path;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Native JForex SDK boundary. Compiled only with Maven profile {@code native-sdk}. */
public final class JForexExporter {
    private static final String DEMO_JNLP =
            "http://platform.dukascopy.com/demo_3/jforex_3.jnlp";
    private static final String USER_ENV = "DUKASCOPY_DEMO_USER";
    private static final String PASSWORD_ENV = "DUKASCOPY_DEMO_PASSWORD";

    private JForexExporter() {}

    public static void main(String[] arguments) throws Exception {
        Options options = Options.parse(arguments);
        String username = requiredEnvironment(USER_ENV);
        String password = requiredEnvironment(PASSWORD_ENV);
        IClient client = ClientFactory.getDefaultInstance();
        client.setSystemListener(new ISystemListener() {
            @Override public void onStart(long processId) {}
            @Override public void onStop(long processId) {}
            @Override public void onConnect() {}
            @Override public void onDisconnect() {}
        });
        try {
            try {
                client.connect(DEMO_JNLP, username, password);
            } catch (Exception exception) {
                throw new IllegalStateException("JForex demo connection failed");
            }
            awaitConnection(client);
            CountDownLatch finished = new CountDownLatch(1);
            AtomicReference<String> failure = new AtomicReference<>();
            client.startStrategy(new ExportStrategy(options, finished, failure));
            if (!finished.await(7, TimeUnit.DAYS)) {
                throw new IllegalStateException("native export did not finish before process timeout");
            }
            if (failure.get() != null) {
                throw new IllegalStateException(failure.get());
            }
        } finally {
            if (client.isConnected()) {
                client.disconnect();
            }
        }
    }

    private static final class ExportStrategy implements IStrategy {
        private final Options options;
        private final CountDownLatch finished;
        private final AtomicReference<String> failure;

        private ExportStrategy(
                Options options,
                CountDownLatch finished,
                AtomicReference<String> failure) {
            this.options = options;
            this.finished = finished;
            this.failure = failure;
        }

        @Override
        public void onStart(IContext context) {
            try {
                Map<String, IFinancialInstrument> instruments =
                        resolveFinancialInstruments(context, options.symbols());

                context.setSubscribedFinancialInstruments(
                        new LinkedHashSet<>(instruments.values()),
                        true);

                requireFinancialSubscriptions(context, instruments);

                IHistory history = context.getHistory();
                Instant currentM5Open = Instant.ofEpochMilli(
                        Math.floorDiv(System.currentTimeMillis(), Contracts.M5_MILLIS)
                                * Contracts.M5_MILLIS);
                Instant effectiveEnd = options.endExclusiveUtc().isBefore(currentM5Open)
                        ? options.endExclusiveUtc()
                        : currentM5Open;
                for (String symbol : options.symbols()) {
                    Instant sourceStart = Contracts.FIRST_NATIVE_M5_UTC.get(symbol);
                    Instant start = options.startUtc() == null || sourceStart.isAfter(options.startUtc())
                            ? sourceStart
                            : options.startUtc();
                    if (!start.isBefore(effectiveEnd)) {
                        throw new IllegalArgumentException("invalid export range for symbol=" + symbol);
                    }
                    exportSymbol(
                            history,
                            options.cacheRoot(),
                            symbol,
                            instruments.get(symbol),
                            start,
                            effectiveEnd,
                            options.resume());
                }
            } catch (Exception exception) {
                failure.set(safeFailure(exception));
            } finally {
                finished.countDown();
                context.stop();
            }
        }

        @Override public void onTick(Instrument instrument, ITick tick) throws JFException {}
        @Override public void onBar(Instrument instrument, Period period, IBar askBar, IBar bidBar) throws JFException {}
        @Override public void onMessage(IMessage message) throws JFException {}
        @Override public void onAccount(IAccount account) throws JFException {}
        @Override
        public void onStop() throws JFException {
            String error = failure.get();
            if (error != null) {
                System.err.println("JFOREX_EXPORTER_FAILURE=" + error);
                System.exit(1);
            }
            System.exit(0);
        }

        private static String safeFailure(Exception exception) {
            String message = exception.getMessage();
            if (message != null
                    && (message.startsWith("native history request failed for symbol=")
                    || message.startsWith("empty native history partition for symbol=")
                    || message.startsWith("invalid export range for symbol="))) {
                return message;
            }
            return "native export failed closed; retry the process/session after inspecting local logs";
        }
    }

    private static void exportSymbol(
            IHistory history,
            Path cacheRoot,
            String canonicalSymbol,
            IFinancialInstrument instrument,
            Instant start,
            Instant endExclusive,
            boolean resume) throws Exception {
        List<SourceManifest.Chunk> completed = new ArrayList<>();
        Instant cursor = start;
        Path manifest = cacheRoot.resolve(canonicalSymbol).resolve("source_manifest.json");
        while (cursor.isBefore(endExclusive)) {
            Instant partitionEnd = monthAfter(cursor).isBefore(endExclusive)
                    ? monthAfter(cursor)
                    : endExclusive;
            List<IBar> providerBars;
            try {
                providerBars = history.getBars(
                        instrument,
                        Period.FIVE_MINS,
                        OfferSide.BID,
                        Filter.NO_FILTER,
                        cursor.toEpochMilli(),
                        partitionEnd.minusMillis(Contracts.M5_MILLIS).toEpochMilli());
            } catch (Exception exception) {
                throw new IllegalStateException(
                        "native history request failed for symbol=" + canonicalSymbol
                                + "; retry in a new JForex process/session");
            }
            final long partitionStartEpochMillis = cursor.toEpochMilli();
            List<NativeBar> bars = providerBars.stream()
                    .filter(bar -> bar.getTime() >= partitionStartEpochMillis)
                    .filter(bar -> bar.getTime() < partitionEnd.toEpochMilli())
                    .map(JForexExporter::nativeBar)
                    .sorted(java.util.Comparator.comparing(NativeBar::timestampUtc))
                    .toList();
            if (bars.isEmpty()) {
                throw new IllegalStateException(
                        "empty native history partition for symbol=" + canonicalSymbol
                                + "; retry in a new JForex process/session");
            }
            completed.add(ChunkWriter.write(
                    cacheRoot,
                    canonicalSymbol,
                    cursor,
                    partitionEnd,
                    bars,
                    resume));
            SourceManifest.write(manifest, canonicalSymbol, start, endExclusive, false, completed);
            cursor = partitionEnd;
        }
        SourceManifest.write(manifest, canonicalSymbol, start, endExclusive, true, completed);
    }

    private static NativeBar nativeBar(IBar bar) {
        return NativeBar.fromProvider(
                bar.getTime(),
                bar.getOpen(),
                bar.getHigh(),
                bar.getLow(),
                bar.getClose(),
                bar.getVolume());
    }

    private static Map<String, IFinancialInstrument> resolveFinancialInstruments(
            IContext context,
            List<String> symbols) {
        Map<String, IFinancialInstrument> instruments = new LinkedHashMap<>();
        var provider = context.getFinancialInstrumentProvider();

        for (String symbol : symbols) {
            Contracts.requireCanonicalSymbol(symbol);
            String providerSymbol = Contracts.SYMBOL_MAPPING.get(symbol);

            IFinancialInstrument instrument =
                    provider.getFinancialInstrument(providerSymbol);

            if (instrument == null) {
                throw new IllegalArgumentException(
                        "JForex financial instrument mapping did not resolve for symbol="
                                + symbol);
            }

            instruments.put(symbol, instrument);
        }

        return instruments;
    }

    private static void requireFinancialSubscriptions(
            IContext context,
            Map<String, IFinancialInstrument> instruments) {
        List<String> subscribedNames =
                context.getSubscribedFinancialInstruments().stream()
                        .map(IFinancialInstrument::getName)
                        .sorted()
                        .toList();

        for (Map.Entry<String, IFinancialInstrument> entry : instruments.entrySet()) {
            String requestedName = entry.getValue().getName();
            if (!subscribedNames.contains(requestedName)) {
                throw new IllegalStateException(
                        "JForex financial instrument subscription was not established for symbol="
                                + entry.getKey());
            }
        }

        System.out.println(
                "JFOREX_FINANCIAL_SUBSCRIPTIONS=" + subscribedNames);
    }

    private static Instant monthAfter(Instant value) {
        YearMonth month = YearMonth.from(value.atZone(ZoneOffset.UTC)).plusMonths(1);
        return month.atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC);
    }

    private static void awaitConnection(IClient client) throws InterruptedException {
        for (int attempts = 0; attempts < 60 && !client.isConnected(); attempts++) {
            TimeUnit.SECONDS.sleep(1);
        }
        if (!client.isConnected()) {
            throw new IllegalStateException("JForex demo connection was not established");
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("required Dukascopy demo credential environment variable is missing");
        }
        return value;
    }

    private record Options(
            Path cacheRoot,
            List<String> symbols,
            Instant startUtc,
            Instant endExclusiveUtc,
            boolean resume) {

        private static Options parse(String[] arguments) {
            Map<String, String> values = new LinkedHashMap<>();
            boolean resume = false;
            for (int index = 0; index < arguments.length; index++) {
                String argument = arguments[index];
                if ("--resume".equals(argument)) {
                    resume = true;
                    continue;
                }
                if (!argument.startsWith("--") || index + 1 >= arguments.length) {
                    throw new IllegalArgumentException("invalid exporter arguments");
                }
                values.put(argument, arguments[++index]);
            }
            Path cacheRoot = Path.of(required(values, "--cache-root")).toAbsolutePath().normalize();
            List<String> symbols = Arrays.stream(required(values, "--symbols").split(","))
                    .map(String::trim)
                    .filter(value -> !value.isEmpty())
                    .toList();
            if (symbols.isEmpty() || new LinkedHashSet<>(symbols).size() != symbols.size()) {
                throw new IllegalArgumentException("symbols must be unique");
            }
            for (String symbol : symbols) {
                Contracts.requireCanonicalSymbol(symbol);
            }
            Instant start = values.containsKey("--start-utc")
                    ? aligned(Instant.parse(values.get("--start-utc")))
                    : null;
            Instant end = aligned(Instant.parse(required(values, "--end-utc")));
            return new Options(cacheRoot, symbols, start, end, resume);
        }

        private static String required(Map<String, String> values, String key) {
            String value = values.get(key);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("missing required exporter argument " + key);
            }
            return value;
        }

        private static Instant aligned(Instant value) {
            if (value.toEpochMilli() % Contracts.M5_MILLIS != 0L) {
                throw new IllegalArgumentException("export timestamps must be M5 aligned");
            }
            return value;
        }
    }
}
