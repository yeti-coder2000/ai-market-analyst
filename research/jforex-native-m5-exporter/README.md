# JForex native M5 BID exporter

Research-only Java 17 exporter for the `JFOREX_NATIVE_M5_BID` provider
profile. It requests Dukascopy bars directly with `Period.FIVE_MINS`,
`OfferSide.BID`, and `Filter.NO_FILTER`. It does not decode BI5 ticks,
aggregate ticks, infer price scales, or repair source data.

The default Maven build compiles and tests only the network-free exporter
core:

```bash
mvn test
```

The native SDK adapter is enabled explicitly:

```bash
mvn -Pnative-sdk package
```

The native profile resolves
`com.dukascopy.dds2:DDS2-jClient-JForex:3.6.51` from Dukascopy's public
repository. If that repository is unavailable, build the profile on the local
Mac with normal access to the Dukascopy repository.

Credentials are read only from these process environment variables:

- `DUKASCOPY_DEMO_USER`
- `DUKASCOPY_DEMO_PASSWORD`

They are never written to chunks, manifests, command output, or error text.

Example export (the end timestamp is an exclusive, fully closed M5 boundary):

```bash
export DUKASCOPY_DEMO_USER='...'
export DUKASCOPY_DEMO_PASSWORD='...'
mvn -Pnative-sdk exec:java \
  -Dexec.mainClass=com.aimarketanalyst.research.jforex.JForexExporter \
  -Dexec.args='--cache-root /absolute/path/to/jforex-cache --symbols XAUUSD,EURUSD,GBPUSD,USDJPY,USDCHF,USDCAD,AUDUSD,BTCUSD,ETHUSD,GER40,NAS100,SPX500,UKOIL --end-utc 2026-08-10T00:00:00Z --resume'
```

Each symbol is exported into deterministic monthly UTF-8 CSV chunks. Files
are first written below `.partial/`, validated, hashed, and atomically promoted
to `completed/`. A complete `source_manifest.json` is published only after all
requested chunks have succeeded. An empty or failed historical partition
terminates the process so a new JForex JVM/session can retry it; empty results
are never accepted as valid history.
