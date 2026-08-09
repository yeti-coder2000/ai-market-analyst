# Dukascopy-format fixtures

These UTF-8 `.b64` files contain Base64-encoded, synthetic, redistributable golden payloads in the real-format big-endian `>3I2f` tick layout and LZMA-alone container. Tests decode them with strict Base64 validation and verify the decoded byte counts and SHA-256 values declared in `expected.json` before writing temporary `.bi5` files for the production decoder.

They are not downloaded market data and do not prove native Dukascopy SWFX/CFD bid M5 parity or provider symbol mappings. The provider profile remains `TICK_AGGREGATED_M5_NON_PARITY`.
