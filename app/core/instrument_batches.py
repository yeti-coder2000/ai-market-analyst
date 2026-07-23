from __future__ import annotations

"""
Instrument batch configuration.

Цей модуль визначає:
- групи інструментів: core / fx_major / indices
- canonical symbols для runner
- offset + interval для Render worker scheduling
- aliases для людських назв: DAX, NDQ, SNP500, USOIL тощо
"""

from typing import TypedDict


# ======================================================================================
# TYPES
# ======================================================================================


class BatchConfig(TypedDict):
    symbols: list[str]
    offset_minutes: int
    interval_minutes: int


# ======================================================================================
# CANONICAL SYMBOLS
# ======================================================================================

# ВАЖЛИВО:
# Тут мають бути саме ті значення, які підтримує app.core.enums.Instrument:
#
# DAX / DE40       -> GER40
# NDQ / NDX        -> NAS100
# SNP500 / SPX     -> SPX500
# UKOIL / BRENT    -> UKOIL
# USOIL / WTI      -> поки alias на UKOIL, бо зараз додаємо саме Brent/UKOIL


SYMBOL_ALIASES: dict[str, str] = {
    # gold / fx / crypto
    "GOLD": "XAUUSD",
    "XAU": "XAUUSD",
    "XAUUSD": "XAUUSD",
    "BTC": "BTCUSD",
    "BTCUSD": "BTCUSD",
    "ETH": "ETHUSD",
    "ETHUSD": "ETHUSD",
    "EUR": "EURUSD",
    "EURUSD": "EURUSD",
    "GBP": "GBPUSD",
    "GBPUSD": "GBPUSD",

    # fx_major
    "USDJPY": "USDJPY",
    "USD/JPY": "USDJPY",
    "USDCHF": "USDCHF",
    "USD/CHF": "USDCHF",
    "USDCAD": "USDCAD",
    "USD/CAD": "USDCAD",
    "AUDUSD": "AUDUSD",
    "AUD/USD": "AUDUSD",

    # indices / oil
    "DAX": "GER40",
    "GER40": "GER40",
    "DE40": "GER40",
    "GDAXI": "GER40",
    "^GDAXI": "GER40",

    "NDQ": "NAS100",
    "NDX": "NAS100",
    "^NDX": "NAS100",
    "NASDAQ": "NAS100",
    "NASDAQ100": "NAS100",
    "NAS100": "NAS100",

    "SNP500": "SPX500",
    "SP500": "SPX500",
    "S&P500": "SPX500",
    "S&P 500": "SPX500",
    "SPX": "SPX500",
    "^GSPC": "SPX500",
    "GSPC": "SPX500",
    "SPX500": "SPX500",

    "UKOIL": "UKOIL",
    "BRENT": "UKOIL",
    "BZ=F": "UKOIL",

    # Залишаємо alias, але свідомо мапимо USOIL/WTI не в окремий інструмент.
    # Якщо потім захочеш WTI, додамо окремий Instrument.USOIL у enums/settings/provider.
    "USOIL": "UKOIL",
    "WTI": "UKOIL",
}


# ======================================================================================
# CONFIG
# ======================================================================================

INSTRUMENT_BATCHES: dict[str, BatchConfig] = {
    "core": {
        "symbols": [
            "XAUUSD",
            "BTCUSD",
            "ETHUSD",
            "EURUSD",
            "GBPUSD",
        ],
        "offset_minutes": 0,
        "interval_minutes": 15,
    },

    "fx_major": {
        "symbols": [
            "USDJPY",
            "USDCHF",
            "USDCAD",
            "AUDUSD",
        ],
        "offset_minutes": 5,
        "interval_minutes": 15,
    },

    # London Focus v1 keeps the US-index/oil implementation available but
    # removes those symbols from the active production batch. Re-activation is
    # a configuration change, not a code recovery exercise.
    "indices": {
        "symbols": [
            "GER40",
        ],
        "offset_minutes": 10,
        "interval_minutes": 15,
    },
}


# ======================================================================================
# HELPERS
# ======================================================================================


def normalize_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()

    if not normalized:
        raise ValueError("Empty symbol is not supported.")

    return SYMBOL_ALIASES.get(normalized, normalized)


def normalize_batch_group(batch_group: str | None) -> str:
    normalized = str(batch_group or "core").strip().lower()

    if not normalized:
        return "core"

    return normalized


def get_batch_symbols(batch_group: str = "core") -> list[str]:
    group = normalize_batch_group(batch_group)

    if group not in INSTRUMENT_BATCHES:
        raise ValueError(
            f"Unknown batch_group: {batch_group!r}. "
            f"Available batches: {', '.join(list_available_batches())}"
        )

    return [
        normalize_symbol(symbol)
        for symbol in INSTRUMENT_BATCHES[group]["symbols"]
    ]


def get_batch_config(batch_group: str = "core") -> BatchConfig:
    group = normalize_batch_group(batch_group)

    if group not in INSTRUMENT_BATCHES:
        raise ValueError(
            f"Unknown batch_group: {batch_group!r}. "
            f"Available batches: {', '.join(list_available_batches())}"
        )

    config = INSTRUMENT_BATCHES[group]

    return {
        "symbols": [normalize_symbol(symbol) for symbol in config["symbols"]],
        "offset_minutes": int(config["offset_minutes"]),
        "interval_minutes": int(config["interval_minutes"]),
    }


def get_batch_offset_minutes(batch_group: str = "core") -> int:
    return int(get_batch_config(batch_group)["offset_minutes"])


def get_batch_interval_minutes(batch_group: str = "core") -> int:
    return int(get_batch_config(batch_group)["interval_minutes"])


def list_available_batches() -> list[str]:
    return sorted(INSTRUMENT_BATCHES.keys())
