"""
Instrument batch configuration

Цей модуль визначає:
- групи інструментів (core / indices / інші в майбутньому)
- їх розклад (offset + interval)
"""

from typing import Dict, List, TypedDict


# ======================================================================================
# TYPES
# ======================================================================================

class BatchConfig(TypedDict):
    symbols: List[str]
    offset_minutes: int
    interval_minutes: int


# ======================================================================================
# CONFIG
# ======================================================================================

INSTRUMENT_BATCHES: Dict[str, BatchConfig] = {
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
    "indices": {
        "symbols": [
            "SNP500",
            "NDX",
            "DAX",
            "USOIL",
        ],
        "offset_minutes": 5,
        "interval_minutes": 15,
    },
}


# ======================================================================================
# HELPERS
# ======================================================================================

def get_batch_symbols(batch_group: str) -> List[str]:
    if batch_group not in INSTRUMENT_BATCHES:
        raise ValueError(f"Unknown batch_group: {batch_group}")

    return INSTRUMENT_BATCHES[batch_group]["symbols"]


def get_batch_config(batch_group: str) -> BatchConfig:
    if batch_group not in INSTRUMENT_BATCHES:
        raise ValueError(f"Unknown batch_group: {batch_group}")

    return INSTRUMENT_BATCHES[batch_group]


def list_available_batches() -> List[str]:
    return list(INSTRUMENT_BATCHES.keys())