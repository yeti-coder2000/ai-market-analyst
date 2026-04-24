from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from schema import Instrument


@dataclass(frozen=True)
class InstrumentConfig:
    instrument: Instrument
    enabled: bool = True
    priority: int = 1  # 1 = core, 2 = secondary, 3 = optional


INSTRUMENTS: Dict[str, InstrumentConfig] = {

    # =========================
    # CORE
    # =========================
    "XAU/USD": InstrumentConfig(instrument=Instrument.XAUUSD, enabled=True, priority=1),
    "EUR/USD": InstrumentConfig(instrument=Instrument.EURUSD, enabled=True, priority=1),
    "GBP/USD": InstrumentConfig(instrument=Instrument.GBPUSD, enabled=True, priority=1),

    # =========================
    # CRYPTO
    # =========================
    "BTC/USD": InstrumentConfig(instrument=Instrument.BTCUSD, enabled=True, priority=2),
    "ETH/USD": InstrumentConfig(instrument=Instrument.ETHUSD, enabled=True, priority=2),

    # =========================
    # INDICES / OIL (ВКЛЮЧАЄМО)
    # =========================
    "UKOIL": InstrumentConfig(instrument=Instrument.UKOIL, enabled=True, priority=2),
    "GER40": InstrumentConfig(instrument=Instrument.GER40, enabled=True, priority=2),
    "NAS100": InstrumentConfig(instrument=Instrument.NAS100, enabled=True, priority=2),
    "SPX500": InstrumentConfig(instrument=Instrument.SPX500, enabled=True, priority=2),

    # =========================
    # OPTIONAL
    # =========================
    "DXY": InstrumentConfig(instrument=Instrument.DXY, enabled=False, priority=3),
}


# ======================================================================================
# HELPERS
# ======================================================================================

def get_enabled_instruments() -> List[InstrumentConfig]:
    return [cfg for cfg in INSTRUMENTS.values() if cfg.enabled]


def get_core_instruments() -> List[InstrumentConfig]:
    return [
        cfg for cfg in INSTRUMENTS.values()
        if cfg.enabled and cfg.priority == 1
    ]


def get_secondary_instruments() -> List[InstrumentConfig]:
    return [
        cfg for cfg in INSTRUMENTS.values()
        if cfg.enabled and cfg.priority == 2
    ]


def get_optional_instruments() -> List[InstrumentConfig]:
    return [
        cfg for cfg in INSTRUMENTS.values()
        if cfg.enabled and cfg.priority == 3
    ]


# ======================================================================================
# NEW: BATCH-AWARE HELPERS
# ======================================================================================

def get_instruments_by_enum_list(instruments: List[Instrument]) -> List[InstrumentConfig]:
    """
    Використовується для batch system (core / indices)
    """
    result = []

    for cfg in INSTRUMENTS.values():
        if cfg.instrument in instruments and cfg.enabled:
            result.append(cfg)

    return result