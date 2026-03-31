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
    "XAU/USD": InstrumentConfig(instrument=Instrument.XAUUSD, enabled=True, priority=1),
    "EUR/USD": InstrumentConfig(instrument=Instrument.EURUSD, enabled=True, priority=1),
    "GBP/USD": InstrumentConfig(instrument=Instrument.GBPUSD, enabled=True, priority=1),

    "BTC/USD": InstrumentConfig(instrument=Instrument.BTCUSD, enabled=True, priority=2),
    "ETH/USD": InstrumentConfig(instrument=Instrument.ETHUSD, enabled=True, priority=2),

    "UKOIL": InstrumentConfig(instrument=Instrument.UKOIL, enabled=False, priority=2),
    "GER40": InstrumentConfig(instrument=Instrument.GER40, enabled=False, priority=2),
    "NAS100": InstrumentConfig(instrument=Instrument.NAS100, enabled=False, priority=2),
    "SPX500": InstrumentConfig(instrument=Instrument.SPX500, enabled=False, priority=2),

    "DXY": InstrumentConfig(instrument=Instrument.DXY, enabled=False, priority=3),
}


def get_enabled_instruments() -> List[InstrumentConfig]:
    return [cfg for cfg in INSTRUMENTS.values() if cfg.enabled]


def get_core_instruments() -> List[InstrumentConfig]:
    return [cfg for cfg in INSTRUMENTS.values() if cfg.enabled and cfg.priority == 1]


def get_secondary_instruments() -> List[InstrumentConfig]:
    return [cfg for cfg in INSTRUMENTS.values() if cfg.enabled and cfg.priority == 2]


def get_optional_instruments() -> List[InstrumentConfig]:
    return [cfg for cfg in INSTRUMENTS.values() if cfg.enabled and cfg.priority == 3]