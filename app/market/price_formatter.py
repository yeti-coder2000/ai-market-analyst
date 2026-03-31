from __future__ import annotations

from schema import Instrument


PRICE_PRECISION = {
    Instrument.XAUUSD: 2,
    Instrument.UKOIL: 2,
    Instrument.GER40: 1,
    Instrument.NAS100: 1,
    Instrument.SPX500: 2,

    Instrument.EURUSD: 5,
    Instrument.GBPUSD: 5,
    Instrument.DXY: 3,

    Instrument.BTCUSD: 2,
    Instrument.ETHUSD: 2,
}


def get_price_precision(instrument: Instrument) -> int:
    return PRICE_PRECISION.get(instrument, 2)


def fmt_price(value, instrument: Instrument) -> str:
    if value is None:
        return "-"
    try:
        precision = get_price_precision(instrument)
        return f"{float(value):.{precision}f}"
    except Exception:
        return str(value)


def fmt_generic(value, precision: int = 2) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.{precision}f}"
    except Exception:
        return str(value)