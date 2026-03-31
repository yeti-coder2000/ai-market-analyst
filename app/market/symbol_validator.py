from __future__ import annotations

from typing import List, Tuple

from fetcher import TwelveDataClient
from schema import FetchRequest, Instrument, Timeframe, TWELVEDATA_SYMBOL_MAP


def print_separator(char: str = "=", n: int = 72) -> None:
    print(char * n)


def validate_symbol(
    client: TwelveDataClient,
    instrument: Instrument,
    timeframe: Timeframe = Timeframe.M15,
    outputsize: int = 2,
) -> Tuple[bool, str]:
    """
    Returns:
        (is_valid, message)
    """
    try:
        req = FetchRequest(
            instrument=instrument,
            timeframe=timeframe,
            outputsize=outputsize,
        )
        payload = client.fetch_time_series(req)

        values = payload.get("values", [])
        if not values:
            return False, "API відповів без помилки, але values порожній"

        meta = payload.get("meta", {})
        symbol_used = meta.get("symbol", TWELVEDATA_SYMBOL_MAP.get(instrument, instrument.value))
        interval_used = meta.get("interval", timeframe.value)

        return True, f"OK | symbol={symbol_used} | interval={interval_used} | bars={len(values)}"

    except Exception as e:
        return False, str(e)


def main() -> None:
    client = TwelveDataClient()

    instruments_to_check: List[Instrument] = [
        Instrument.XAUUSD,
        Instrument.EURUSD,
        Instrument.GBPUSD,
        Instrument.BTCUSD,
        Instrument.ETHUSD,
        Instrument.UKOIL,
        Instrument.GER40,
        Instrument.NAS100,
        Instrument.SPX500,
        Instrument.DXY,
    ]

    print_separator()
    print("TWELVEDATA SYMBOL VALIDATOR")
    print_separator()

    results = []

    for instrument in instruments_to_check:
        provider_symbol = TWELVEDATA_SYMBOL_MAP.get(instrument, instrument.value)
        print(f"\nChecking {instrument.value} -> {provider_symbol}")

        ok, message = validate_symbol(
            client=client,
            instrument=instrument,
            timeframe=Timeframe.M15,
            outputsize=2,
        )

        status = "VALID" if ok else "INVALID"
        print(f"  Status:  {status}")
        print(f"  Result:  {message}")

        results.append(
            {
                "instrument": instrument.value,
                "provider_symbol": provider_symbol,
                "status": status,
                "message": message,
            }
        )

    print("\n")
    print_separator()
    print("SUMMARY")
    print_separator()

    for row in results:
        print(
            f"{row['instrument']:>8} | "
            f"{row['provider_symbol']:<12} | "
            f"{row['status']:<7} | "
            f"{row['message']}"
        )

    print_separator()


if __name__ == "__main__":
    main()