from __future__ import annotations

import json
from pathlib import Path

from app.services.positioning.collectors.cftc_cot_collector import (
    CFTCContractSpec,
    CFTC_CONTRACTS,
    collect_and_write_cftc_cot_snapshot,
    collect_cftc_cot_snapshot,
)


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, tff_rows, disagg_rows):
        self.tff_rows = tff_rows
        self.disagg_rows = disagg_rows
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if "gpe5-46if" in url:
            return FakeResponse(self.tff_rows)
        if "72hh-3qpy" in url:
            return FakeResponse(self.disagg_rows)
        raise AssertionError(url)


def _row(report_date: str, code: str, long_value: int, short_value: int, oi: int):
    return {
        "market_and_exchange_names": "TEST MARKET",
        "report_date_as_yyyy_mm_dd": report_date + "T00:00:00.000",
        "cftc_contract_market_code": code,
        "open_interest_all": str(oi),
        "lev_money_positions_long": str(long_value),
        "lev_money_positions_short": str(short_value),
        "m_money_positions_long_all": str(long_value),
        "m_money_positions_short_all": str(short_value),
    }


def test_cftc_collector_normalizes_percentile_and_inverse_quote() -> None:
    contracts = (
        CFTCContractSpec(
            "USDJPY",
            "TFF",
            "097741",
            "LEVERAGED_MONEY",
            "lev_money_positions_long",
            "lev_money_positions_short",
            -1,
            "Japanese Yen",
        ),
        CFTCContractSpec(
            "XAUUSD",
            "DISAGGREGATED",
            "088691",
            "MANAGED_MONEY",
            "m_money_positions_long_all",
            "m_money_positions_short_all",
            1,
            "Gold",
        ),
    )
    tff_rows = [
        _row("2026-06-30", "097741", 100, 150, 1000),
        _row("2026-07-07", "097741", 90, 160, 1000),
        _row("2026-07-14", "097741", 80, 180, 1000),
    ]
    disagg_rows = [
        _row("2026-06-30", "088691", 100, 90, 1000),
        _row("2026-07-07", "088691", 120, 90, 1000),
        _row("2026-07-14", "088691", 160, 80, 1000),
    ]

    payload = collect_cftc_cot_snapshot(
        target_date="2026-07-19",
        session=FakeSession(tff_rows, disagg_rows),
        contracts=contracts,
        lookback_weeks=52,
        max_attempts=1,
    )

    assert payload["status"] == "OK"
    assert payload["battle_gate_impact"] == "none"
    assert payload["telegram_signal_impact"] == "none"

    jpy = next(item for item in payload["items"] if item["symbol"] == "USDJPY")
    assert jpy["positions"]["raw_net_contracts"] == -100
    assert jpy["positions"]["project_net_contracts"] == 100
    assert jpy["positions"]["weekly_change_net_contracts"] == 30
    assert "INVERTED_TO_PROJECT_SYMBOL" in jpy["flags"]

    gold = next(item for item in payload["items"] if item["symbol"] == "XAUUSD")
    assert gold["positions"]["project_net_contracts"] == 80
    assert gold["normalization"]["percentile"] > 80
    assert gold["interpretation"]["primary_tag"] == "COT_BUILDING_LONGS"
    assert "COT_EXTREME_LONG" not in gold["flags"]
    assert gold["interpretation"]["battle_gate_impact"] == "none"


def test_default_contract_map_includes_cme_ether() -> None:
    ether = next(spec for spec in CFTC_CONTRACTS if spec.symbol == "ETHUSD")
    assert ether.dataset == "TFF"
    assert ether.contract_code == "146021"
    assert ether.trader_group == "LEVERAGED_MONEY"


def test_cftc_empty_refresh_does_not_overwrite_last_snapshot(tmp_path: Path) -> None:
    output = tmp_path / "positioning" / "cftc_weekly_positioning_snapshot.json"
    output.parent.mkdir(parents=True)
    existing = {"version": "existing", "status": "OK", "items": [{"symbol": "XAUUSD"}]}
    output.write_text(json.dumps(existing), encoding="utf-8")

    path, payload = collect_and_write_cftc_cot_snapshot(
        output_path=str(output),
        target_date="2026-07-19",
        session=FakeSession([], []),
        contracts=(
            CFTCContractSpec(
                "XAUUSD",
                "DISAGGREGATED",
                "088691",
                "MANAGED_MONEY",
                "m_money_positions_long_all",
                "m_money_positions_short_all",
            ),
        ),
        max_attempts=1,
        persist_empty=False,
    )

    assert path == output
    assert payload["status"] == "NO_DATA"
    assert json.loads(output.read_text(encoding="utf-8")) == existing
