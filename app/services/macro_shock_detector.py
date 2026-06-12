from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


MACRO_SHOCK_DETECTOR_VERSION = "macro-shock-detector-v1.0-usd-rebound-risk-pullback"


USD_LONG_SYMBOLS = {"USDCHF", "USDJPY", "USDCAD", "DXY", "DX"}
USD_SHORT_SYMBOLS = {"EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "XAUUSD", "XAGUSD"}
RISK_SYMBOLS = {"BTCUSD", "BTCUSDT", "ETHUSD", "ETHUSDT", "NAS100", "NDX", "SPX500", "SP500", "US500", "GER40"}
COMMODITY_SYMBOLS = {"UKOIL", "USOIL", "BRENT", "WTI"}


@dataclass
class MacroShockResult:
    macro_detector_version: str = MACRO_SHOCK_DETECTOR_VERSION
    macro_regime: str = "NO_MACRO_SHOCK"
    macro_shock_recent: bool = False
    macro_shock_score: float = 0.0
    macro_risk_mode: str = "NORMAL"
    macro_direction_for_symbol: str | None = None
    macro_caution_flags: list[str] = field(default_factory=list)
    macro_reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _as_upper(value: Any) -> str | None:
    if value in (None, "", [], {}):
        return None
    return str(value).strip().upper()


def _as_float(value: Any) -> float | None:
    if value in (None, "", [], {}):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value in (None, "", [], {}):
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _deep_get(data: dict[str, Any], *paths: str) -> Any:
    for path in paths:
        current: Any = data
        for part in path.split("."):
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(part)
        if current not in (None, "", [], {}):
            return current
    return None


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _normalize_symbol(value: Any) -> str | None:
    if value in (None, "", [], {}):
        return None
    return str(value).strip().upper().replace("/", "").replace(" ", "")


def _normalize_direction(value: Any) -> str | None:
    value = _as_upper(value)
    if value in {"LONG", "BUY", "BULL", "BULLISH", "UP"}:
        return "LONG"
    if value in {"SHORT", "SELL", "BEAR", "BEARISH", "DOWN"}:
        return "SHORT"
    if value in {"NEUTRAL", "NONE", "NO_TRADE"}:
        return "NEUTRAL"
    return value


def _is_usd_sensitive(symbol: str | None) -> bool:
    if not symbol:
        return False
    return symbol in USD_LONG_SYMBOLS or symbol in USD_SHORT_SYMBOLS or symbol in RISK_SYMBOLS or symbol in COMMODITY_SYMBOLS or "USD" in symbol


def _direction_is_usd_strength(symbol: str | None, direction: str | None) -> bool:
    if not symbol or direction not in {"LONG", "SHORT"}:
        return False
    if symbol in USD_LONG_SYMBOLS and direction == "LONG":
        return True
    if symbol in USD_SHORT_SYMBOLS and direction == "SHORT":
        return True
    return False


def _direction_is_usd_weakness(symbol: str | None, direction: str | None) -> bool:
    if not symbol or direction not in {"LONG", "SHORT"}:
        return False
    if symbol in USD_LONG_SYMBOLS and direction == "SHORT":
        return True
    if symbol in USD_SHORT_SYMBOLS and direction == "LONG":
        return True
    return False


def _direction_is_risk_pullback(symbol: str | None, direction: str | None) -> bool:
    return bool(symbol in RISK_SYMBOLS and direction == "SHORT")


def _direction_is_risk_on(symbol: str | None, direction: str | None) -> bool:
    return bool(symbol in RISK_SYMBOLS and direction == "LONG")


def _collect_basket_score(macro_context: dict[str, Any]) -> tuple[str | None, float, list[str]]:
    """
    Optional cross-asset mode.

    If a caller later passes a basket like:
      {"EURUSD":"UP", "GBPUSD":"UP", "XAUUSD":"UP", "USDJPY":"DOWN", ...}
    this function returns a macro regime. For now, the detector also works from a
    single signal using conservative post-shock heuristics.
    """
    basket = _safe_dict(macro_context.get("basket") or macro_context.get("moves") or macro_context.get("symbols"))
    if not basket:
        return None, 0.0, []

    def move(sym: str) -> str | None:
        raw = basket.get(sym) or basket.get(sym.replace("USD", "/USD"))
        return _as_upper(raw)

    usd_weak_votes = 0
    usd_strong_votes = 0
    reasons: list[str] = []

    for sym in ("EURUSD", "GBPUSD", "AUDUSD", "XAUUSD"):
        if move(sym) in {"UP", "LONG", "BULLISH"}:
            usd_weak_votes += 1
        if move(sym) in {"DOWN", "SHORT", "BEARISH"}:
            usd_strong_votes += 1

    for sym in ("USDJPY", "USDCHF", "USDCAD"):
        if move(sym) in {"DOWN", "SHORT", "BEARISH"}:
            usd_weak_votes += 1
        if move(sym) in {"UP", "LONG", "BULLISH"}:
            usd_strong_votes += 1

    risk_on_votes = 0
    risk_off_votes = 0
    for sym in ("NAS100", "SPX500", "GER40", "BTCUSD", "ETHUSD"):
        if move(sym) in {"UP", "LONG", "BULLISH"}:
            risk_on_votes += 1
        if move(sym) in {"DOWN", "SHORT", "BEARISH"}:
            risk_off_votes += 1

    if usd_weak_votes >= 4:
        reasons.append(f"cross-asset basket confirms USD weakness: votes={usd_weak_votes}")
        return "USD_WEAKNESS_SHOCK", float(usd_weak_votes), reasons
    if usd_strong_votes >= 4:
        reasons.append(f"cross-asset basket confirms USD strength: votes={usd_strong_votes}")
        return "USD_STRENGTH_SHOCK", float(usd_strong_votes), reasons
    if risk_on_votes >= 4:
        reasons.append(f"cross-asset basket confirms risk-on shock: votes={risk_on_votes}")
        return "RISK_ON_SHOCK", float(risk_on_votes), reasons
    if risk_off_votes >= 4:
        reasons.append(f"cross-asset basket confirms risk-off shock: votes={risk_off_votes}")
        return "RISK_OFF_SHOCK", float(risk_off_votes), reasons

    return "MIXED_MACRO", float(max(usd_weak_votes, usd_strong_votes, risk_on_votes, risk_off_votes)), reasons


def evaluate_macro_shock(payload: dict[str, Any]) -> MacroShockResult:
    metadata = _safe_dict(payload.get("metadata"))
    macro_context = _safe_dict(_first_non_empty(payload.get("macro_context"), metadata.get("macro_context")))

    explicit_regime = _as_upper(_first_non_empty(
        payload.get("macro_regime"),
        metadata.get("macro_regime"),
        macro_context.get("macro_regime"),
        macro_context.get("regime"),
    ))
    explicit_recent = _as_bool(_first_non_empty(
        payload.get("macro_shock_recent"),
        metadata.get("macro_shock_recent"),
        macro_context.get("macro_shock_recent"),
        macro_context.get("shock_recent"),
    ))
    explicit_score = _as_float(_first_non_empty(
        payload.get("macro_shock_score"),
        metadata.get("macro_shock_score"),
        macro_context.get("macro_shock_score"),
        macro_context.get("score"),
    ))

    symbol = _normalize_symbol(_first_non_empty(payload.get("symbol"), payload.get("instrument"), metadata.get("symbol")))
    direction = _normalize_direction(_first_non_empty(payload.get("direction"), metadata.get("direction")))
    scenario = _as_upper(_first_non_empty(payload.get("scenario"), payload.get("scenario_type"), metadata.get("scenario"), metadata.get("scenario_type"))) or ""
    market_state = _as_upper(_first_non_empty(payload.get("market_state"), metadata.get("market_state")))
    open_behavior = _as_upper(_first_non_empty(payload.get("open_behavior"), metadata.get("open_behavior")))
    entry_model_hint = _as_upper(_first_non_empty(payload.get("entry_model_hint"), metadata.get("entry_model_hint"), payload.get("execution_model"))) or ""
    news_risk_state = _as_upper(_first_non_empty(payload.get("news_risk_state"), metadata.get("news_risk_state")))
    news_provider_status = _as_upper(_first_non_empty(payload.get("news_provider_status"), metadata.get("news_provider_status")))
    local_structure_damaged = _as_bool(_first_non_empty(payload.get("local_structure_damaged"), metadata.get("local_structure_damaged")))
    recent_impulse_atr = _as_float(_first_non_empty(payload.get("recent_impulse_atr"), metadata.get("recent_impulse_atr"), payload.get("displacement_atr"), metadata.get("displacement_atr")))

    basket_regime, basket_score, basket_reasons = _collect_basket_score(macro_context)

    reasons: list[str] = []
    flags: list[str] = []
    score = explicit_score if explicit_score is not None else 0.0
    regime = explicit_regime or basket_regime or "NO_MACRO_SHOCK"
    recent = bool(explicit_recent) if explicit_recent is not None else False

    if basket_regime and basket_regime != "MIXED_MACRO":
        recent = True
        score = max(score, basket_score)
        reasons.extend(basket_reasons)

    high_or_unknown_news = news_risk_state in {"HIGH_IMPACT", "POST_NEWS_CAUTION", "PROVIDER_UNAVAILABLE"} or bool(
        news_provider_status and any(token in news_provider_status for token in {"UNAVAILABLE", "ERROR", "403", "429", "TIMEOUT"})
    )

    shock_like_ltf = (
        _is_usd_sensitive(symbol)
        and market_state in {"TRANSITION", "TREND"}
        and (
            "OPEN_TEST_DRIVE" in scenario
            or "SWEEP_RETURN" in scenario
            or open_behavior in {"OPEN_TEST_DRIVE", "OPEN_AUCTION"}
            or "FAILED_ACCEPTANCE_RETEST" in entry_model_hint
        )
        and (
            local_structure_damaged is True
            or recent_impulse_atr is not None and recent_impulse_atr >= 1.5
            or high_or_unknown_news
            or "FAILED_ACCEPTANCE_RETEST" in entry_model_hint
        )
    )

    if shock_like_ltf and not recent:
        recent = True
        score = max(score, 3.0)
        reasons.append("single-symbol LTF structure is consistent with post-shock/reclaim conditions")

        if _direction_is_usd_strength(symbol, direction):
            regime = "USD_REBOUND_AFTER_SHOCK"
        elif _direction_is_usd_weakness(symbol, direction):
            regime = "USD_WEAKNESS_CONTINUATION_AFTER_SHOCK"
        elif _direction_is_risk_pullback(symbol, direction):
            regime = "RISK_PULLBACK_AFTER_SHOCK"
        elif _direction_is_risk_on(symbol, direction):
            regime = "RISK_ON_CONTINUATION_AFTER_SHOCK"
        elif symbol in COMMODITY_SYMBOLS:
            regime = "COMMODITY_SHOCK_AFTERMOVE"
        else:
            regime = "POST_SHOCK_RETEST"

    direction_for_symbol: str | None = None
    if _direction_is_usd_strength(symbol, direction):
        direction_for_symbol = "USD_STRENGTH"
    elif _direction_is_usd_weakness(symbol, direction):
        direction_for_symbol = "USD_WEAKNESS"
    elif _direction_is_risk_pullback(symbol, direction):
        direction_for_symbol = "RISK_PULLBACK"
    elif _direction_is_risk_on(symbol, direction):
        direction_for_symbol = "RISK_ON"

    risk_mode = "NORMAL"
    if recent:
        risk_mode = "POST_SHOCK_CAUTION"
        flags.append("macro_shock_recent")
        if high_or_unknown_news:
            flags.append("news_or_provider_uncertainty")
        if local_structure_damaged is True:
            flags.append("local_structure_damaged")
        if "FAILED_ACCEPTANCE_RETEST" in entry_model_hint:
            flags.append("failed_acceptance_retest_after_shock")

    return MacroShockResult(
        macro_regime=regime,
        macro_shock_recent=recent,
        macro_shock_score=round(score, 3),
        macro_risk_mode=risk_mode,
        macro_direction_for_symbol=direction_for_symbol,
        macro_caution_flags=list(dict.fromkeys(flags)),
        macro_reasons=list(dict.fromkeys(reasons)),
    )


def apply_macro_shock_context(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return payload

    result = evaluate_macro_shock(payload).to_dict()
    enriched = dict(payload)

    metadata = enriched.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    else:
        metadata = dict(metadata)

    for key, value in result.items():
        if value in (None, "", [], {}):
            continue
        enriched[key] = value
        metadata[key] = value

    enriched["metadata"] = metadata
    return enriched


if __name__ == "__main__":
    sample = {
        "symbol": "AUDUSD",
        "direction": "SHORT",
        "scenario": "TPO_OPEN_TEST_DRIVE_SHORT",
        "market_state": "TRANSITION",
        "open_behavior": "OPEN_TEST_DRIVE",
        "entry_model_hint": "FAILED_ACCEPTANCE_RETEST",
        "news_provider_status": "FINNHUB_UNAVAILABLE",
    }
    print(evaluate_macro_shock(sample).to_dict())
