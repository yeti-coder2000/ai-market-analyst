from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
import json
import os
from pathlib import Path
from typing import Any

try:
    from app.services.battle_gate_open_behavior_policy import evaluate_open_behavior_policy
except Exception:  # pragma: no cover
    evaluate_open_behavior_policy = None  # type: ignore[assignment]


BATTLE_PERMISSION_VERSION = "battle-permission-v1.3-v2-neutral-otd-authoritative"


class BattlePermission(str, Enum):
    BATTLE_READY = "BATTLE_READY"
    RESEARCH_ONLY = "RESEARCH_ONLY"
    BLOCKED_BY_MARKET_CLOSED = "BLOCKED_BY_MARKET_CLOSED"
    BLOCKED_BY_STALE_DATA = "BLOCKED_BY_STALE_DATA"
    BLOCKED_BY_AUCTION = "BLOCKED_BY_AUCTION"
    BLOCKED_BY_HTF = "BLOCKED_BY_HTF"
    BLOCKED_BY_EXECUTION = "BLOCKED_BY_EXECUTION"
    BLOCKED_BY_RR = "BLOCKED_BY_RR"
    BLOCKED_BY_STOP_QUALITY = "BLOCKED_BY_STOP_QUALITY"
    BLOCKED_BY_QUALITY = "BLOCKED_BY_QUALITY"
    BLOCKED_BY_CONTEXT = "BLOCKED_BY_CONTEXT"
    NOT_READY = "NOT_READY"


class TelegramDeliveryMode(str, Enum):
    BATTLE_ALERT = "BATTLE_ALERT"
    RESEARCH_ALERT = "RESEARCH_ALERT"
    SUPPRESS = "SUPPRESS"


@dataclass
class BattlePermissionResult:
    battle_permission: str
    telegram_delivery_mode: str
    battle_ready: bool
    auction_context_score: int
    reasons: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    modifiers: list[str] = field(default_factory=list)

    market_is_open: bool | None = None
    market_status: str | None = None
    tpo_signal_permission: str | None = None
    tpo_telegram_modifier: str | None = None
    open_relation: str | None = None
    auction_bias: str | None = None

    # TPO/open-behavior context fields.
    open_context: str | None = None
    open_behavior: str | None = None
    open_behavior_confidence: float | None = None
    entry_model_hint: str | None = None
    stop_model_hint: str | None = None
    battle_bias_hint: str | None = None
    primary_interest_zone: dict[str, Any] | None = None
    interest_zone_type: str | None = None
    interest_zone_price: float | None = None
    interest_zone_role: str | None = None

    direction: str | None = None
    htf_bias: str | None = None
    signal_alignment: str | None = None
    execution_status: str | None = None
    practical_rr: float | None = None
    stop_quality: str | None = None
    quality_tier: str | None = None

    # Battle Gate v2 shadow-mode fields.
    # Legacy Battle Gate remains the execution authority for now.
    battle_gate_v2_decision: str | None = None
    battle_gate_v2_risk_mode: str | None = None
    battle_gate_v2_battle_allowed: bool | None = None
    battle_gate_v2_should_suppress_telegram: bool | None = None
    battle_gate_v2_score_delta: float | None = None
    battle_gate_v2_reasons: list[str] = field(default_factory=list)
    battle_gate_v2_blockers: list[str] = field(default_factory=list)
    battle_gate_v2_modifiers: list[str] = field(default_factory=list)
    battle_gate_v2_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _deep_get(data: dict[str, Any], *paths: str) -> Any:
    """
    Reads the first non-empty value from dotted paths.

    Example:
    _deep_get(payload, "metadata.auction_context.market_status", "market_status")
    """
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

    if value is None:
        return None

    if isinstance(value, str):
        normalized = value.strip().lower()

        if normalized in {"true", "yes", "1"}:
            return True

        if normalized in {"false", "no", "0"}:
            return False

    return None


def _normalize_direction(value: Any) -> str | None:
    normalized = _as_upper(value)

    if normalized in {"LONG", "BUY", "BULL", "BULLISH", "UP"}:
        return "LONG"

    if normalized in {"SHORT", "SELL", "BEAR", "BEARISH", "DOWN"}:
        return "SHORT"

    if normalized in {"NEUTRAL", "NONE", "NO_TRADE"}:
        return "NEUTRAL"

    return normalized


def _normalize_open_relation(value: Any) -> str | None:
    normalized = _as_upper(value)

    if normalized in {"OPEN_INSIDE_VA", "INSIDE_VALUE", "INSIDE_VALUE_AREA"}:
        return "INSIDE_VA"

    if normalized in {"OPEN_IN_RANGE", "IN_RANGE"}:
        return "RANGE"

    if normalized in {"OPEN_OUT_OF_RANGE", "OUTSIDE_RANGE", "OUTSIDE_PREVIOUS_RANGE"}:
        return "OUT_OF_RANGE"

    return normalized


def _direction_matches_htf(direction: str | None, htf_bias: str | None) -> bool:
    if not direction or not htf_bias:
        return False

    direction = _normalize_direction(direction)
    htf_bias = _normalize_direction(htf_bias)

    return direction in {"LONG", "SHORT"} and direction == htf_bias


def _is_neutral_htf(value: str | None) -> bool:
    normalized = _normalize_direction(value)
    return normalized in {None, "", "NEUTRAL", "NONE", "FLAT", "NO_TRADE"}


def _is_valid_stop_quality_for_battle(stop_quality: str | None) -> bool:
    if stop_quality in {None, "", "TIGHT_STOP", "BAD", "WEAK", "NO_STOP", "NONE"}:
        return False
    return True


def _v2_allows_neutral_open_test_drive_transition(
    *,
    inputs: dict[str, Any],
    v2_policy: dict[str, Any],
) -> bool:
    """
    Authoritative narrow override for the legacy HTF gate.

    OPEN_TEST_DRIVE with HTF NEUTRAL is a valid transition model:
    balance/accumulation -> directional distribution.

    This does NOT bypass hard blockers:
    - market/data/TPO permission blockers are evaluated before this override;
    - status/execution blockers are evaluated before this override;
    - RR/stop/quality checks are still evaluated after this override.

    The override only prevents legacy from misclassifying NEUTRAL HTF as HTF conflict.
    """
    open_behavior = _as_upper(inputs.get("open_behavior"))
    htf_bias = _normalize_direction(inputs.get("htf_bias"))
    direction = _normalize_direction(inputs.get("direction"))
    execution_status = _as_upper(inputs.get("execution_status"))
    practical_rr = _as_float(inputs.get("practical_rr"))
    stop_quality = _as_upper(inputs.get("stop_quality"))

    decision = _as_upper(v2_policy.get("decision"))
    risk_mode = _as_upper(v2_policy.get("risk_mode"))
    battle_allowed = _as_bool(v2_policy.get("battle_allowed"))

    if open_behavior != "OPEN_TEST_DRIVE":
        return False

    if not _is_neutral_htf(htf_bias):
        return False

    if direction not in {"LONG", "SHORT"}:
        return False

    if decision not in {"ALLOW", "ALLOW_WITH_CAUTION"}:
        return False

    if battle_allowed is not True:
        return False

    if risk_mode not in {"TRANSITION_CANDIDATE", "BATTLE_CANDIDATE", "CAUTION_BATTLE_CANDIDATE"}:
        return False

    if execution_status != "EXECUTABLE":
        return False

    if practical_rr is None or practical_rr < 2.0:
        return False

    if not _is_valid_stop_quality_for_battle(stop_quality):
        return False

    return True


def _extract_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Some journal events are shaped like:
    {"payload": {"payload": signal}}

    Telegram payloads are usually already flat enough.
    This keeps the gate tolerant.
    """
    nested = payload.get("payload")

    if isinstance(nested, dict):
        nested_2 = nested.get("payload")
        if isinstance(nested_2, dict):
            return nested_2
        return nested

    return payload


_TPO_STORE_CACHE: dict[str, Any] = {
    "path": None,
    "mtime": None,
    "data": None,
}


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _set_if_missing(target: dict[str, Any], key: str, value: Any) -> None:
    if value in (None, "", [], {}):
        return
    if target.get(key) in (None, "", [], {}):
        target[key] = value


def _resolve_tpo_store_path() -> Path:
    """
    Resolve TPO store path without requiring settings import.

    Priority:
    1. TPO_STORE_PATH env.
    2. RUNTIME_DIR env + /tpo/tpo_latest.json.
    3. /var/data/runtime/tpo/tpo_latest.json on Render.
    4. runtime/tpo/tpo_latest.json locally.
    """
    explicit = os.getenv("TPO_STORE_PATH")
    if explicit:
        return Path(explicit)

    runtime_dir = os.getenv("RUNTIME_DIR")
    if runtime_dir:
        return Path(runtime_dir) / "tpo" / "tpo_latest.json"

    render_path = Path("/var/data/runtime/tpo/tpo_latest.json")
    if render_path.exists():
        return render_path

    return Path("runtime/tpo/tpo_latest.json")


def _load_tpo_store() -> dict[str, Any] | None:
    """
    Load tpo_latest.json with mtime cache.

    The store is small enough to read when changed, but this avoids parsing it
    for every signal during a busy cycle.
    """
    path = _resolve_tpo_store_path()

    try:
        stat = path.stat()
    except OSError:
        return None

    cached_path = _TPO_STORE_CACHE.get("path")
    cached_mtime = _TPO_STORE_CACHE.get("mtime")

    if cached_path == str(path) and cached_mtime == stat.st_mtime:
        data = _TPO_STORE_CACHE.get("data")
        return data if isinstance(data, dict) else None

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None

    if not isinstance(data, dict):
        return None

    _TPO_STORE_CACHE["path"] = str(path)
    _TPO_STORE_CACHE["mtime"] = stat.st_mtime
    _TPO_STORE_CACHE["data"] = data
    return data


def _extract_primary_interest_zone(*sources: Any) -> dict[str, Any] | None:
    for source in sources:
        if not isinstance(source, dict):
            continue

        zone = source.get("primary_interest_zone")
        if isinstance(zone, dict) and zone:
            return dict(zone)

        zone = source.get("interest_zone")
        if isinstance(zone, dict) and zone:
            return dict(zone)

    return None


def _get_symbol_tpo_record(symbol: str | None) -> dict[str, Any] | None:
    if not symbol:
        return None

    store = _load_tpo_store()
    if not isinstance(store, dict):
        return None

    symbols = store.get("symbols")
    if not isinstance(symbols, dict):
        return None

    exact = symbols.get(symbol)
    if isinstance(exact, dict):
        return exact

    upper_symbol = str(symbol).upper()
    for key, value in symbols.items():
        if str(key).upper() == upper_symbol and isinstance(value, dict):
            return value

    return None


def _enrich_payload_with_tpo_store(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Attach TPO/open-behavior fields from tpo_latest.json when the signal payload
    does not already contain them.

    This is intentionally defensive:
    - it never fails the gate if the store is missing/bad;
    - it does not overwrite already-present signal fields;
    - it keeps Battle Gate v2 in shadow mode but makes its inputs visible to
      payload/metadata/telemetry/statistics.
    """
    enriched = dict(payload)

    metadata = enriched.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    else:
        metadata = dict(metadata)

    symbol = _deep_get(
        enriched,
        "symbol",
        "instrument",
        "metadata.symbol",
        "metadata.instrument",
    )
    symbol = str(symbol).upper() if symbol not in (None, "", [], {}) else None

    record = _get_symbol_tpo_record(symbol)
    if not isinstance(record, dict):
        enriched["metadata"] = metadata
        return enriched

    context = record.get("context")
    if not isinstance(context, dict):
        context = {}

    filters = record.get("filters")
    if not isinstance(filters, dict):
        filters = {}

    open_behavior_record = record.get("open_behavior")
    if not isinstance(open_behavior_record, dict):
        open_behavior_record = {}

    primary_zone = _extract_primary_interest_zone(open_behavior_record, context, filters, record)

    values = {
        "market_status": _first_non_empty(
            context.get("market_status"),
            filters.get("market_status"),
            record.get("market_status"),
        ),
        "market_is_open": _first_non_empty(
            context.get("market_is_open"),
            filters.get("market_is_open"),
            record.get("market_is_open"),
        ),
        "tpo_signal_permission": _first_non_empty(
            context.get("tpo_signal_permission"),
            filters.get("tpo_signal_permission"),
            filters.get("signal_permission"),
            record.get("tpo_signal_permission"),
            record.get("signal_permission"),
        ),
        "tpo_telegram_modifier": _first_non_empty(
            context.get("tpo_telegram_modifier"),
            filters.get("tpo_telegram_modifier"),
            filters.get("telegram_modifier"),
            record.get("tpo_telegram_modifier"),
            record.get("telegram_modifier"),
        ),
        "telegram_modifier": _first_non_empty(
            filters.get("telegram_modifier"),
            context.get("telegram_modifier"),
            record.get("telegram_modifier"),
        ),
        "open_relation": _first_non_empty(
            context.get("open_relation"),
            filters.get("open_relation"),
            record.get("open_relation"),
        ),
        "auction_bias": _first_non_empty(
            context.get("auction_bias"),
            filters.get("auction_bias"),
            record.get("auction_bias"),
        ),
        "open_context": _first_non_empty(
            context.get("open_context"),
            open_behavior_record.get("open_context"),
            record.get("open_context"),
        ),
        "open_behavior": _first_non_empty(
            context.get("open_behavior"),
            open_behavior_record.get("open_behavior"),
            record.get("open_behavior") if not isinstance(record.get("open_behavior"), dict) else None,
        ),
        "open_behavior_confidence": _first_non_empty(
            context.get("open_behavior_confidence"),
            open_behavior_record.get("open_behavior_confidence"),
            open_behavior_record.get("confidence"),
            record.get("open_behavior_confidence"),
        ),
        "entry_model_hint": _first_non_empty(
            context.get("entry_model_hint"),
            open_behavior_record.get("entry_model_hint"),
            record.get("entry_model_hint"),
        ),
        "stop_model_hint": _first_non_empty(
            context.get("stop_model_hint"),
            open_behavior_record.get("stop_model_hint"),
            record.get("stop_model_hint"),
        ),
        "battle_bias_hint": _first_non_empty(
            context.get("battle_bias_hint"),
            open_behavior_record.get("battle_bias_hint"),
            record.get("battle_bias_hint"),
        ),
        "nearest_npoc_distance": _first_non_empty(
            context.get("nearest_npoc_distance"),
            filters.get("nearest_npoc_distance"),
            record.get("nearest_npoc_distance"),
        ),
        "ib_extension_up_pct": _first_non_empty(
            context.get("ib_extension_up_pct"),
            filters.get("ib_extension_up_pct"),
            record.get("ib_extension_up_pct"),
        ),
        "ib_extension_down_pct": _first_non_empty(
            context.get("ib_extension_down_pct"),
            filters.get("ib_extension_down_pct"),
            record.get("ib_extension_down_pct"),
        ),
        "accepted_back_inside_value": _first_non_empty(
            context.get("accepted_back_inside_value"),
            filters.get("accepted_back_inside_value"),
            record.get("accepted_back_inside_value"),
        ),
    }

    for key, value in values.items():
        _set_if_missing(enriched, key, value)
        _set_if_missing(metadata, key, value)

    if primary_zone:
        _set_if_missing(enriched, "primary_interest_zone", primary_zone)
        _set_if_missing(metadata, "primary_interest_zone", primary_zone)

        _set_if_missing(enriched, "interest_zone_type", primary_zone.get("zone_type"))
        _set_if_missing(metadata, "interest_zone_type", primary_zone.get("zone_type"))

        _set_if_missing(enriched, "interest_zone_price", primary_zone.get("price"))
        _set_if_missing(metadata, "interest_zone_price", primary_zone.get("price"))

        _set_if_missing(enriched, "interest_zone_role", primary_zone.get("role"))
        _set_if_missing(metadata, "interest_zone_role", primary_zone.get("role"))

    enriched["metadata"] = metadata
    return enriched



def _evaluate_v2_shadow(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Evaluate Battle Gate v2 policy in shadow mode.

    This must never break legacy Battle Gate.
    If v2 policy import/evaluation fails, legacy gate still works and we attach an error marker.
    """
    if evaluate_open_behavior_policy is None:
        return {
            "decision": None,
            "risk_mode": None,
            "battle_allowed": None,
            "should_suppress_telegram": None,
            "score_delta": None,
            "reasons": [],
            "blockers": [],
            "modifiers": [],
            "error": "battle_gate_open_behavior_policy_import_failed",
        }

    try:
        return evaluate_open_behavior_policy(payload)
    except Exception as exc:  # noqa: BLE001
        return {
            "decision": None,
            "risk_mode": None,
            "battle_allowed": None,
            "should_suppress_telegram": None,
            "score_delta": None,
            "reasons": [],
            "blockers": [],
            "modifiers": [],
            "error": f"{type(exc).__name__}: {exc}",
        }


def extract_battle_inputs(raw_payload: dict[str, Any]) -> dict[str, Any]:
    payload = _enrich_payload_with_tpo_store(_extract_payload(raw_payload))

    market_is_open = _as_bool(
        _deep_get(
            payload,
            "metadata.auction_context.market_is_open",
            "metadata.auction_filters.market_is_open",
            "auction_context.market_is_open",
            "auction_filters.market_is_open",
            "market_is_open",
        )
    )

    market_status = _as_upper(
        _deep_get(
            payload,
            "metadata.auction_context.market_status",
            "metadata.auction_filters.market_status",
            "auction_context.market_status",
            "auction_filters.market_status",
            "market_status",
        )
    )

    tpo_signal_permission = _as_upper(
        _deep_get(
            payload,
            "metadata.tpo_signal_permission",
            "metadata.signal_permission",
            "metadata.auction_filters.tpo_signal_permission",
            "metadata.auction_filters.signal_permission",
            "metadata.filters.tpo_signal_permission",
            "metadata.filters.signal_permission",
            "filters.tpo_signal_permission",
            "filters.signal_permission",
            "context.tpo_signal_permission",
            "context.signal_permission",
            "auction_filters.tpo_signal_permission",
            "auction_filters.signal_permission",
            "tpo_signal_permission",
            "signal_permission",
        )
    )

    tpo_telegram_modifier = _as_upper(
        _deep_get(
            payload,
            "metadata.tpo_telegram_modifier",
            "metadata.telegram_modifier",
            "metadata.auction_filters.telegram_modifier",
            "metadata.auction_filters.tpo_telegram_modifier",
            "metadata.filters.telegram_modifier",
            "metadata.filters.tpo_telegram_modifier",
            "filters.telegram_modifier",
            "filters.tpo_telegram_modifier",
            "context.telegram_modifier",
            "context.tpo_telegram_modifier",
            "auction_filters.telegram_modifier",
            "auction_filters.tpo_telegram_modifier",
            "telegram_modifier",
            "tpo_telegram_modifier",
        )
    )

    open_relation = _normalize_open_relation(
        _deep_get(
            payload,
            "metadata.tpo_open_relation",
            "metadata.open_relation",
            "metadata.auction_context.open_relation",
            "metadata.auction_filters.open_relation",
            "metadata.context.open_relation",
            "metadata.filters.open_relation",
            "context.open_relation",
            "filters.open_relation",
            "auction_context.open_relation",
            "auction_filters.open_relation",
            "open_relation",
        )
    )

    auction_bias = _as_upper(
        _deep_get(
            payload,
            "metadata.tpo_auction_bias",
            "metadata.auction_bias",
            "metadata.auction_context.auction_bias",
            "metadata.auction_filters.auction_bias",
            "metadata.context.auction_bias",
            "metadata.filters.auction_bias",
            "context.auction_bias",
            "filters.auction_bias",
            "auction_context.auction_bias",
            "auction_filters.auction_bias",
            "auction_bias",
        )
    )

    open_context = _as_upper(
        _deep_get(
            payload,
            "metadata.open_context",
            "metadata.context.open_context",
            "metadata.open_behavior.open_context",
            "context.open_context",
            "open_behavior.open_context",
            "open_context",
        )
    )

    open_behavior = _as_upper(
        _deep_get(
            payload,
            "metadata.open_behavior",
            "metadata.context.open_behavior",
            "metadata.open_behavior.open_behavior",
            "context.open_behavior",
            "open_behavior.open_behavior",
            "open_behavior",
        )
    )

    open_behavior_confidence = _as_float(
        _deep_get(
            payload,
            "metadata.open_behavior_confidence",
            "metadata.context.open_behavior_confidence",
            "metadata.open_behavior.open_behavior_confidence",
            "metadata.open_behavior.confidence",
            "context.open_behavior_confidence",
            "open_behavior.open_behavior_confidence",
            "open_behavior.confidence",
            "open_behavior_confidence",
        )
    )

    entry_model_hint = _as_upper(
        _deep_get(
            payload,
            "metadata.entry_model_hint",
            "metadata.context.entry_model_hint",
            "metadata.open_behavior.entry_model_hint",
            "context.entry_model_hint",
            "open_behavior.entry_model_hint",
            "entry_model_hint",
        )
    )

    stop_model_hint = _as_upper(
        _deep_get(
            payload,
            "metadata.stop_model_hint",
            "metadata.context.stop_model_hint",
            "metadata.open_behavior.stop_model_hint",
            "context.stop_model_hint",
            "open_behavior.stop_model_hint",
            "stop_model_hint",
        )
    )

    battle_bias_hint = _as_upper(
        _deep_get(
            payload,
            "metadata.battle_bias_hint",
            "metadata.context.battle_bias_hint",
            "metadata.open_behavior.battle_bias_hint",
            "context.battle_bias_hint",
            "open_behavior.battle_bias_hint",
            "battle_bias_hint",
        )
    )

    primary_interest_zone = _deep_get(
        payload,
        "metadata.primary_interest_zone",
        "metadata.open_behavior.primary_interest_zone",
        "open_behavior.primary_interest_zone",
        "primary_interest_zone",
    )
    if not isinstance(primary_interest_zone, dict):
        primary_interest_zone = None

    interest_zone_type = _as_upper(
        _deep_get(
            payload,
            "metadata.interest_zone_type",
            "metadata.primary_interest_zone.zone_type",
            "metadata.open_behavior.primary_interest_zone.zone_type",
            "open_behavior.primary_interest_zone.zone_type",
            "primary_interest_zone.zone_type",
            "interest_zone_type",
        )
    )

    interest_zone_price = _as_float(
        _deep_get(
            payload,
            "metadata.interest_zone_price",
            "metadata.primary_interest_zone.price",
            "metadata.open_behavior.primary_interest_zone.price",
            "open_behavior.primary_interest_zone.price",
            "primary_interest_zone.price",
            "interest_zone_price",
        )
    )

    interest_zone_role = _as_upper(
        _deep_get(
            payload,
            "metadata.interest_zone_role",
            "metadata.primary_interest_zone.role",
            "metadata.open_behavior.primary_interest_zone.role",
            "open_behavior.primary_interest_zone.role",
            "primary_interest_zone.role",
            "interest_zone_role",
        )
    )

    direction = _normalize_direction(
        _deep_get(
            payload,
            "direction",
            "trade_direction",
            "metadata.direction",
        )
    )

    htf_bias = _normalize_direction(
        _deep_get(
            payload,
            "htf_bias",
            "metadata.htf_bias",
            "context.htf_bias",
        )
    )

    signal_alignment = _as_upper(
        _deep_get(
            payload,
            "signal_alignment",
            "alignment",
            "metadata.signal_alignment",
            "metadata.alignment",
        )
    )

    execution_status = _as_upper(
        _deep_get(
            payload,
            "execution_status",
            "metadata.execution_status",
            "execution.status",
        )
    )

    practical_rr = _as_float(
        _deep_get(
            payload,
            "practical_rr",
            "rr",
            "risk_reward",
            "metadata.practical_rr",
            "metadata.rr",
            "execution.practical_rr",
        )
    )

    stop_quality = _as_upper(
        _deep_get(
            payload,
            "stop_quality",
            "metadata.stop_quality",
            "execution.stop_quality",
        )
    )

    quality_tier = _as_upper(
        _deep_get(
            payload,
            "quality_tier",
            "quality_level",
            "metadata.quality_tier",
            "metadata.quality_level",
        )
    )

    status = _as_upper(
        _deep_get(
            payload,
            "status",
            "alert_type",
            "signal_class",
        )
    )

    market_state = _as_upper(
        _deep_get(
            payload,
            "market_state",
            "metadata.market_state",
            "context.market_state",
        )
    )

    scenario = _as_upper(
        _deep_get(
            payload,
            "scenario",
            "metadata.scenario",
        )
    )

    nearest_npoc_distance = _as_float(
        _deep_get(
            payload,
            "metadata.auction_context.nearest_npoc_distance",
            "auction_context.nearest_npoc_distance",
            "nearest_npoc_distance",
        )
    )

    ib_extension_up_pct = _as_float(
        _deep_get(
            payload,
            "metadata.auction_context.ib_extension_up_pct",
            "auction_context.ib_extension_up_pct",
            "ib_extension_up_pct",
        )
    )

    ib_extension_down_pct = _as_float(
        _deep_get(
            payload,
            "metadata.auction_context.ib_extension_down_pct",
            "auction_context.ib_extension_down_pct",
            "ib_extension_down_pct",
        )
    )

    accepted_back_inside_value = _as_bool(
        _deep_get(
            payload,
            "metadata.auction_context.accepted_back_inside_value",
            "auction_context.accepted_back_inside_value",
            "accepted_back_inside_value",
        )
    )

    return {
        "payload": payload,
        "market_is_open": market_is_open,
        "market_status": market_status,
        "tpo_signal_permission": tpo_signal_permission,
        "tpo_telegram_modifier": tpo_telegram_modifier,
        "open_relation": open_relation,
        "auction_bias": auction_bias,
        "open_context": open_context,
        "open_behavior": open_behavior,
        "open_behavior_confidence": open_behavior_confidence,
        "entry_model_hint": entry_model_hint,
        "stop_model_hint": stop_model_hint,
        "battle_bias_hint": battle_bias_hint,
        "primary_interest_zone": primary_interest_zone,
        "interest_zone_type": interest_zone_type,
        "interest_zone_price": interest_zone_price,
        "interest_zone_role": interest_zone_role,
        "direction": direction,
        "htf_bias": htf_bias,
        "signal_alignment": signal_alignment,
        "execution_status": execution_status,
        "practical_rr": practical_rr,
        "stop_quality": stop_quality,
        "quality_tier": quality_tier,
        "status": status,
        "market_state": market_state,
        "scenario": scenario,
        "nearest_npoc_distance": nearest_npoc_distance,
        "ib_extension_up_pct": ib_extension_up_pct,
        "ib_extension_down_pct": ib_extension_down_pct,
        "accepted_back_inside_value": accepted_back_inside_value,
    }


def calculate_auction_context_score(inputs: dict[str, Any]) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []

    open_relation = inputs.get("open_relation")
    direction = inputs.get("direction")
    htf_bias = inputs.get("htf_bias")
    nearest_npoc_distance = inputs.get("nearest_npoc_distance")
    ib_extension_up_pct = inputs.get("ib_extension_up_pct")
    ib_extension_down_pct = inputs.get("ib_extension_down_pct")
    accepted_back_inside_value = inputs.get("accepted_back_inside_value")

    if open_relation == "OUT_OF_RANGE":
        score += 2
        reasons.append("open_relation OUT_OF_RANGE: +2")

    elif open_relation == "RANGE":
        score += 1
        reasons.append("open_relation RANGE: +1")

    elif open_relation == "INSIDE_VA":
        score -= 2
        reasons.append("open_relation INSIDE_VA: -2")

    if _direction_matches_htf(direction, htf_bias):
        score += 2
        reasons.append("direction aligned with HTF: +2")
    else:
        reasons.append("direction not aligned with HTF: +0")

    if nearest_npoc_distance is not None:
        score += 1
        reasons.append("nearest nPOC available as interest zone: +1")

    direction_norm = _normalize_direction(direction)

    if direction_norm == "LONG" and ib_extension_up_pct is not None and ib_extension_up_pct >= 0.5:
        score += 1
        reasons.append("IB upside extension >= 0.5 in LONG direction: +1")

    if direction_norm == "SHORT" and ib_extension_down_pct is not None and ib_extension_down_pct >= 0.5:
        score += 1
        reasons.append("IB downside extension >= 0.5 in SHORT direction: +1")

    if accepted_back_inside_value is True:
        score -= 2
        reasons.append("accepted back inside value: -2")

    return score, reasons


def _build_result(
    *,
    inputs: dict[str, Any],
    auction_score: int,
    reasons: list[str],
    blockers: list[str],
    modifiers: list[str],
    battle_permission: str,
    telegram_delivery_mode: str,
    battle_ready: bool,
    v2_policy: dict[str, Any],
) -> BattlePermissionResult:
    return BattlePermissionResult(
        battle_permission=battle_permission,
        telegram_delivery_mode=telegram_delivery_mode,
        battle_ready=battle_ready,
        auction_context_score=auction_score,
        reasons=reasons,
        blockers=blockers,
        modifiers=modifiers,
        market_is_open=inputs.get("market_is_open"),
        market_status=inputs.get("market_status"),
        tpo_signal_permission=inputs.get("tpo_signal_permission"),
        tpo_telegram_modifier=inputs.get("tpo_telegram_modifier"),
        open_relation=inputs.get("open_relation"),
        auction_bias=inputs.get("auction_bias"),
        open_context=inputs.get("open_context"),
        open_behavior=inputs.get("open_behavior"),
        open_behavior_confidence=inputs.get("open_behavior_confidence"),
        entry_model_hint=inputs.get("entry_model_hint"),
        stop_model_hint=inputs.get("stop_model_hint"),
        battle_bias_hint=inputs.get("battle_bias_hint"),
        primary_interest_zone=inputs.get("primary_interest_zone"),
        interest_zone_type=inputs.get("interest_zone_type"),
        interest_zone_price=inputs.get("interest_zone_price"),
        interest_zone_role=inputs.get("interest_zone_role"),
        direction=inputs.get("direction"),
        htf_bias=inputs.get("htf_bias"),
        signal_alignment=inputs.get("signal_alignment"),
        execution_status=inputs.get("execution_status"),
        practical_rr=inputs.get("practical_rr"),
        stop_quality=inputs.get("stop_quality"),
        quality_tier=inputs.get("quality_tier"),
        battle_gate_v2_decision=v2_policy.get("decision"),
        battle_gate_v2_risk_mode=v2_policy.get("risk_mode"),
        battle_gate_v2_battle_allowed=v2_policy.get("battle_allowed"),
        battle_gate_v2_should_suppress_telegram=v2_policy.get("should_suppress_telegram"),
        battle_gate_v2_score_delta=v2_policy.get("score_delta"),
        battle_gate_v2_reasons=list(v2_policy.get("reasons") or []),
        battle_gate_v2_blockers=list(v2_policy.get("blockers") or []),
        battle_gate_v2_modifiers=list(v2_policy.get("modifiers") or []),
        battle_gate_v2_error=v2_policy.get("error"),
    )


def evaluate_battle_permission(raw_payload: dict[str, Any]) -> BattlePermissionResult:
    inputs = extract_battle_inputs(raw_payload)
    auction_score, score_reasons = calculate_auction_context_score(inputs)
    v2_policy = _evaluate_v2_shadow(inputs.get("payload") if isinstance(inputs.get("payload"), dict) else raw_payload)

    reasons: list[str] = list(score_reasons)
    blockers: list[str] = []
    modifiers: list[str] = []

    market_is_open = inputs.get("market_is_open")
    market_status = inputs.get("market_status")
    tpo_signal_permission = inputs.get("tpo_signal_permission")
    tpo_telegram_modifier = inputs.get("tpo_telegram_modifier")
    direction = inputs.get("direction")
    htf_bias = inputs.get("htf_bias")
    signal_alignment = inputs.get("signal_alignment")
    execution_status = inputs.get("execution_status")
    practical_rr = inputs.get("practical_rr")
    stop_quality = inputs.get("stop_quality")
    quality_tier = inputs.get("quality_tier")
    status = inputs.get("status")
    market_state = inputs.get("market_state")
    scenario = inputs.get("scenario")

    v2_neutral_otd_transition_allowed = _v2_allows_neutral_open_test_drive_transition(
        inputs=inputs,
        v2_policy=v2_policy,
    )

    if v2_neutral_otd_transition_allowed:
        modifiers.append("v2_neutral_otd_transition_allowed")
        reasons.append(
            "Battle Gate v2 allows OPEN_TEST_DRIVE with HTF NEUTRAL as a transition candidate; "
            "legacy HTF conflict block will not be applied to this case."
        )

    # 1. Absolute market / data blockers.
    if market_is_open is False or market_status in {"MARKET_CLOSED", "MARKET_CLOSED_AND_STALE"}:
        blockers.append("market_closed")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + ["market is closed; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_MARKET_CLOSED.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    if market_status == "STALE_DATA" or tpo_signal_permission == "STALE_DATA":
        blockers.append("stale_data")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + ["market data is stale; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_STALE_DATA.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    # 2. TPO / auction research blockers.
    if tpo_signal_permission in {"MARKET_CLOSED", "RESEARCH_ONLY", "BLOCKED_BY_CONTEXT", "BLOCKED_BY_AUCTION"}:
        blockers.append(f"tpo_permission_{str(tpo_signal_permission).lower()}")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [f"TPO permission is {tpo_signal_permission}; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    if tpo_telegram_modifier == "DOWNGRADE":
        blockers.append("tpo_downgrade")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + ["TPO telegram modifier is DOWNGRADE; research only"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    # 3. Technical readiness.
    if status not in {"READY", "ENTRY_READY", "EXECUTABLE"}:
        blockers.append("not_ready_status")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [f"status={status}; not a battle-ready signal"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.NOT_READY.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    if execution_status != "EXECUTABLE":
        blockers.append("execution_not_executable")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [f"execution_status={execution_status}; not executable"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_EXECUTION.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    # 4. HTF alignment.
    if not _direction_matches_htf(direction, htf_bias):
        if v2_neutral_otd_transition_allowed:
            modifiers.append("legacy_htf_block_overridden_by_v2_neutral_otd")
            reasons.append(
                f"direction={direction} not aligned with htf_bias={htf_bias}, "
                "but OPEN_TEST_DRIVE + HTF NEUTRAL is treated as a valid transition candidate."
            )
        else:
            blockers.append("direction_not_aligned_with_htf")
            return _build_result(
                inputs=inputs,
                auction_score=auction_score,
                reasons=reasons + [f"direction={direction} not aligned with htf_bias={htf_bias}"],
                blockers=blockers,
                modifiers=modifiers,
                battle_permission=BattlePermission.BLOCKED_BY_HTF.value,
                telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
                battle_ready=False,
                v2_policy=v2_policy,
            )

    if signal_alignment == "COUNTER_TREND":
        if v2_neutral_otd_transition_allowed:
            modifiers.append("legacy_countertrend_label_overridden_by_v2_neutral_otd")
            reasons.append(
                "signal_alignment=COUNTER_TREND ignored for OPEN_TEST_DRIVE + HTF NEUTRAL transition candidate."
            )
        else:
            blockers.append("counter_trend")
            return _build_result(
                inputs=inputs,
                auction_score=auction_score,
                reasons=reasons + ["signal_alignment=COUNTER_TREND; battle signal disabled"],
                blockers=blockers,
                modifiers=modifiers,
                battle_permission=BattlePermission.BLOCKED_BY_HTF.value,
                telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
                battle_ready=False,
                v2_policy=v2_policy,
            )

    # 5. RR / stop / quality.
    if practical_rr is None or practical_rr < 2.0:
        blockers.append("practical_rr_below_2")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [f"practical_rr={practical_rr}; minimum is 2.0"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_RR.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    if stop_quality == "TIGHT_STOP":
        blockers.append("tight_stop")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + ["stop_quality=TIGHT_STOP; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_STOP_QUALITY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    if quality_tier in {"DANGER", "BLOCK", "FAIL"}:
        blockers.append("quality_tier_blocked")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [f"quality_tier={quality_tier}; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_QUALITY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    if quality_tier == "CAUTION" and market_state == "TRANSITION" and scenario in {"SWEEP_RETURN_LONG", "SWEEP_RETURN_SHORT"}:
        blockers.append("caution_transition_sweep_return")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + ["CAUTION + TRANSITION + SWEEP_RETURN; research only"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    # 6. Auction score final gate.
    if auction_score < 3:
        if v2_neutral_otd_transition_allowed:
            modifiers.append("auction_score_override_by_v2_neutral_otd")
            reasons.append(
                f"auction_context_score={auction_score} is below legacy minimum 3, "
                "but Battle Gate v2 allows this OPEN_TEST_DRIVE + HTF NEUTRAL transition candidate."
            )
        else:
            blockers.append("auction_context_score_below_3")
            return _build_result(
                inputs=inputs,
                auction_score=auction_score,
                reasons=reasons + [f"auction_context_score={auction_score}; minimum is 3"],
                blockers=blockers,
                modifiers=modifiers,
                battle_permission=BattlePermission.BLOCKED_BY_AUCTION.value,
                telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
                battle_ready=False,
                v2_policy=v2_policy,
            )

    # 7. Battle ready.
    if tpo_telegram_modifier == "BOOST":
        modifiers.append("tpo_boost")

    return _build_result(
        inputs=inputs,
        auction_score=auction_score,
        reasons=reasons + ["all battle permission checks passed"],
        blockers=blockers,
        modifiers=modifiers,
        battle_permission=BattlePermission.BATTLE_READY.value,
        telegram_delivery_mode=TelegramDeliveryMode.BATTLE_ALERT.value,
        battle_ready=True,
        v2_policy=v2_policy,
    )



def _attach_tpo_open_behavior_fields_to_metadata(metadata: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    metadata["market_is_open"] = result.get("market_is_open")
    metadata["market_status"] = result.get("market_status")
    metadata["tpo_signal_permission"] = result.get("tpo_signal_permission")
    metadata["tpo_telegram_modifier"] = result.get("tpo_telegram_modifier")
    metadata["open_relation"] = result.get("open_relation")
    metadata["auction_bias"] = result.get("auction_bias")

    metadata["open_context"] = result.get("open_context")
    metadata["open_behavior"] = result.get("open_behavior")
    metadata["open_behavior_confidence"] = result.get("open_behavior_confidence")
    metadata["entry_model_hint"] = result.get("entry_model_hint")
    metadata["stop_model_hint"] = result.get("stop_model_hint")
    metadata["battle_bias_hint"] = result.get("battle_bias_hint")
    metadata["primary_interest_zone"] = result.get("primary_interest_zone")
    metadata["interest_zone_type"] = result.get("interest_zone_type")
    metadata["interest_zone_price"] = result.get("interest_zone_price")
    metadata["interest_zone_role"] = result.get("interest_zone_role")

    return metadata


def _attach_v2_shadow_fields_to_metadata(metadata: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    metadata["battle_gate_v2_decision"] = result.get("battle_gate_v2_decision")
    metadata["battle_gate_v2_risk_mode"] = result.get("battle_gate_v2_risk_mode")
    metadata["battle_gate_v2_battle_allowed"] = result.get("battle_gate_v2_battle_allowed")
    metadata["battle_gate_v2_should_suppress_telegram"] = result.get("battle_gate_v2_should_suppress_telegram")
    metadata["battle_gate_v2_score_delta"] = result.get("battle_gate_v2_score_delta")
    metadata["battle_gate_v2_reasons"] = result.get("battle_gate_v2_reasons") or []
    metadata["battle_gate_v2_blockers"] = result.get("battle_gate_v2_blockers") or []
    metadata["battle_gate_v2_modifiers"] = result.get("battle_gate_v2_modifiers") or []
    metadata["battle_gate_v2_error"] = result.get("battle_gate_v2_error")
    return metadata


def apply_battle_permission(raw_payload: dict[str, Any]) -> dict[str, Any]:
    """
    Returns a copy of payload enriched with final battle permission fields.
    Does not mutate the input payload.

    Battle Gate v2 is currently attached in shadow mode:
    - legacy battle_permission / telegram_delivery_mode remain authoritative;
    - v2 fields are added for telemetry/statistics comparison.
    """
    payload = dict(raw_payload)
    result = evaluate_battle_permission(payload).to_dict()

    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    metadata["battle_permission"] = result["battle_permission"]
    metadata["telegram_delivery_mode"] = result["telegram_delivery_mode"]
    metadata["battle_ready"] = result["battle_ready"]
    metadata["auction_context_score"] = result["auction_context_score"]
    metadata["battle_permission_reasons"] = result["reasons"]
    metadata["battle_permission_blockers"] = result["blockers"]
    metadata["battle_permission_modifiers"] = result["modifiers"]
    metadata["battle_permission_version"] = BATTLE_PERMISSION_VERSION

    metadata = _attach_tpo_open_behavior_fields_to_metadata(metadata, result)
    metadata = _attach_v2_shadow_fields_to_metadata(metadata, result)

    payload["metadata"] = metadata
    payload["battle_permission"] = result["battle_permission"]
    payload["telegram_delivery_mode"] = result["telegram_delivery_mode"]
    payload["battle_ready"] = result["battle_ready"]
    payload["auction_context_score"] = result["auction_context_score"]
    payload["battle_permission_version"] = BATTLE_PERMISSION_VERSION

    # Root-level TPO/open-behavior fields are useful for journal, telemetry and flat statistics.
    payload["market_is_open"] = result.get("market_is_open")
    payload["market_status"] = result.get("market_status")
    payload["tpo_signal_permission"] = result.get("tpo_signal_permission")
    payload["tpo_telegram_modifier"] = result.get("tpo_telegram_modifier")
    payload["open_relation"] = result.get("open_relation")
    payload["auction_bias"] = result.get("auction_bias")
    payload["open_context"] = result.get("open_context")
    payload["open_behavior"] = result.get("open_behavior")
    payload["open_behavior_confidence"] = result.get("open_behavior_confidence")
    payload["entry_model_hint"] = result.get("entry_model_hint")
    payload["stop_model_hint"] = result.get("stop_model_hint")
    payload["battle_bias_hint"] = result.get("battle_bias_hint")
    payload["primary_interest_zone"] = result.get("primary_interest_zone")
    payload["interest_zone_type"] = result.get("interest_zone_type")
    payload["interest_zone_price"] = result.get("interest_zone_price")
    payload["interest_zone_role"] = result.get("interest_zone_role")

    # Root-level v2 fields are useful for journal, telemetry and flat statistics.
    payload["battle_gate_v2_decision"] = result.get("battle_gate_v2_decision")
    payload["battle_gate_v2_risk_mode"] = result.get("battle_gate_v2_risk_mode")
    payload["battle_gate_v2_battle_allowed"] = result.get("battle_gate_v2_battle_allowed")
    payload["battle_gate_v2_should_suppress_telegram"] = result.get("battle_gate_v2_should_suppress_telegram")
    payload["battle_gate_v2_score_delta"] = result.get("battle_gate_v2_score_delta")
    payload["battle_gate_v2_reasons"] = result.get("battle_gate_v2_reasons") or []
    payload["battle_gate_v2_blockers"] = result.get("battle_gate_v2_blockers") or []
    payload["battle_gate_v2_modifiers"] = result.get("battle_gate_v2_modifiers") or []
    payload["battle_gate_v2_error"] = result.get("battle_gate_v2_error")

    return payload