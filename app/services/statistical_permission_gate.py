from __future__ import annotations

"""
Statistical Permission Gate for AI Market Analyst.

Version: statistical-permission-gate-v1.0-jeffreys-shadow

Purpose
-------
This module is a conservative risk-desk overlay that decides whether a
structurally valid setup has enough evidence to be allowed into Telegram READY.

It is intentionally independent from Battle Gate and Telegram formatting:
- no network calls;
- no mutation of input payloads;
- no dependency on project storage;
- safe to run in shadow mode first.

Recommended pipeline:
    auction state -> structural permission -> LTF confirmation
    -> battle_permission -> statistical_permission_gate -> Telegram routing

Core idea:
    A setup is evaluated in a granular cell:
        setup × instrument × session × regime/day_type × direction

    READY is allowed only when the cell has:
        - enough closed observations;
        - positive net expectancy after costs/slippage;
        - posterior lower bound above threshold;
        - no macro/event lockout;
        - no late-continuation/chase risk.

The module uses Jeffreys posterior mean for Bernoulli win probability. For the
posterior lower bound it tries SciPy's beta quantile when available. If SciPy is
not installed in production, it falls back to Wilson lower bound and marks the
method explicitly in the output. This keeps the runner dependency-light.
"""

from dataclasses import dataclass, field, asdict
from enum import Enum
import math
from statistics import mean
from typing import Any, Mapping


STATISTICAL_PERMISSION_GATE_VERSION = "statistical-permission-gate-v1.0-jeffreys-shadow"


class StatisticalPermission(str, Enum):
    SUPPRESS = "SUPPRESS"
    WATCHLIST = "WATCHLIST"
    RESEARCH_ONLY = "RESEARCH_ONLY"
    READY = "READY"


class EvidenceTier(str, Enum):
    NO_DATA = "NO_DATA"
    EXPLORATORY = "EXPLORATORY"
    PROVISIONAL = "PROVISIONAL"
    CANDIDATE = "CANDIDATE"
    PRODUCTION = "PRODUCTION"


class StatisticalStatus(str, Enum):
    NO_CELL_STATS = "STAT_NO_CELL_STATS"
    INSUFFICIENT_SAMPLE = "STAT_INSUFFICIENT_SAMPLE"
    RESEARCH_ONLY = "STAT_RESEARCH_ONLY"
    WATCHLIST = "STAT_WATCHLIST"
    CANDIDATE_READY = "STAT_CANDIDATE_READY"
    PRODUCTION_READY = "STAT_PRODUCTION_READY"
    DEGRADED = "STAT_DEGRADED"
    FROZEN = "STAT_FROZEN"


@dataclass(frozen=True)
class StatisticalGateConfig:
    # Evidence tiers.
    exploratory_max_n: int = 29
    provisional_min_n: int = 30
    candidate_min_n: int = 75
    production_min_n: int = 150

    # READY constraints.
    min_ready_closed_trades: int = 75
    min_production_closed_trades: int = 150
    posterior_lower_threshold: float = 0.50
    min_net_expectancy_r: float = 0.0
    min_regime_match_score: float = 0.60

    # Caution / downgrade behavior.
    exploratory_permission: str = StatisticalPermission.RESEARCH_ONLY.value
    provisional_permission: str = StatisticalPermission.WATCHLIST.value
    late_continuation_permission: str = StatisticalPermission.WATCHLIST.value
    macro_lockout_permission: str = StatisticalPermission.WATCHLIST.value

    # Costs used when no explicit cell costs are provided.
    default_costs_r: float = 0.0
    default_slippage_r: float = 0.0

    # Wilson fallback z-score for one-sided 95% lower confidence bound.
    # 1.6448536269514722 is the standard normal 95th percentile.
    one_sided_95_z: float = 1.6448536269514722

    # Field separator for stable cell keys.
    cell_key_separator: str = "|"


DEFAULT_STATISTICAL_GATE_CONFIG = StatisticalGateConfig()


@dataclass
class StatisticalCellStats:
    """Aggregated statistics for one setup × instrument × session × regime cell."""

    wins: int = 0
    losses: int = 0

    # Optional richer payoff information in R.
    avg_win_r: float | None = None
    avg_loss_r: float | None = None
    total_gross_r: float | None = None
    total_net_r: float | None = None
    costs_r: float | None = None
    slippage_r: float | None = None

    # Optional recent/decay diagnostics.
    last_20_net_expectancy_r: float | None = None
    previous_window_posterior_mean: float | None = None
    regime_match_score: float | None = None

    # Optional operational state.
    frozen: bool = False
    frozen_reason: str | None = None
    degraded: bool = False
    degraded_reason: str | None = None

    # Optional raw closed trade outcomes, each item may be:
    # - numeric R value;
    # - mapping with "r", "net_r", "outcome_R", "result_R", "outcome_status".
    closed_trades: list[Any] = field(default_factory=list)

    @property
    def n(self) -> int:
        return max(0, int(self.wins or 0)) + max(0, int(self.losses or 0))


@dataclass
class StatisticalPermissionResult:
    version: str
    cell_key: str
    evidence_tier: str
    closed_trades: int
    wins: int
    losses: int
    raw_winrate: float | None
    posterior_method: str
    posterior_alpha: float | None
    posterior_beta: float | None
    posterior_mean: float | None
    posterior_lower_95: float | None
    avg_win_r: float | None
    avg_loss_r: float | None
    gross_expectancy_r: float | None
    net_expectancy_r: float | None
    costs_r: float
    slippage_r: float
    regime_match_score: float | None
    statistical_permission: str
    statistical_status: str
    statistical_multiplier: float
    allows_ready: bool
    should_suppress_telegram: bool
    reasons: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    modifiers: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _first_non_empty(*values: Any, default: Any = None) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return default


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
        "ok",
        "confirmed",
        "blocked",
        "frozen",
    }


def _normalize_token(value: Any, *, default: str = "UNKNOWN") -> str:
    raw = str(value if value is not None else "").strip().upper()
    if not raw:
        raw = default
    return (
        raw.replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("__", "_")
    )


def build_statistical_cell_key(
    payload: Mapping[str, Any] | None,
    *,
    config: StatisticalGateConfig = DEFAULT_STATISTICAL_GATE_CONFIG,
) -> str:
    """Build stable setup × instrument × session × regime/day-type × direction key."""

    payload = payload or {}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}

    setup = _first_non_empty(
        payload.get("auction_ltf_setup"),
        metadata.get("auction_ltf_setup"),
        payload.get("tpo_watch_setup"),
        metadata.get("tpo_watch_setup"),
        payload.get("scenario"),
        payload.get("scenario_type"),
        payload.get("setup_type"),
        default="UNKNOWN_SETUP",
    )
    symbol = _first_non_empty(
        payload.get("symbol"),
        payload.get("instrument"),
        metadata.get("symbol"),
        metadata.get("instrument"),
        default="UNKNOWN_SYMBOL",
    )
    session = _first_non_empty(
        payload.get("session"),
        payload.get("market_session"),
        payload.get("session_label"),
        payload.get("batch_group"),
        metadata.get("session"),
        metadata.get("market_session"),
        metadata.get("session_label"),
        metadata.get("batch_group"),
        default="UNKNOWN_SESSION",
    )
    day_type = _first_non_empty(
        payload.get("day_type"),
        payload.get("day_type_candidate"),
        payload.get("auction_day_type"),
        metadata.get("day_type"),
        metadata.get("day_type_candidate"),
        metadata.get("auction_day_type"),
        default="UNKNOWN_DAY_TYPE",
    )
    regime = _first_non_empty(
        payload.get("regime"),
        payload.get("market_regime"),
        payload.get("macro_regime"),
        payload.get("vol_regime"),
        metadata.get("regime"),
        metadata.get("market_regime"),
        metadata.get("macro_regime"),
        metadata.get("vol_regime"),
        day_type,
        default="UNKNOWN_REGIME",
    )
    direction = _first_non_empty(
        payload.get("direction"),
        payload.get("side"),
        metadata.get("direction"),
        metadata.get("side"),
        default="UNKNOWN_DIRECTION",
    )

    parts = [
        _normalize_token(setup),
        _normalize_token(symbol),
        _normalize_token(session),
        _normalize_token(regime),
        _normalize_token(day_type),
        _normalize_token(direction),
    ]
    return config.cell_key_separator.join(parts)


def _extract_r_from_trade(trade: Any) -> float | None:
    if isinstance(trade, Mapping):
        value = _first_non_empty(
            trade.get("net_r"),
            trade.get("outcome_R"),
            trade.get("result_R"),
            trade.get("r"),
            trade.get("R"),
            trade.get("profit_R"),
        )
        f = _as_float(value)
        if f is not None:
            return f

        status = str(trade.get("outcome_status") or trade.get("status") or "").upper()
        if status in {"TP_HIT", "WIN", "PROFIT"}:
            return 1.0
        if status in {"SL_HIT", "LOSS", "STOP"}:
            return -1.0
        return None

    return _as_float(trade)


def _infer_wins_losses_from_trades(trades: list[Any]) -> tuple[int, int, list[float]]:
    values: list[float] = []
    wins = 0
    losses = 0
    for trade in trades or []:
        r = _extract_r_from_trade(trade)
        if r is None:
            continue
        values.append(float(r))
        if r > 0:
            wins += 1
        elif r < 0:
            losses += 1
    return wins, losses, values


def parse_cell_stats(raw: Mapping[str, Any] | StatisticalCellStats | None) -> StatisticalCellStats | None:
    """Normalize project-specific statistics payloads into StatisticalCellStats."""

    if raw is None:
        return None
    if isinstance(raw, StatisticalCellStats):
        return raw
    if not isinstance(raw, Mapping):
        return None

    closed_trades = raw.get("closed_trades") or raw.get("trades") or raw.get("outcomes") or []
    if not isinstance(closed_trades, list):
        closed_trades = []

    inferred_wins, inferred_losses, r_values = _infer_wins_losses_from_trades(closed_trades)

    wins = int(_as_float(_first_non_empty(raw.get("wins"), raw.get("tp_hits"), raw.get("tp"), default=0)) or 0)
    losses = int(_as_float(_first_non_empty(raw.get("losses"), raw.get("sl_hits"), raw.get("sl"), default=0)) or 0)

    if wins == 0 and losses == 0 and (inferred_wins or inferred_losses):
        wins, losses = inferred_wins, inferred_losses

    avg_win_r = _as_float(_first_non_empty(raw.get("avg_win_r"), raw.get("avg_win_R")))
    avg_loss_r = _as_float(_first_non_empty(raw.get("avg_loss_r"), raw.get("avg_loss_R")))
    total_net_r = _as_float(_first_non_empty(raw.get("total_net_r"), raw.get("net_R"), raw.get("net_r")))
    total_gross_r = _as_float(_first_non_empty(raw.get("total_gross_r"), raw.get("gross_R"), raw.get("gross_r")))

    if r_values:
        wins_r = [x for x in r_values if x > 0]
        losses_r = [-x for x in r_values if x < 0]
        if avg_win_r is None and wins_r:
            avg_win_r = mean(wins_r)
        if avg_loss_r is None and losses_r:
            avg_loss_r = mean(losses_r)
        if total_net_r is None:
            total_net_r = sum(r_values)

    return StatisticalCellStats(
        wins=max(0, wins),
        losses=max(0, losses),
        avg_win_r=avg_win_r,
        avg_loss_r=avg_loss_r,
        total_gross_r=total_gross_r,
        total_net_r=total_net_r,
        costs_r=_as_float(raw.get("costs_r")),
        slippage_r=_as_float(raw.get("slippage_r")),
        last_20_net_expectancy_r=_as_float(raw.get("last_20_net_expectancy_r")),
        previous_window_posterior_mean=_as_float(raw.get("previous_window_posterior_mean")),
        regime_match_score=_as_float(raw.get("regime_match_score")),
        frozen=_as_bool(raw.get("frozen")),
        frozen_reason=str(raw.get("frozen_reason") or "") or None,
        degraded=_as_bool(raw.get("degraded")),
        degraded_reason=str(raw.get("degraded_reason") or "") or None,
        closed_trades=closed_trades,
    )


def evidence_tier_for_n(n: int, config: StatisticalGateConfig = DEFAULT_STATISTICAL_GATE_CONFIG) -> str:
    if n <= 0:
        return EvidenceTier.NO_DATA.value
    if n <= config.exploratory_max_n:
        return EvidenceTier.EXPLORATORY.value
    if n < config.candidate_min_n:
        return EvidenceTier.PROVISIONAL.value
    if n < config.production_min_n:
        return EvidenceTier.CANDIDATE.value
    return EvidenceTier.PRODUCTION.value


def jeffreys_posterior(wins: int, losses: int) -> tuple[float, float, float | None]:
    """Return alpha, beta, posterior_mean for Jeffreys Beta(0.5, 0.5)."""

    n = max(0, int(wins or 0)) + max(0, int(losses or 0))
    if n <= 0:
        return 0.5, 0.5, None
    alpha = max(0, int(wins or 0)) + 0.5
    beta = max(0, int(losses or 0)) + 0.5
    mean_value = alpha / (alpha + beta)
    return alpha, beta, mean_value


def wilson_lower_bound(
    wins: int,
    n: int,
    *,
    z: float = DEFAULT_STATISTICAL_GATE_CONFIG.one_sided_95_z,
) -> float | None:
    """One-sided Wilson lower confidence bound for a binomial proportion."""

    if n <= 0:
        return None
    p = wins / n
    z2 = z * z
    denom = 1.0 + z2 / n
    center = p + z2 / (2.0 * n)
    margin = z * math.sqrt((p * (1.0 - p) / n) + (z2 / (4.0 * n * n)))
    return max(0.0, min(1.0, (center - margin) / denom))


def posterior_lower_95(
    wins: int,
    losses: int,
    *,
    config: StatisticalGateConfig = DEFAULT_STATISTICAL_GATE_CONFIG,
) -> tuple[float | None, str]:
    """Return one-sided 95% lower bound.

    Preference:
    - Jeffreys posterior lower 5% quantile via scipy.stats.beta.ppf;
    - Wilson one-sided 95% lower bound if SciPy is unavailable.
    """

    n = max(0, int(wins or 0)) + max(0, int(losses or 0))
    if n <= 0:
        return None, "none_no_data"

    alpha, beta, _ = jeffreys_posterior(wins, losses)

    try:
        # Optional dependency. Do not require SciPy in production.
        from scipy.stats import beta as scipy_beta  # type: ignore

        return float(scipy_beta.ppf(0.05, alpha, beta)), "jeffreys_beta_scipy_ppf_0.05"
    except Exception:
        return wilson_lower_bound(wins, n, z=config.one_sided_95_z), "wilson_one_sided_95_fallback"


def _payoff_metrics(
    stats: StatisticalCellStats,
    *,
    config: StatisticalGateConfig,
) -> tuple[float | None, float | None, float | None, float | None, float, float]:
    n = stats.n
    if n <= 0:
        return None, None, None, None, config.default_costs_r, config.default_slippage_r

    winrate = stats.wins / n
    lossrate = stats.losses / n

    avg_win_r = stats.avg_win_r if stats.avg_win_r is not None else 1.0
    avg_loss_r = stats.avg_loss_r if stats.avg_loss_r is not None else 1.0

    gross_expectancy = winrate * avg_win_r - lossrate * avg_loss_r

    costs_r = stats.costs_r if stats.costs_r is not None else config.default_costs_r
    slippage_r = stats.slippage_r if stats.slippage_r is not None else config.default_slippage_r

    if stats.total_net_r is not None:
        net_expectancy = stats.total_net_r / n
    else:
        net_expectancy = gross_expectancy - costs_r - slippage_r

    return avg_win_r, avg_loss_r, gross_expectancy, net_expectancy, costs_r, slippage_r


def _is_macro_lockout(payload: Mapping[str, Any] | None) -> bool:
    if not isinstance(payload, Mapping):
        return False

    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
    flags = payload.get("flags") or payload.get("caution_flags") or payload.get("macro_caution_flags") or []
    if not isinstance(flags, (list, tuple, set)):
        flags = [flags]

    haystack = " ".join(
        str(x or "")
        for x in list(flags)
        + [
            payload.get("macro_lockout_flag"),
            payload.get("macro_lockout"),
            payload.get("event_lockout"),
            payload.get("macro_risk_mode"),
            payload.get("risk_mode"),
            payload.get("post_news_regime"),
            metadata.get("macro_lockout_flag"),
            metadata.get("macro_lockout"),
            metadata.get("event_lockout"),
            metadata.get("macro_risk_mode"),
            metadata.get("risk_mode"),
        ]
    ).upper()

    return any(
        token in haystack
        for token in (
            "MACRO_LOCKOUT",
            "EVENT_LOCKOUT",
            "HIGH_IMPACT_LOCKOUT",
            "PRE_NEWS_LOCKOUT",
            "POST_NEWS_LOCKOUT",
            "MACRO_RISK_POST_SHOCK_CAUTION",
        )
    )


def _is_late_continuation(payload: Mapping[str, Any] | None) -> bool:
    if not isinstance(payload, Mapping):
        return False
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}

    flags = payload.get("flags") or payload.get("caution_flags") or []
    if not isinstance(flags, (list, tuple, set)):
        flags = [flags]

    haystack = " ".join(
        str(x or "")
        for x in list(flags)
        + [
            payload.get("late_continuation"),
            payload.get("late_entry_risk"),
            payload.get("entry_timing_status"),
            payload.get("late_signal_reason"),
            payload.get("continuation_day_guard"),
            payload.get("continuation_day_reason"),
            payload.get("trigger_reason"),
            metadata.get("late_continuation"),
            metadata.get("late_entry_risk"),
            metadata.get("entry_timing_status"),
            metadata.get("continuation_day_guard"),
            metadata.get("continuation_day_reason"),
        ]
    ).upper()

    return any(
        token in haystack
        for token in (
            "LATE_CONTINUATION",
            "LATE_ENTRY_RISK",
            "LATE_SIGNAL",
            "HARD_LATE_SIGNAL",
            "CONTINUATION_DAY",
            "FIRST_IMPULSE_ALREADY_GONE",
            "ENTRY_WINDOW_GONE",
            "PRICE_ALREADY_MOVED",
        )
    )


def _statistical_multiplier(
    *,
    evidence_tier: str,
    posterior_lower: float | None,
    net_expectancy_r: float | None,
    config: StatisticalGateConfig,
    macro_lockout: bool,
    late_continuation: bool,
    degraded: bool,
    frozen: bool,
) -> float:
    if frozen:
        return 0.0
    if posterior_lower is None or net_expectancy_r is None:
        return 0.0
    if net_expectancy_r <= config.min_net_expectancy_r:
        return 0.0
    if posterior_lower <= config.posterior_lower_threshold:
        return 0.0
    if macro_lockout:
        return 0.25
    if late_continuation:
        return 0.35
    if degraded:
        return 0.40

    tier_base = {
        EvidenceTier.NO_DATA.value: 0.0,
        EvidenceTier.EXPLORATORY.value: 0.20,
        EvidenceTier.PROVISIONAL.value: 0.45,
        EvidenceTier.CANDIDATE.value: 0.75,
        EvidenceTier.PRODUCTION.value: 1.00,
    }.get(evidence_tier, 0.0)

    # More evidence above the threshold gives a modest boost, capped by tier.
    edge = max(0.0, posterior_lower - config.posterior_lower_threshold)
    boost = min(0.15, edge * 1.5)
    return max(0.0, min(1.0, tier_base + boost))


def evaluate_statistical_permission(
    payload: Mapping[str, Any] | None,
    cell_stats: Mapping[str, Any] | StatisticalCellStats | None,
    *,
    config: StatisticalGateConfig = DEFAULT_STATISTICAL_GATE_CONFIG,
    cell_key: str | None = None,
) -> StatisticalPermissionResult:
    """Evaluate whether a structural setup has statistical permission for READY."""

    payload = payload or {}
    stats = parse_cell_stats(cell_stats)
    key = cell_key or build_statistical_cell_key(payload, config=config)

    if stats is None:
        return StatisticalPermissionResult(
            version=STATISTICAL_PERMISSION_GATE_VERSION,
            cell_key=key,
            evidence_tier=EvidenceTier.NO_DATA.value,
            closed_trades=0,
            wins=0,
            losses=0,
            raw_winrate=None,
            posterior_method="none_no_cell_stats",
            posterior_alpha=None,
            posterior_beta=None,
            posterior_mean=None,
            posterior_lower_95=None,
            avg_win_r=None,
            avg_loss_r=None,
            gross_expectancy_r=None,
            net_expectancy_r=None,
            costs_r=config.default_costs_r,
            slippage_r=config.default_slippage_r,
            regime_match_score=None,
            statistical_permission=StatisticalPermission.RESEARCH_ONLY.value,
            statistical_status=StatisticalStatus.NO_CELL_STATS.value,
            statistical_multiplier=0.0,
            allows_ready=False,
            should_suppress_telegram=False,
            reasons=["no statistical cell stats available"],
            blockers=["stat_no_cell_stats"],
            modifiers=["shadow_safe_default_research_only"],
        )

    n = stats.n
    tier = evidence_tier_for_n(n, config=config)
    raw_winrate = (stats.wins / n) if n > 0 else None

    alpha, beta, posterior_mean = jeffreys_posterior(stats.wins, stats.losses)
    lower, lower_method = posterior_lower_95(stats.wins, stats.losses, config=config)

    avg_win_r, avg_loss_r, gross_exp, net_exp, costs_r, slippage_r = _payoff_metrics(
        stats,
        config=config,
    )

    regime_match = stats.regime_match_score
    if regime_match is None and isinstance(payload, Mapping):
        regime_match = _as_float(payload.get("regime_match_score"))

    macro_lockout = _is_macro_lockout(payload)
    late_continuation = _is_late_continuation(payload)

    blockers: list[str] = []
    reasons: list[str] = []
    modifiers: list[str] = []

    if stats.frozen:
        blockers.append("stat_cell_frozen")
        reasons.append(stats.frozen_reason or "statistical cell is frozen")
    if stats.degraded:
        modifiers.append("stat_cell_degraded")
        reasons.append(stats.degraded_reason or "statistical cell is degraded")

    if n <= 0:
        blockers.append("stat_no_closed_trades")
        reasons.append("no closed trades in statistical cell")
    elif n <= config.exploratory_max_n:
        blockers.append("stat_exploratory_sample")
        reasons.append(f"exploratory sample n={n}; READY disabled")
    elif n < config.min_ready_closed_trades:
        blockers.append("stat_below_ready_sample")
        reasons.append(f"closed trades n={n} below READY minimum {config.min_ready_closed_trades}")

    if lower is None:
        blockers.append("stat_missing_posterior_lower")
    elif lower <= config.posterior_lower_threshold:
        blockers.append("posterior_lower_below_threshold")
        reasons.append(
            f"posterior lower 95%={lower:.4f} <= threshold {config.posterior_lower_threshold:.4f}"
        )

    if net_exp is None:
        blockers.append("stat_missing_expectancy")
    elif net_exp <= config.min_net_expectancy_r:
        blockers.append("net_expectancy_not_positive")
        reasons.append(f"net expectancy {net_exp:.4f}R <= {config.min_net_expectancy_r:.4f}R")

    if regime_match is not None and regime_match < config.min_regime_match_score:
        blockers.append("regime_match_too_low")
        reasons.append(
            f"regime match {regime_match:.2f} < {config.min_regime_match_score:.2f}"
        )

    if macro_lockout:
        blockers.append("macro_event_lockout")
        modifiers.append("macro_lockout_watchlist")
        reasons.append("macro/event lockout active")

    if late_continuation:
        blockers.append("late_continuation_risk")
        modifiers.append("late_continuation_watchlist")
        reasons.append("late continuation or chase risk detected")

    if stats.last_20_net_expectancy_r is not None and stats.last_20_net_expectancy_r < 0:
        modifiers.append("recent_window_negative_expectancy")
        reasons.append(f"last_20_net_expectancy={stats.last_20_net_expectancy_r:.4f}R")

    if (
        stats.previous_window_posterior_mean is not None
        and posterior_mean is not None
        and posterior_mean < stats.previous_window_posterior_mean
    ):
        modifiers.append("posterior_mean_deteriorating")

    multiplier = _statistical_multiplier(
        evidence_tier=tier,
        posterior_lower=lower,
        net_expectancy_r=net_exp,
        config=config,
        macro_lockout=macro_lockout,
        late_continuation=late_continuation,
        degraded=stats.degraded,
        frozen=stats.frozen,
    )

    allows_ready = False
    should_suppress = False
    permission = StatisticalPermission.RESEARCH_ONLY.value
    status = StatisticalStatus.RESEARCH_ONLY.value

    # Hard frozen/degraded or toxic stats.
    if stats.frozen:
        permission = StatisticalPermission.SUPPRESS.value
        status = StatisticalStatus.FROZEN.value
        should_suppress = True
    elif n <= config.exploratory_max_n:
        permission = config.exploratory_permission
        status = StatisticalStatus.INSUFFICIENT_SAMPLE.value
    elif macro_lockout:
        permission = config.macro_lockout_permission
        status = StatisticalStatus.WATCHLIST.value
    elif late_continuation:
        permission = config.late_continuation_permission
        status = StatisticalStatus.WATCHLIST.value
    elif stats.degraded:
        permission = StatisticalPermission.RESEARCH_ONLY.value
        status = StatisticalStatus.DEGRADED.value
    elif blockers:
        permission = StatisticalPermission.RESEARCH_ONLY.value
        status = StatisticalStatus.RESEARCH_ONLY.value
    elif tier == EvidenceTier.PROVISIONAL.value:
        permission = config.provisional_permission
        status = StatisticalStatus.WATCHLIST.value
    elif n >= config.min_ready_closed_trades:
        permission = StatisticalPermission.READY.value
        allows_ready = True
        if n >= config.min_production_closed_trades:
            status = StatisticalStatus.PRODUCTION_READY.value
        else:
            status = StatisticalStatus.CANDIDATE_READY.value
    else:
        permission = StatisticalPermission.RESEARCH_ONLY.value
        status = StatisticalStatus.RESEARCH_ONLY.value

    if allows_ready:
        reasons.append("statistical gate allows READY for this cell")
    elif not reasons:
        reasons.append("statistical gate does not allow READY")

    return StatisticalPermissionResult(
        version=STATISTICAL_PERMISSION_GATE_VERSION,
        cell_key=key,
        evidence_tier=tier,
        closed_trades=n,
        wins=stats.wins,
        losses=stats.losses,
        raw_winrate=round(raw_winrate, 6) if raw_winrate is not None else None,
        posterior_method=lower_method,
        posterior_alpha=round(alpha, 6) if n > 0 else None,
        posterior_beta=round(beta, 6) if n > 0 else None,
        posterior_mean=round(posterior_mean, 6) if posterior_mean is not None else None,
        posterior_lower_95=round(lower, 6) if lower is not None else None,
        avg_win_r=round(avg_win_r, 6) if avg_win_r is not None else None,
        avg_loss_r=round(avg_loss_r, 6) if avg_loss_r is not None else None,
        gross_expectancy_r=round(gross_exp, 6) if gross_exp is not None else None,
        net_expectancy_r=round(net_exp, 6) if net_exp is not None else None,
        costs_r=round(costs_r, 6),
        slippage_r=round(slippage_r, 6),
        regime_match_score=round(regime_match, 6) if regime_match is not None else None,
        statistical_permission=permission,
        statistical_status=status,
        statistical_multiplier=round(multiplier, 6),
        allows_ready=allows_ready,
        should_suppress_telegram=should_suppress,
        reasons=reasons,
        blockers=blockers,
        modifiers=modifiers,
    )


def attach_statistical_permission_fields(
    payload: Mapping[str, Any] | None,
    cell_stats: Mapping[str, Any] | StatisticalCellStats | None,
    *,
    config: StatisticalGateConfig = DEFAULT_STATISTICAL_GATE_CONFIG,
    cell_key: str | None = None,
    shadow_mode: bool = True,
) -> dict[str, Any]:
    """Return a copy of payload enriched with statistical permission fields.

    In shadow mode the function does not override Telegram delivery fields.
    It only attaches diagnostics for telemetry/statistics.

    In enforcing mode it may downgrade Telegram-ready payloads unless
    statistical permission allows READY.
    """

    enriched = dict(payload or {})
    result = evaluate_statistical_permission(enriched, cell_stats, config=config, cell_key=cell_key)
    result_dict = result.to_dict()

    metadata = enriched.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    for key, value in result_dict.items():
        enriched[f"statistical_{key}" if key in {"version"} else key] = value

    # Root-level concise aliases.
    enriched["statistical_permission_gate_version"] = result.version
    enriched["statistical_cell_key"] = result.cell_key
    enriched["statistical_evidence_tier"] = result.evidence_tier
    enriched["statistical_permission"] = result.statistical_permission
    enriched["statistical_status"] = result.statistical_status
    enriched["statistical_multiplier"] = result.statistical_multiplier
    enriched["statistical_allows_ready"] = result.allows_ready
    enriched["statistical_blockers"] = result.blockers
    enriched["statistical_reasons"] = result.reasons
    enriched["posterior_lower_95"] = result.posterior_lower_95
    enriched["posterior_mean"] = result.posterior_mean
    enriched["net_expectancy_r"] = result.net_expectancy_r

    metadata.update(
        {
            "statistical_permission_gate_version": result.version,
            "statistical_cell_key": result.cell_key,
            "statistical_evidence_tier": result.evidence_tier,
            "statistical_permission": result.statistical_permission,
            "statistical_status": result.statistical_status,
            "statistical_multiplier": result.statistical_multiplier,
            "statistical_allows_ready": result.allows_ready,
            "statistical_blockers": result.blockers,
            "statistical_reasons": result.reasons,
            "posterior_lower_95": result.posterior_lower_95,
            "posterior_mean": result.posterior_mean,
            "net_expectancy_r": result.net_expectancy_r,
            "statistical_shadow_mode": bool(shadow_mode),
        }
    )

    if not shadow_mode and not result.allows_ready:
        # Never promote; only downgrade.
        current_delivery = str(enriched.get("telegram_delivery_mode") or "").upper()
        current_battle_ready = _as_bool(enriched.get("battle_ready"))
        if current_delivery in {"BATTLE_ALERT", "READY"} or current_battle_ready:
            enriched["telegram_delivery_mode_before_statistical_gate"] = enriched.get("telegram_delivery_mode")
            enriched["battle_ready_before_statistical_gate"] = enriched.get("battle_ready")
            enriched["telegram_delivery_mode"] = (
                StatisticalPermission.WATCHLIST.value
                if result.statistical_permission == StatisticalPermission.WATCHLIST.value
                else "RESEARCH_ALERT"
            )
            enriched["battle_ready"] = False
            enriched["statistical_gate_downgraded"] = True

    enriched["metadata"] = metadata
    return enriched


__all__ = [
    "STATISTICAL_PERMISSION_GATE_VERSION",
    "StatisticalPermission",
    "EvidenceTier",
    "StatisticalStatus",
    "StatisticalGateConfig",
    "DEFAULT_STATISTICAL_GATE_CONFIG",
    "StatisticalCellStats",
    "StatisticalPermissionResult",
    "build_statistical_cell_key",
    "parse_cell_stats",
    "evidence_tier_for_n",
    "jeffreys_posterior",
    "posterior_lower_95",
    "evaluate_statistical_permission",
    "attach_statistical_permission_fields",
]
