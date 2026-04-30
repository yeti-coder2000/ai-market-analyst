from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# =============================================================================
# PARSING HELPERS
# =============================================================================

def _to_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _to_int(value: str | None, default: int) -> int:
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Expected integer value, got: {value!r}") from exc


def _to_float(value: str | None, default: float) -> float:
    if value is None or value.strip() == "":
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Expected float value, got: {value!r}") from exc


def _clean_str(value: str | None, default: str = "") -> str:
    if value is None:
        return default
    return value.strip()


def _expand_path(value: str | None, default: str) -> Path:
    raw = value.strip() if value else default
    return Path(raw).expanduser().resolve()


def _is_render_runtime() -> bool:
    """
    Render exposes service-specific environment variables during runtime.
    We use this only for safe path defaults; explicit env paths still win.
    """
    return any(
        bool(os.getenv(name))
        for name in (
            "RENDER",
            "RENDER_SERVICE_ID",
            "RENDER_SERVICE_NAME",
            "RENDER_EXTERNAL_HOSTNAME",
        )
    )


def _is_production_env(app_env: str) -> bool:
    return str(app_env or "").strip().lower() in {"prod", "production"}


def _default_data_root(project_root: Path, app_env: str) -> Path:
    """
    Runtime/cache/logs root.

    Priority:
    1. DATA_DIR / PERSISTENT_DATA_DIR env override.
    2. Render/production default: /var/data.
    3. Local development default: project root.

    Why: Render deploy directory /opt/render/project/src is ephemeral. For a
    4-week statistics window we want runtime artifacts on the persistent disk.
    """
    explicit = _clean_str(
        os.getenv("DATA_DIR") or os.getenv("PERSISTENT_DATA_DIR"),
        "",
    )
    if explicit:
        return Path(explicit).expanduser().resolve()

    if _is_production_env(app_env) or _is_render_runtime():
        return Path("/var/data").resolve()

    return project_root


def _available_batch_groups() -> set[str]:
    """
    Read supported batch groups from app.core.instrument_batches.

    This prevents settings.py from becoming a stale hardcoded gate every time we
    add a new batch group such as fx_major or indices.
    """
    try:
        from app.core.instrument_batches import list_available_batches

        groups = {
            str(item).strip().lower()
            for item in list_available_batches()
            if str(item).strip()
        }
        if groups:
            return groups
    except Exception:
        pass

    # Safe fallback for early imports / transitional states.
    return {"core", "fx_major", "indices"}


# =============================================================================
# MAIN SETTINGS MODEL
# =============================================================================

@dataclass(frozen=True)
class AppSettings:
    # -------------------------------------------------------------------------
    # Application
    # -------------------------------------------------------------------------
    app_name: str
    app_env: str
    log_level: str
    timezone: str

    # -------------------------------------------------------------------------
    # Runtime / scheduler
    # -------------------------------------------------------------------------
    run_interval_sec: int
    cycle_timeout_sec: int
    startup_grace_sec: int
    enable_weekend_skip: bool
    enable_telegram: bool
    enable_heartbeat: bool
    enable_alert_deduper: bool
    watch_alerts_enabled: bool
    main_worker_alert_forwarding_enabled: bool
    paper_mode: bool
    fail_fast: bool

    # -------------------------------------------------------------------------
    # Paths
    # -------------------------------------------------------------------------
    project_root: Path
    data_dir: Path
    runtime_dir: Path
    cache_dir: Path
    logs_dir: Path

    runner_state_path: Path
    radar_journal_path: Path
    alerts_state_path: Path
    heartbeat_path: Path

    # -------------------------------------------------------------------------
    # Data provider
    # -------------------------------------------------------------------------
    twelvedata_api_key: str
    provider_name: str
    provider_timeout_sec: int
    provider_max_retries: int
    provider_retry_backoff_sec: float

    # -------------------------------------------------------------------------
    # Telegram
    # -------------------------------------------------------------------------
    telegram_bot_token: str
    telegram_chat_id: str
    telegram_parse_mode: str
    telegram_silent: bool

    # -------------------------------------------------------------------------
    # Alert / scenario controls
    # -------------------------------------------------------------------------
    scenario_probability_threshold: float
    strong_watch_probability_threshold: float
    alert_cooldown_sec: int

    # -------------------------------------------------------------------------
    # Universe / runtime control
    # -------------------------------------------------------------------------
    default_batch_size: int
    force_batch: Optional[int]
    auto_mode: bool
    simulation_mode: bool
    batch_group: str
    enabled_symbols_raw: str

    @property
    def is_production(self) -> bool:
        return _is_production_env(self.app_env)

    @property
    def enabled_symbols(self) -> list[str]:
        if not self.enabled_symbols_raw:
            return []
        return [
            item.strip().upper()
            for item in self.enabled_symbols_raw.split(",")
            if item.strip()
        ]

    def validate(self) -> None:
        """
        Keep validation safe at import time.

        IMPORTANT:
        Do not require provider credentials here, because many legacy modules
        import app.core.settings only for constants/classes and should not crash
        before runtime.
        """
        errors: list[str] = []

        if self.enable_telegram:
            if not self.telegram_bot_token:
                errors.append("TELEGRAM_BOT_TOKEN is required when ENABLE_TELEGRAM=true.")
            if not self.telegram_chat_id:
                errors.append("TELEGRAM_CHAT_ID is required when ENABLE_TELEGRAM=true.")

        if self.run_interval_sec <= 0:
            errors.append("RUN_INTERVAL_SEC must be > 0.")

        if self.cycle_timeout_sec <= 0:
            errors.append("CYCLE_TIMEOUT_SEC must be > 0.")

        if not (0.0 <= self.scenario_probability_threshold <= 1.0):
            errors.append("SCENARIO_PROBABILITY_THRESHOLD must be between 0.0 and 1.0.")

        if not (0.0 <= self.strong_watch_probability_threshold <= 1.0):
            errors.append("STRONG_WATCH_PROBABILITY_THRESHOLD must be between 0.0 and 1.0.")

        if self.strong_watch_probability_threshold < self.scenario_probability_threshold:
            errors.append(
                "STRONG_WATCH_PROBABILITY_THRESHOLD must be >= SCENARIO_PROBABILITY_THRESHOLD."
            )

        if self.default_batch_size <= 0:
            errors.append("DEFAULT_BATCH_SIZE must be > 0.")

        if self.provider_timeout_sec <= 0:
            errors.append("PROVIDER_TIMEOUT_SEC must be > 0.")

        if self.provider_max_retries < 0:
            errors.append("PROVIDER_MAX_RETRIES must be >= 0.")

        if self.provider_retry_backoff_sec < 0:
            errors.append("PROVIDER_RETRY_BACKOFF_SEC must be >= 0.")

        allowed_batch_groups = _available_batch_groups()
        if self.batch_group not in allowed_batch_groups:
            errors.append(
                "BATCH_GROUP must be one of: "
                + ", ".join(sorted(allowed_batch_groups))
                + "."
            )

        # In production/Render we deliberately default to /var/data. If the
        # persistent disk is not mounted, directory creation will fail loudly in
        # ensure_directories(), which is better than silently losing statistics
        # on every deploy.
        if self.is_production or _is_render_runtime():
            if str(self.runtime_dir).startswith(str(self.project_root / "runtime")):
                errors.append(
                    "Production runtime_dir points to project runtime. "
                    "Set RUNTIME_DIR=/var/data/runtime or use the default /var/data runtime root."
                )

        if errors:
            raise ValueError("Invalid application settings:\n- " + "\n- ".join(errors))

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        self.runner_state_path.parent.mkdir(parents=True, exist_ok=True)
        self.radar_journal_path.parent.mkdir(parents=True, exist_ok=True)
        self.alerts_state_path.parent.mkdir(parents=True, exist_ok=True)
        self.heartbeat_path.parent.mkdir(parents=True, exist_ok=True)


# =============================================================================
# SETTINGS LOADER
# =============================================================================

def load_settings() -> AppSettings:
    project_root = _expand_path(os.getenv("PROJECT_ROOT"), ".")

    app_env = _clean_str(os.getenv("APP_ENV"), "development")
    data_dir = _default_data_root(project_root=project_root, app_env=app_env)

    runtime_dir = _expand_path(os.getenv("RUNTIME_DIR"), str(data_dir / "runtime"))
    cache_dir = _expand_path(os.getenv("CACHE_DIR"), str(data_dir / "cache"))
    logs_dir = _expand_path(os.getenv("LOGS_DIR"), str(data_dir / "logs"))

    runner_state_path = _expand_path(
        os.getenv("RUNNER_STATE_PATH"),
        str(runtime_dir / "runner_state.json"),
    )
    radar_journal_path = _expand_path(
        os.getenv("RADAR_JOURNAL_PATH"),
        str(runtime_dir / "radar_journal.ndjson"),
    )
    alerts_state_path = _expand_path(
        os.getenv("ALERTS_STATE_PATH"),
        str(runtime_dir / "alerts_state.json"),
    )
    heartbeat_path = _expand_path(
        os.getenv("HEARTBEAT_PATH"),
        str(runtime_dir / "heartbeat.json"),
    )

    force_batch_raw = _clean_str(os.getenv("FORCE_BATCH"), "")
    force_batch = int(force_batch_raw) if force_batch_raw else None

    app_settings = AppSettings(
        # Application
        app_name=_clean_str(os.getenv("APP_NAME"), "ai_market_analyst"),
        app_env=app_env,
        log_level=_clean_str(os.getenv("LOG_LEVEL"), "INFO").upper(),
        timezone=_clean_str(os.getenv("TIMEZONE"), "Europe/Kiev"),

        # Runtime / scheduler
        run_interval_sec=_to_int(os.getenv("RUN_INTERVAL_SEC"), 900),
        cycle_timeout_sec=_to_int(os.getenv("CYCLE_TIMEOUT_SEC"), 840),
        startup_grace_sec=_to_int(os.getenv("STARTUP_GRACE_SEC"), 15),
        enable_weekend_skip=_to_bool(os.getenv("ENABLE_WEEKEND_SKIP"), True),
        enable_telegram=_to_bool(os.getenv("ENABLE_TELEGRAM"), False),
        enable_heartbeat=_to_bool(os.getenv("ENABLE_HEARTBEAT"), True),
        enable_alert_deduper=_to_bool(os.getenv("ENABLE_ALERT_DEDUPER"), True),
        watch_alerts_enabled=_to_bool(os.getenv("WATCH_ALERTS_ENABLED"), True),
        main_worker_alert_forwarding_enabled=_to_bool(
            os.getenv("MAIN_WORKER_ALERT_FORWARDING_ENABLED"),
            False,
        ),
        paper_mode=_to_bool(os.getenv("PAPER_MODE"), True),
        fail_fast=_to_bool(os.getenv("FAIL_FAST"), False),

        # Paths
        project_root=project_root,
        data_dir=data_dir,
        runtime_dir=runtime_dir,
        cache_dir=cache_dir,
        logs_dir=logs_dir,
        runner_state_path=runner_state_path,
        radar_journal_path=radar_journal_path,
        alerts_state_path=alerts_state_path,
        heartbeat_path=heartbeat_path,

        # Data provider
        twelvedata_api_key=_clean_str(os.getenv("TWELVEDATA_API_KEY"), ""),
        provider_name=_clean_str(os.getenv("PROVIDER_NAME"), "twelvedata"),
        provider_timeout_sec=_to_int(os.getenv("PROVIDER_TIMEOUT_SEC"), 30),
        provider_max_retries=_to_int(os.getenv("PROVIDER_MAX_RETRIES"), 3),
        provider_retry_backoff_sec=_to_float(os.getenv("PROVIDER_RETRY_BACKOFF_SEC"), 2.0),

        # Telegram
        telegram_bot_token=_clean_str(os.getenv("TELEGRAM_BOT_TOKEN"), ""),
        telegram_chat_id=_clean_str(os.getenv("TELEGRAM_CHAT_ID"), ""),
        telegram_parse_mode=_clean_str(os.getenv("TELEGRAM_PARSE_MODE"), "HTML"),
        telegram_silent=_to_bool(os.getenv("TELEGRAM_SILENT"), False),

        # Alert / scenario controls
        scenario_probability_threshold=_to_float(
            os.getenv("SCENARIO_PROBABILITY_THRESHOLD"), 0.55
        ),
        strong_watch_probability_threshold=_to_float(
            os.getenv("STRONG_WATCH_PROBABILITY_THRESHOLD"), 0.70
        ),
        alert_cooldown_sec=_to_int(os.getenv("ALERT_COOLDOWN_SEC"), 3600),

        # Universe / runtime control
        default_batch_size=_to_int(os.getenv("DEFAULT_BATCH_SIZE"), 5),
        force_batch=force_batch,
        auto_mode=_to_bool(os.getenv("AUTO_MODE"), True),
        simulation_mode=_to_bool(os.getenv("SIMULATION_MODE"), False),
        batch_group=_clean_str(os.getenv("BATCH_GROUP"), "core").lower(),
        enabled_symbols_raw=_clean_str(
            os.getenv(
                "ENABLED_SYMBOLS",
                ",".join(
                    [
                        # core
                        "XAUUSD",
                        "EURUSD",
                        "GBPUSD",
                        "BTCUSD",
                        "ETHUSD",

                        # fx_major
                        "USDJPY",
                        "USDCHF",
                        "USDCAD",
                        "AUDUSD",

                        # optional future fx reserve
                        "NZDUSD",
                        "EURJPY",
                        "GBPJPY",
                        "AUDJPY",

                        # multi-provider reserve
                        "UKOIL",
                        "GER40",
                        "NAS100",
                        "SPX500",
                        "DXY",
                    ]
                ),
            ),
            "",
        ),
    )

    app_settings.validate()
    app_settings.ensure_directories()
    return app_settings


settings = load_settings()


# =============================================================================
# BACKWARD COMPATIBILITY EXPORTS
# =============================================================================

@dataclass(frozen=True)
class LoaderConfig:
    """
    Legacy compatibility config for modules that still import LoaderConfig
    from app.core.settings.
    """
    use_cache_only: bool = False
    validate_price_sanity: bool = True
    allow_stale_cache: bool = False

    # legacy/common loader expectations
    default_outputsize: int = 500
    timeout_seconds: int = 20
    cache_only_on_weekend: bool = True
    allow_api_refresh: bool = True
    strict_sanity_check: bool = True
    prefer_cache_when_fresh: bool = True


EXPECTED_PRICE_RANGES: dict[str, tuple[float, float]] = {
    # metals
    "XAUUSD": (1500.0, 5000.0),

    # core FX
    "EURUSD": (0.5, 2.0),
    "GBPUSD": (0.5, 2.5),

    # fx_major
    "USDJPY": (50.0, 250.0),
    "USDCHF": (0.3, 2.0),
    "USDCAD": (0.5, 2.5),
    "AUDUSD": (0.3, 1.5),

    # optional future fx reserve
    "NZDUSD": (0.3, 1.5),
    "EURJPY": (50.0, 250.0),
    "GBPJPY": (70.0, 300.0),
    "AUDJPY": (40.0, 150.0),

    # crypto
    "BTCUSD": (1000.0, 250000.0),
    "ETHUSD": (100.0, 20000.0),

    # multi-provider reserve
    "UKOIL": (10.0, 200.0),
    "GER40": (5000.0, 50000.0),
    "NAS100": (5000.0, 50000.0),
    "SPX500": (1000.0, 10000.0),
    "DXY": (50.0, 200.0),
}

# legacy path aliases
PROJECT_ROOT = settings.project_root
DATA_DIR = settings.data_dir
RUNTIME_DIR = settings.runtime_dir
CACHE_DIR = settings.cache_dir
LOGS_DIR = settings.logs_dir
PROCESSED_DIR = settings.cache_dir

RUNNER_STATE_FILE = settings.runner_state_path
RADAR_JOURNAL_FILE = settings.radar_journal_path
ALERTS_STATE_FILE = settings.alerts_state_path
HEARTBEAT_FILE = settings.heartbeat_path

# legacy/simple exports
BATCH_GROUP = settings.batch_group