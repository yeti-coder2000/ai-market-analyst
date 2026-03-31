from schema import Instrument, Timeframe


# ============================================================
# PROFILE TYPES
# ============================================================

PROFILE_FULL = "FULL"
PROFILE_LIGHT = "LIGHT"


# ============================================================
# PROFILE MAP
# ============================================================

INSTRUMENT_PROFILES = {

    # Core markets (частіше скануємо)
    Instrument.XAUUSD: PROFILE_FULL,
    Instrument.EURUSD: PROFILE_FULL,
    Instrument.GBPUSD: PROFILE_FULL,

    # Crypto
    Instrument.BTCUSD: PROFILE_LIGHT,
    Instrument.ETHUSD: PROFILE_LIGHT,

    # Indices / commodities
    Instrument.UKOIL: PROFILE_LIGHT,
    Instrument.GER40: PROFILE_LIGHT,
    Instrument.NAS100: PROFILE_LIGHT,
    Instrument.SPX500: PROFILE_LIGHT,
}


# ============================================================
# TIMEFRAME RULES
# ============================================================

PROFILE_TIMEFRAMES = {

    PROFILE_FULL: [
        Timeframe.M15,
        Timeframe.M30,
        Timeframe.H1,
        Timeframe.H4,
        Timeframe.D1,
    ],

    PROFILE_LIGHT: [
        Timeframe.M15,
        Timeframe.M30,
    ],
}


# ============================================================
# HELPERS
# ============================================================

def get_profile(instrument: Instrument):

    return INSTRUMENT_PROFILES.get(
        instrument,
        PROFILE_LIGHT
    )


def get_timeframes_for_instrument(instrument: Instrument):

    profile = get_profile(instrument)

    return PROFILE_TIMEFRAMES[profile]