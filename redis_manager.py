# redis_manager.py
"""
Nexus Redis Manager (Async)

✅ Supports:
- API config (api_key/api_secret)
- Zerodha access_token
- Market cache (token -> {symbol,sma,pdh,pdl,prev_close,sync_time})
- Strategy settings persistence (nexus:settings:{side})
- Trade limit counters (per engine side)
- Subscribe universe persistence (eligible tokens list)

Environment:
- On Heroku, set REDIS_URL (recommended) or REDIS_HOST/REDIS_PORT/REDIS_PASSWORD

Dependencies:
- redis>=4.2.0  (uses redis.asyncio)
"""

import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, Tuple, Optional, List

import pytz
import redis.asyncio as redis

# -----------------------------
# LOGGING / TIMEZONE
# -----------------------------
logger = logging.getLogger("Redis_Manager")
IST = pytz.timezone("Asia/Kolkata")

# -----------------------------
# REDIS CONNECTION
# -----------------------------
REDIS_URL = os.getenv("REDIS_URL")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Key prefixes
KEY_API_CONFIG = "nexus:config:api"              # {api_key, api_secret}
KEY_ACCESS_TOKEN = "nexus:auth:access_token"     # string
KEY_LAST_SYNC = "nexus:sync:last"                # string timestamp

MARKET_KEY_PREFIX = "nexus:market:"              # nexus:market:{token}
SETTINGS_KEY_PREFIX = "nexus:settings:"          # nexus:settings:{side}

KEY_UNIVERSE_TOKENS = "nexus:universe:tokens"    # json list[int]
KEY_UNIVERSE_SYMBOLS = "nexus:universe:symbols"  # json list[str]
KEY_UNIVERSE_UPDATED = "nexus:universe:updated_at"

TRADE_COUNT_PREFIX = "nexus:tradecount:"         # nexus:tradecount:{side} -> int daily
TRADE_COUNT_DATE = "nexus:tradecount:date"       # YYYY-MM-DD

# Create one global redis client
if REDIS_URL:
    r = redis.from_url(REDIS_URL, decode_responses=True)
else:
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        decode_responses=True,
    )


class TradeControl:
    # -----------------------------
    # API CONFIG / AUTH
    # -----------------------------
    @staticmethod
    async def save_config(api_key: str, api_secret: str) -> bool:
        try:
            await r.set(KEY_API_CONFIG, json.dumps({"api_key": api_key, "api_secret": api_secret}))
            return True
        except Exception as e:
            logger.error(f"Failed to save api config: {e}")
            return False

    @staticmethod
    async def get_config() -> Tuple[str, str]:
        try:
            raw = await r.get(KEY_API_CONFIG)
            if not raw:
                return "", ""
            data = json.loads(raw)
            return (data.get("api_key", "") or "", data.get("api_secret", "") or "")
        except Exception as e:
            logger.error(f"Failed to get api config: {e}")
            return "", ""

    @staticmethod
    async def save_access_token(access_token: str) -> bool:
        try:
            await r.set(KEY_ACCESS_TOKEN, access_token)
            return True
        except Exception as e:
            logger.error(f"Failed to save access token: {e}")
            return False

    @staticmethod
    async def get_access_token() -> str:
        try:
            return (await r.get(KEY_ACCESS_TOKEN)) or ""
        except Exception as e:
            logger.error(f"Failed to get access token: {e}")
            return ""

    # -----------------------------
    # MARKET DATA CACHE
    # -----------------------------
    @staticmethod
    async def save_market_data(token: str, market_data: Dict[str, Any]) -> bool:
        """
        Saves market cache per token.
        Key: nexus:market:{token}
        """
        try:
            key = f"{MARKET_KEY_PREFIX}{token}"
            await r.set(key, json.dumps(market_data))
            return True
        except Exception as e:
            logger.error(f"Failed to save market data {token}: {e}")
            return False

    @staticmethod
    async def get_market_data(token: str) -> Dict[str, Any]:
        try:
            key = f"{MARKET_KEY_PREFIX}{token}"
            raw = await r.get(key)
            return json.loads(raw) if raw else {}
        except Exception as e:
            logger.error(f"Failed to get market data {token}: {e}")
            return {}

    @staticmethod
    async def get_all_market_data() -> Dict[str, Dict[str, Any]]:
        """
        Returns dict token_str -> market_data
        """
        try:
            pattern = f"{MARKET_KEY_PREFIX}*"
            keys = await r.keys(pattern)
            if not keys:
                return {}
            vals = await r.mget(keys)

            out: Dict[str, Dict[str, Any]] = {}
            for k, v in zip(keys, vals):
                if not v:
                    continue
                token = k.replace(MARKET_KEY_PREFIX, "")
                try:
                    out[token] = json.loads(v)
                except Exception:
                    continue
            return out
        except Exception as e:
            logger.error(f"Failed to get all market data: {e}")
            return {}

    @staticmethod
    async def delete_market_data(token: str) -> bool:
        """
        Deletes one instrument market cache entry: nexus:market:{token}
        (useful when it fails SMA filter so stale data doesn't remain)
        """
        try:
            key = f"{MARKET_KEY_PREFIX}{token}"
            await r.delete(key)
            return True
        except Exception as e:
            logger.error(f"Failed to delete market data {token}: {e}")
            return False

    @staticmethod
    async def set_last_sync() -> bool:
        try:
            await r.set(KEY_LAST_SYNC, datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"))
            return True
        except Exception as e:
            logger.error(f"Failed to set last sync: {e}")
            return False

    @staticmethod
    async def get_last_sync() -> str:
        try:
            return (await r.get(KEY_LAST_SYNC)) or ""
        except Exception as e:
            logger.error(f"Failed to get last sync: {e}")
            return ""

    # -----------------------------
    # STRATEGY SETTINGS (PERSISTED)
    # -----------------------------
    @staticmethod
    async def save_strategy_settings(side: str, cfg: dict) -> bool:
        try:
            key = f"{SETTINGS_KEY_PREFIX}{side}"
            await r.set(key, json.dumps(cfg))
            return True
        except Exception as e:
            logger.error(f"Failed to save strategy settings {side}: {e}")
            return False

    @staticmethod
    async def get_strategy_settings(side: str) -> dict:
        try:
            key = f"{SETTINGS_KEY_PREFIX}{side}"
            val = await r.get(key)
            return json.loads(val) if val else {}
        except Exception as e:
            logger.error(f"Failed to get strategy settings {side}: {e}")
            return {}

    # -----------------------------
    # SUBSCRIBE UNIVERSE (ELIGIBLE TOKENS)
    # -----------------------------
    @staticmethod
    async def save_subscribe_universe(tokens: List[int], symbols: Optional[List[str]] = None) -> bool:
        """
        Store eligible instrument tokens to subscribe all day.
        """
        try:
            await r.set(KEY_UNIVERSE_TOKENS, json.dumps([int(x) for x in tokens]))
            if symbols is not None:
                await r.set(KEY_UNIVERSE_SYMBOLS, json.dumps(list(symbols)))
            await r.set(KEY_UNIVERSE_UPDATED, datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"))
            return True
        except Exception as e:
            logger.error(f"Failed to save subscribe universe: {e}")
            return False

    @staticmethod
    async def get_subscribe_universe_tokens() -> List[int]:
        """
        Read eligible instrument tokens list.
        """
        try:
            raw = await r.get(KEY_UNIVERSE_TOKENS)
            if not raw:
                return []
            data = json.loads(raw)
            return [int(x) for x in data]
        except Exception as e:
            logger.error(f"Failed to get subscribe universe tokens: {e}")
            return []

    @staticmethod
    async def get_subscribe_universe_symbols() -> List[str]:
        try:
            raw = await r.get(KEY_UNIVERSE_SYMBOLS)
            if not raw:
                return []
            data = json.loads(raw)
            return [str(x) for x in data]
        except Exception as e:
            logger.error(f"Failed to get subscribe universe symbols: {e}")
            return []

    @staticmethod
    async def get_subscribe_universe_updated_at() -> str:
        try:
            return (await r.get(KEY_UNIVERSE_UPDATED)) or ""
        except Exception as e:
            logger.error(f"Failed to get subscribe universe updated_at: {e}")
            return ""

    # -----------------------------
    # DAILY TRADE LIMIT COUNTERS
    # -----------------------------
    @staticmethod
    async def _roll_trade_count_if_new_day() -> None:
        """
        Ensure trade counters reset once per day (IST).
        """
        today = datetime.now(IST).strftime("%Y-%m-%d")
        try:
            stored = await r.get(TRADE_COUNT_DATE)
            if stored != today:
                # New day: delete all counters
                keys = await r.keys(f"{TRADE_COUNT_PREFIX}*")
                if keys:
                    await r.delete(*keys)
                await r.set(TRADE_COUNT_DATE, today)
        except Exception as e:
            logger.error(f"Trade count roll error: {e}")

    @staticmethod
    async def can_trade(side: str, max_trades: int) -> bool:
        """
        Returns True if side has remaining trades for the day.
        NOTE: This function is used by engines before placing orders.
        """
        try:
            await TradeControl._roll_trade_count_if_new_day()
            key = f"{TRADE_COUNT_PREFIX}{side}"
            current = await r.get(key)
            current_n = int(current) if current else 0
            if current_n >= int(max_trades):
                return False
            # increment and allow
            await r.incr(key)
            return True
        except Exception as e:
            logger.error(f"can_trade failed for {side}: {e}")
            # safest: block trade if counter broken
            return False

    # -----------------------------
    # UTIL
    # -----------------------------
    @staticmethod
    async def ping() -> bool:
        try:
            return bool(await r.ping())
        except Exception:
            return False
