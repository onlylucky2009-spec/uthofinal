# sync_market_data.py
import asyncio
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import pytz
from kiteconnect import KiteConnect

from redis_manager import TradeControl

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ASYNC-SYNC] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Sync_Command")
IST = pytz.timezone("Asia/Kolkata")

# -----------------------------
# FILTERS / TUNING
# -----------------------------
MIN_VOL_SMA = int(os.getenv("MIN_VOL_SMA", "1000"))  # keep stocks with SMA >= MIN_VOL_SMA
MAX_CONCURRENCY = int(os.getenv("SYNC_CONCURRENCY", "6"))  # parallel tasks
REQ_SLEEP = float(os.getenv("SYNC_SLEEP_SEC", "0.34"))  # Zerodha-friendly delay per task
HIST_LOOKBACK_DAYS = int(os.getenv("HIST_LOOKBACK_DAYS", "10"))
CANDLE_INTERVAL = os.getenv("HIST_INTERVAL", "day")

# -----------------------------
# STOCK UNIVERSE (example)
# -----------------------------
STOCK_INDEX_MAPPING = {
    "MRF": "NIFTY 500",
    "VIDYAWIRES": "NIFTY 500",
    "MEDICO": "NIFTY 500",
    "BLKASHYAP": "NIFTY 500",
    "AMJLAND": "NIFTY 500",
    "AHLADA": "NIFTY 500",
    "AMDIND": "NIFTY 500",
    "ROML": "NIFTY 500",
    "TEXMOPIPES": "NIFTY 500",
}


def _now_ist() -> datetime:
    return datetime.now(IST)


def _safe_float(x, d=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _safe_int(x, d=0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _compute_sma_from_records(records: List[dict]) -> float:
    """
    records: list of daily candles with volume/high/low/close etc.
    SMA definition used in your app:
      avg_vol_per_minute = (sum(last 5 sessions volume)) / 1875
    1875 = 5 days * 375 minutes/session (NSE regular)
    """
    if len(records) < 6:
        return 0.0
    last_5 = records[-6:-1]  # last 5 completed sessions
    total_vol = sum(_safe_int(day.get("volume", 0), 0) for day in last_5)
    avg_vol_per_minute = total_vol / 1875.0
    return round(avg_vol_per_minute, 2)


async def _fetch_one(
    kite: KiteConnect,
    t_id: int,
    symbol: str,
    sem: asyncio.Semaphore,
) -> Tuple[int, str, bool, float, Optional[dict]]:
    """
    Fetch historical -> compute SMA -> build market_data
    Returns:
      (token, symbol, eligible, sma, market_data_if_any)
    """
    async with sem:
        try:
            to_date = _now_ist()
            from_date = to_date - timedelta(days=HIST_LOOKBACK_DAYS)

            # network call in thread (kiteconnect is sync)
            records = await asyncio.to_thread(
                kite.historical_data,
                t_id,
                from_date,
                to_date,
                CANDLE_INTERVAL,
            )

            sma = _compute_sma_from_records(records)
            if sma < MIN_VOL_SMA:
                return (t_id, symbol, False, sma, None)

            last_5 = records[-6:-1]
            last = last_5[-1]

            market_data = {
                "symbol": symbol,
                "sma": float(sma),
                "pdh": _safe_float(last.get("high", 0.0), 0.0),
                "pdl": _safe_float(last.get("low", 0.0), 0.0),
                "prev_close": _safe_float(last.get("close", 0.0), 0.0),
                "sync_time": _now_ist().strftime("%Y-%m-%d %H:%M:%S"),
            }

            return (t_id, symbol, True, sma, market_data)

        except Exception as e:
            logger.debug(f"Error syncing {symbol} ({t_id}): {e}")
            return (t_id, symbol, False, 0.0, None)

        finally:
            # Zerodha historical rate-limit friendliness
            await asyncio.sleep(REQ_SLEEP)


async def _persist_results(results: List[Tuple[int, str, bool, float, Optional[dict]]]) -> Tuple[List[int], List[str]]:
    """
    Parallel Redis writes (safe).
    Also cleans up non-eligible tokens so stale market cache doesn't remain.
    """
    eligible_tokens: List[int] = []
    eligible_symbols: List[str] = []

    async def _save_ok(t_id: int, md: dict):
        await TradeControl.save_market_data(str(t_id), md)

    async def _delete_bad(t_id: int):
        # optional cleanup
        await TradeControl.delete_market_data(str(t_id))

    save_tasks = []
    delete_tasks = []

    for (t_id, sym, ok, sma, md) in results:
        if ok and md:
            eligible_tokens.append(int(t_id))
            eligible_symbols.append(str(sym))
            save_tasks.append(asyncio.create_task(_save_ok(t_id, md)))
        else:
            delete_tasks.append(asyncio.create_task(_delete_bad(t_id)))

    # do Redis ops concurrently (this is the "parallel processing integration" for sync)
    if save_tasks:
        await asyncio.gather(*save_tasks, return_exceptions=True)
    if delete_tasks:
        await asyncio.gather(*delete_tasks, return_exceptions=True)

    return eligible_tokens, eligible_symbols


async def run_sync() -> None:
    logger.info("🚀 Starting Async Market Data Sync (parallel + SMA filter)...")

    api_key, _api_secret = await TradeControl.get_config()
    access_token = await TradeControl.get_access_token()

    if not api_key or not access_token:
        logger.error("❌ Sync Aborted: API Key or Access Token missing in Redis.")
        return

    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        logger.info("📥 Fetching NSE instruments...")
        instruments = await asyncio.to_thread(kite.instruments, "NSE")

        # Build target list
        target: List[Tuple[int, str]] = []
        for instr in instruments:
            sym = instr.get("tradingsymbol")
            if sym and sym in STOCK_INDEX_MAPPING:
                try:
                    target.append((int(instr["instrument_token"]), str(sym)))
                except Exception:
                    pass

        logger.info(f"🧭 Universe candidates: {len(target)} stocks")

        if not target:
            await TradeControl.save_subscribe_universe([], [])
            await TradeControl.set_last_sync()
            logger.warning("⚠️ No target stocks found. Universe cleared.")
            return

        sem = asyncio.Semaphore(MAX_CONCURRENCY)

        # Parallel historical fetch
        tasks = [
            asyncio.create_task(_fetch_one(kite, t_id, sym, sem))
            for (t_id, sym) in target
        ]
        results = await asyncio.gather(*tasks)

        # Persist to Redis (parallel writes + cleanup)
        eligible_tokens, eligible_symbols = await _persist_results(results)

        # Persist universe tokens list for websocket subscription
        await TradeControl.save_subscribe_universe(eligible_tokens, eligible_symbols)
        await TradeControl.set_last_sync()

        logger.info(
            f"✅ SUCCESS: {len(eligible_tokens)} eligible stocks saved (SMA >= {MIN_VOL_SMA})."
        )
        logger.info(
            f"📡 Universe tokens persisted: nexus:universe:tokens (count={len(eligible_tokens)})"
        )

        # Optional debug summary
        if os.getenv("SYNC_DEBUG", "0") == "1":
            kept = set(eligible_symbols)
            dropped = [sym for (_, sym) in target if sym not in kept]
            logger.info(f"DEBUG kept={sorted(list(kept))}")
            logger.info(f"DEBUG dropped={sorted(dropped)}")

    except Exception as e:
        logger.error(f"❌ Critical Sync Failure: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(run_sync())
