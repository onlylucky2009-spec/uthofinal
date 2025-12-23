import asyncio
import os
import json
import logging
from datetime import datetime, timedelta
import pytz
from kiteconnect import KiteConnect
from redis_manager import TradeControl

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ASYNC-SYNC] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Sync_Command")
IST = pytz.timezone("Asia/Kolkata")

# --- FILTER ---
MIN_VOL_SMA = 1000  # ✅ only keep stocks with SMA >= 1000

# --- STOCK UNIVERSE ---
STOCK_INDEX_MAPPING = {
    "MRF": "NIFTY 500",
    "3MINDIA": "NIFTY 500",
    "VIDYAWIRES": "NIFTY 500",
    "MEDICO": "NIFTY 500",
    "BLKASHYAP": "NIFTY 500",
    "AMJLAND": "NIFTY 500",
    "AHLADA": "NIFTY 500",
    "AMDIND": "NIFTY 500",
    "ROML": "NIFTY 500",
    "TEXMOPIPES": "NIFTY 500",
}


async def fetch_and_sync_stock(kite, t_id: int, symbol: str, semaphore: asyncio.Semaphore):
    """
    Fetch historical, compute:
      SMA = (Sum(last 5 days volume))/1875  (avg vol per minute over 5 days)
    Save to Redis ONLY if sma >= MIN_VOL_SMA
    Returns: (t_id, symbol, saved_bool, sma_value)
    """
    async with semaphore:
        try:
            to_date = datetime.now(IST)
            from_date = to_date - timedelta(days=10)

            records = await asyncio.to_thread(
                kite.historical_data, t_id, from_date, to_date, "day"
            )

            if len(records) < 6:
                return (t_id, symbol, False, 0.0)

            # Last 5 completed sessions
            last_5_days = records[-6:-1]
            total_vol = sum(day["volume"] for day in last_5_days)

            avg_vol_per_minute = total_vol / 1875
            sma = round(avg_vol_per_minute, 2)

            # ✅ FILTER HERE
            if sma < MIN_VOL_SMA:
                # Optional cleanup (so old data doesn't remain)
                try:
                    await TradeControl.delete_market_data(str(t_id))
                except Exception:
                    pass
                return (t_id, symbol, False, sma)

            market_data = {
                "symbol": symbol,
                "sma": sma,
                "pdh": float(last_5_days[-1]["high"]),
                "pdl": float(last_5_days[-1]["low"]),
                "prev_close": float(last_5_days[-1]["close"]),
                "sync_time": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
            }

            await TradeControl.save_market_data(str(t_id), market_data)
            return (t_id, symbol, True, sma)

        except Exception as e:
            logger.debug(f"Error syncing {symbol}: {e}")
            return (t_id, symbol, False, 0.0)

        finally:
            # Zerodha historical rate limit friendly
            await asyncio.sleep(0.34)


async def run_sync():
    logger.info("🚀 Starting Async Market Data Sync (SMA filter enabled)...")

    api_key, api_secret = await TradeControl.get_config()
    access_token = await TradeControl.get_access_token()

    if not api_key or not access_token:
        logger.error("❌ Sync Aborted: API Key or Access Token missing in Redis.")
        return

    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        logger.info("Fetching NSE instruments...")
        instruments = await asyncio.to_thread(kite.instruments, "NSE")

        target_stocks = []
        for instr in instruments:
            symbol = instr.get("tradingsymbol")
            if symbol in STOCK_INDEX_MAPPING:
                target_stocks.append((int(instr["instrument_token"]), symbol))

        logger.info(f"Universe candidates: {len(target_stocks)} stocks")

        sem = asyncio.Semaphore(3)
        tasks = [fetch_and_sync_stock(kite, t_id, sym, sem) for t_id, sym in target_stocks]
        results = await asyncio.gather(*tasks)

        eligible_tokens = [t_id for (t_id, sym, ok, sma) in results if ok]
        eligible_symbols = [sym for (t_id, sym, ok, sma) in results if ok]

        # ✅ Persist eligible tokens list for websocket subscription
        await TradeControl.save_subscribe_universe(eligible_tokens, eligible_symbols)

        await TradeControl.set_last_sync()

        logger.info(
            f"✅ SUCCESS: {len(eligible_tokens)} eligible stocks saved (SMA >= {MIN_VOL_SMA})."
        )
        logger.info("✅ Universe tokens persisted to Redis for websocket subscribe.")

    except Exception as e:
        logger.error(f"Critical Sync Failure: {e}")


if __name__ == "__main__":
    asyncio.run(run_sync())
