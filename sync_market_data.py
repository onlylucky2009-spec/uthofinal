import asyncio
import os
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict

import pytz
from kiteconnect import KiteConnect

# Internal component import
from redis_manager import TradeControl, IST

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] Sync_Market: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("Nexus_Sync")

# --- CONFIGURATION ---
# Aap apni watchlist yahan add kar sakte hain
WATCHLIST = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN", 
    "ICICIBANK", "AXISBANK", "ADANIENT", "BHARTIARTL", "KOTAKBANK"
]

async def run_morning_sync():
    """
    Core function to prepare the engine for the trading day.
    1. Fetches Access Token from Redis.
    2. Filters Kite Master Instruments.
    3. Fetches Historical Data to calculate PDH/PDL.
    4. Saves results to Redis for low-latency engine access.
    """
    logger.info("--- 🌅 MORNING SYNC STARTED ---")

    # 1. API Credentials Setup
    api_key, _ = await TradeControl.get_config()
    access_token = await TradeControl.get_access_token()

    if not api_key or not access_token:
        logger.error("❌ Sync Failed: API Key or Access Token not found in Redis. Login via Dashboard first.")
        return

    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        
        # 2. Fetch Instrument Master
        logger.info("📡 Fetching NSE instruments...")
        # to_thread use kiya hai taaki network blocking call async loop ko na roke
        all_instruments = await asyncio.to_thread(kite.instruments, "NSE")
        
        # Filter based on watchlist
        target_instruments = [
            ins for ins in all_instruments 
            if ins['tradingsymbol'] in WATCHLIST and ins['segment'] == 'NSE'
        ]

        logger.info(f"🔍 Watchlist filtered: {len(target_instruments)} stocks identified.")

        # 3. Process each stock for Technical Levels
        universe_tokens = []
        sync_count = 0

        for ins in target_instruments:
            symbol = ins['tradingsymbol']
            token = ins['instrument_token']
            
            # Historical Range: Last 10 days to ensure we get a complete daily candle
            to_date = datetime.now(IST).date()
            from_date = to_date - timedelta(days=10)

            try:
                # Fetch daily candles
                hist = await asyncio.to_thread(
                    kite.historical_data, token, from_date, to_date, "day"
                )

                if len(hist) >= 2:
                    # Logic: If market hasn't opened today, hist[-1] is yesterday.
                    # If market is open, hist[-2] is yesterday.
                    # Safety check: Compare dates
                    last_candle = hist[-1]
                    if last_candle['date'].date() == to_date:
                        prev_day = hist[-2]
                    else:
                        prev_day = hist[-1]

                    # Calculate Volume SMA (Simple moving average of last 5 days volume)
                    # This helps filtration in MomentumEngine
                    recent_vols = [d['volume'] for d in hist[-6:-1]]
                    vol_sma = sum(recent_vols) / len(recent_vols) if recent_vols else 0

                    market_data = {
                        "symbol": symbol,
                        "token": token,
                        "pdh": float(prev_day['high']),
                        "pdl": float(prev_day['low']),
                        "prev_close": float(prev_day['close']),
                        "sma": int(vol_sma),
                        "sync_time": datetime.now(IST).strftime("%H:%M:%S")
                    }

                    # Save to Redis
                    await TradeControl.save_market_data(str(token), market_data)
                    universe_tokens.append(token)
                    sync_count += 1
                    
                    logger.info(f"✅ {symbol} -> PDH: {market_data['pdh']} | PDL: {market_data['pdl']} | SMA: {market_data['sma']}")
                else:
                    logger.warning(f"⚠️ {symbol}: Insufficient historical data.")

            except Exception as e:
                logger.error(f"❌ Error processing {symbol}: {e}")

        # 4. Save the Final Universe for Ticker Subscription
        r = await TradeControl.get_redis()
        await r.set("nexus:universe:tokens", json.dumps(universe_tokens))
        
        logger.info(f"--- 🚀 SYNC COMPLETE: {sync_count} stocks ready for trading ---")

    except Exception as e:
        logger.error(f"❌ Critical Sync Failure: {e}")

if __name__ == "__main__":
    # Heroku Scheduler or manual trigger logic
    asyncio.run(run_morning_sync())