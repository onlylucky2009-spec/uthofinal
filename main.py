import asyncio
import os
import logging
import threading
import json
import time as t_lib
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Optional, Set

# Core FastAPI & Server
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Kite Connect SDK
from kiteconnect import KiteConnect, KiteTicker

# Optimized Event Loop (C-based)
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

# Custom Async Logic
from breakout_engine import BreakoutEngine
from momentum_engine import MomentumEngine
from redis_manager import TradeControl

# --- DETAILED LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Nexus_Async_Core")
IST = pytz.timezone("Asia/Kolkata")

# --- FASTAPI APP ---
app = FastAPI(strict_slashes=False)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- STOCK UNIVERSE ---
# [USER: PASTE YOUR 500+ STOCK_INDEX_MAPPING DICTIONARY HERE]
STOCK_INDEX_MAPPING = {} 

# --- CONSOLIDATED ASYNC RAM STATE ---
RAM_STATE = {
    "kite": None,
    "kws": None,
    "api_key": "",
    "api_secret": "",
    "access_token": "",
    "stocks": {}, 
    "trades": {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]},
    "config": {
        side: {
            "volume_criteria": [{"min_vol_price_cr": 0, "sma_multiplier": 1.0, "min_sma_avg": 0} for _ in range(10)],
            "total_trades": 5, 
            "risk_trade_1": 2000, 
            "risk_reward": "1:2", 
            "trailing_sl": "1:1.5"
        } for side in ["bull", "bear", "mom_bull", "mom_bear"]
    },
    "engine_live": {side: False for side in ["bull", "bear", "mom_bull", "mom_bear"]},
    "pnl": {"total": 0.0, "bull": 0.0, "bear": 0.0, "mom_bull": 0.0, "mom_bear": 0.0},
    "data_connected": {"breakout": False, "momentum": False},
    "manual_exits": set()
}

# --- ASYNC TICKER BRIDGE ---
def on_ticks(ws, ticks):
    """
    Bridge between Kite's Thread and FastAPI's Async Loop.
    This runs in a background thread and schedules work in the main loop.
    """
    loop = asyncio.get_event_loop()
    for tick in ticks:
        token = tick['instrument_token']
        if token in RAM_STATE["stocks"]:
            ltp = tick['last_price']
            vol = tick.get('volume_traded', 0)
            
            # Immediately update ltp in RAM (thread-safe for reading)
            RAM_STATE["stocks"][token]['ltp'] = ltp
            
            # Schedule the heavy logic inside the async event loop
            asyncio.run_coroutine_threadsafe(
                BreakoutEngine.run(token, ltp, vol, RAM_STATE),
                loop
            )

def on_connect(ws, response):
    logger.info("✅ Ticker Connected. Syncing Subscriptions...")
    tokens = list(RAM_STATE["stocks"].keys())
    if tokens:
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
    RAM_STATE["data_connected"]["breakout"] = True
    RAM_STATE["data_connected"]["momentum"] = True

# --- CONCURRENT MARKET DATA SYNC ---
async def fetch_stock_sma(kite, t_id, symbol, semaphore):
    """
    Fetches historical data for one stock asynchronously.
    Semaphore ensures we stay within Zerodha's 3 requests/sec limit.
    """
    async with semaphore:
        try:
            to_date = datetime.now(IST)
            from_date = to_date - timedelta(days=35)
            
            # Run blocking SDK call in a thread pool to keep the async loop free
            records = await asyncio.to_thread(
                kite.historical_data, t_id, from_date, to_date, "day"
            )
            
            if len(records) >= 20:
                last_20 = records[-21:-1]
                avg_vol = sum(r['volume'] for r in last_20) / 20
                data = {
                    "sma": avg_vol,
                    "pdh": last_20[-1]['high'],
                    "pdl": last_20[-1]['low']
                }
                RAM_STATE["stocks"][t_id].update(data)
                # Save to Redis Cache for Fast-Boot tomorrow
                await TradeControl.save_market_data(str(t_id), data)
                return True
        except Exception as e:
            logger.debug(f"Failed SMA fetch for {symbol}: {e}")
        
        # Micro-sleep to prevent bursting Zerodha limits
        await asyncio.sleep(0.34) 
        return False

async def market_data_sync_logic():
    """
    Orchestrates the parallel sync of all 500+ stocks.
    """
    today = datetime.now(IST).strftime("%Y-%m-%d")
    
    # 1. Check Redis Cache First
    cached_data = await TradeControl.get_all_market_data()
    if cached_data:
        logger.info(f"⚡ FAST BOOT: Restoring {len(cached_data)} stocks from Redis Cache.")
        for t_id_str, data in cached_data.items():
            t_id = int(t_id_str)
            if t_id in RAM_STATE["stocks"]:
                RAM_STATE["stocks"][t_id].update(data)
        logger.info("✅ RAM Hydrated from Cache.")
        return

    # 2. Parallel Fetching if Cache is empty (Morning Refresh)
    logger.info("📅 REFRESHING: Fetching 20-Day SMA from Kite API...")
    sem = asyncio.Semaphore(3) # Max 3 concurrent requests
    tasks = []
    for t_id, stock in RAM_STATE["stocks"].items():
        tasks.append(fetch_stock_sma(RAM_STATE["kite"], t_id, stock['symbol'], sem))
    
    results = await asyncio.gather(*tasks)
    logger.info(f"✅ Morning Sync Finished. {sum(results)} stocks qualified.")

# --- FASTAPI LIFECYCLE ---

@app.on_event("startup")
async def startup_event():
    logger.info("--- 🚀 NEXUS ASYNC ENGINE BOOTING ---")
    
    # 1. Load API Config from Redis
    key, secret = await TradeControl.get_config()
    token = await TradeControl.get_access_token()
    
    if key and secret:
        RAM_STATE["api_key"], RAM_STATE["api_secret"] = key, secret
        logger.info(f"API Credentials loaded: {key[:5]}***")

    if token and key:
        try:
            RAM_STATE["access_token"] = token
            RAM_STATE["kite"] = KiteConnect(api_key=key)
            RAM_STATE["kite"].set_access_token(token)
            
            # Map NSE Instruments
            logger.info("Mapping NSE Instruments...")
            instruments = await asyncio.to_thread(RAM_STATE["kite"].instruments, "NSE")
            for instr in instruments:
                symbol = instr['tradingsymbol']
                if symbol in STOCK_INDEX_MAPPING:
                    t_id = instr['instrument_token']
                    RAM_STATE["stocks"][t_id] = {
                        'symbol': symbol, 'ltp': 0, 'status': 'WAITING', 'trades': 0,
                        'hi': 0, 'lo': 0, 'pdh': 0, 'pdl': 0, 'sma': 0, 'candle': None, 'last_vol': 0
                    }

            # Run Async Sync Logic
            await market_data_sync_logic()
            
            # Start Background WebSocket
            RAM_STATE["kws"] = KiteTicker(key, token)
            RAM_STATE["kws"].on_ticks = on_ticks
            RAM_STATE["kws"].on_connect = on_connect
            
            # Run in daemon thread to avoid blocking the loop
            threading.Thread(target=lambda: RAM_STATE["kws"].connect(threaded=False), daemon=True).start()
            logger.info("Ticker service active.")
            
        except Exception as e:
            logger.error(f"Startup Crash: {e}")

# --- DASHBOARD ENDPOINTS ---

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    with open("index.html", "r") as f: return f.read()

@app.get("/api/kite/login")
async def kite_login_redirect():
    api_key = RAM_STATE["api_key"] or os.getenv("KITE_API_KEY")
    if not api_key: return {"status": "error", "message": "Save API Key First."}
    return RedirectResponse(url=KiteConnect(api_key=api_key).login_url())

@app.get("/login")
async def kite_callback(request_token: str = None):
    try:
        api_key, api_secret = RAM_STATE["api_key"], RAM_STATE["api_secret"]
        kite = KiteConnect(api_key=api_key)
        data = await asyncio.to_thread(kite.generate_session, request_token, api_secret=api_secret)
        
        token = data["access_token"]
        RAM_STATE["access_token"] = token
        RAM_STATE["kite"] = kite
        RAM_STATE["kite"].set_access_token(token)
        
        await TradeControl.save_access_token(token)
        
        # Start ticker connection
        RAM_STATE["kws"] = KiteTicker(api_key, token)
        RAM_STATE["kws"].on_ticks = on_ticks
        RAM_STATE["kws"].on_connect = on_connect
        threading.Thread(target=RAM_STATE["kws"].connect, daemon=True).start()
        
        return RedirectResponse(url="/")
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stats")
async def get_stats():
    total_pnl = 0.0
    engine_stats = {}
    for side in ["bull", "bear", "mom_bull", "mom_bear"]:
        side_pnl = sum(t.get('pnl', 0) for t in RAM_STATE["trades"][side])
        engine_stats[side] = side_pnl
        total_pnl += side_pnl
    
    RAM_STATE["pnl"]["total"] = total_pnl
    return {
        "pnl": {**RAM_STATE["pnl"], **engine_stats},
        "data_connected": RAM_STATE["data_connected"],
        "engine_status": {k: ("1" if v else "0") for k, v in RAM_STATE["engine_live"].items()}
    }

@app.get("/api/orders")
async def get_orders(): return RAM_STATE["trades"]

@app.get("/api/scanner")
async def get_scanner():
    signals = {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]}
    for t_id, s in RAM_STATE["stocks"].items():
        if s.get('status') == 'TRIGGER_WATCH':
            side = s.get('side_latch', '').lower()
            if side in signals:
                signals[side].append({"symbol": s['symbol'], "price": s['trigger_px']})
    return signals

@app.post("/api/control")
async def control_center(data: dict):
    action = data.get("action")
    if action == "save_api":
        RAM_STATE["api_key"], RAM_STATE["api_secret"] = data["api_key"], data["api_secret"]
        await TradeControl.save_config(data["api_key"], data["api_secret"])
    elif action == "get_saved_keys":
        return {"api_key": RAM_STATE["api_key"], "api_secret": "********" if RAM_STATE["api_secret"] else ""}
    elif action == "toggle_engine":
        RAM_STATE["engine_live"][data['side']] = data['enabled']
    elif action == "manual_exit":
        RAM_STATE["manual_exits"].add(data['symbol'])
    return {"status": "ok"}

@app.get("/api/settings/engine/{side}")
async def get_params(side: str): return RAM_STATE["config"].get(side, {})

@app.post("/api/settings/engine/{side}")
async def save_params(side: str, data: dict):
    RAM_STATE["config"][side].update(data)
    return {"status": "success"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    # Using uvloop for maximum performance on Heroku
    uvicorn.run("main:app", host="0.0.0.0", port=port, loop="uvloop")