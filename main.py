# # import asyncio
# # import os
# # import logging
# # from datetime import datetime
# # from typing import Dict, Any, Optional

# # import pytz
# # from fastapi import FastAPI, Request
# # from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
# # from fastapi.middleware.cors import CORSMiddleware

# # from kiteconnect import KiteConnect, KiteTicker

# # from redis_manager import TradeControl
# # from breakout_engine import BreakoutEngine
# # from momentum_engine import MomentumEngine

# # # Twisted signal bypass for gunicorn
# # try:
# #     from twisted.internet import reactor  # type: ignore
# #     _original_run = reactor.run

# #     def _patched_reactor_run(*args, **kwargs):
# #         kwargs["installSignalHandlers"] = False
# #         return _original_run(*args, **kwargs)

# #     reactor.run = _patched_reactor_run
# # except Exception:
# #     pass

# # logging.basicConfig(
# #     level=logging.INFO,
# #     format="%(asctime)s [MAIN] %(message)s",
# #     datefmt="%Y-%m-%d %H:%M:%S",
# # )
# # logger = logging.getLogger("Nexus_Main")
# # IST = pytz.timezone("Asia/Kolkata")

# # # ✅ FastAPI syntax fix: redirect_slashes (not strict_slashes)
# # app = FastAPI(title="Nexus Core", version="1.0.0", redirect_slashes=False)

# # app.add_middleware(
# #     CORSMiddleware,
# #     allow_origins=["*"],
# #     allow_credentials=True,
# #     allow_methods=["*"],
# #     allow_headers=["*"],
# # )

# # def _default_volume_matrix() -> list:
# #     multipliers = [20, 18, 16, 14, 12, 10, 8, 6, 4, 2]
# #     out = []
# #     for i in range(10):
# #         out.append(
# #             {
# #                 "min_vol_price_cr": float(i + 1),
# #                 "sma_multiplier": float(multipliers[i]),
# #                 "min_sma_avg": int(1000),
# #             }
# #         )
# #     return out

# # DEFAULT_CONFIG: Dict[str, Dict[str, Any]] = {
# #     "bull": {"risk_reward":"1:2","trailing_sl":"1:1.5","total_trades":5,"risk_trade_1":2000,"volume_criteria":_default_volume_matrix(),"trade_start":"09:15","trade_end":"15:10"},
# #     "bear": {"risk_reward":"1:2","trailing_sl":"1:1.5","total_trades":5,"risk_trade_1":2000,"volume_criteria":_default_volume_matrix(),"trade_start":"09:15","trade_end":"15:10"},
# #     "mom_bull": {"risk_reward":"1:2","trailing_sl":"1:1.5","total_trades":5,"risk_trade_1":2000,"volume_criteria":_default_volume_matrix(),"trade_start":"09:15","trade_end":"09:17"},
# #     "mom_bear": {"risk_reward":"1:2","trailing_sl":"1:1.5","total_trades":5,"risk_trade_1":2000,"volume_criteria":_default_volume_matrix(),"trade_start":"09:15","trade_end":"09:17"},
# # }

# # RAM_STATE: Dict[str, Any] = {
# #     "main_loop": None,
# #     "tick_queue": None,
# #     "tick_worker_task": None,
# #     "kite": None,
# #     "kws": None,
# #     "api_key": "",
# #     "api_secret": "",
# #     "access_token": "",
# #     "stocks": {},
# #     "trades": {"bull": [], "bear": [], "mom_bull": [], "mom_bear": []},
# #     "engine_live": {"bull": True, "bear": True, "mom_bull": True, "mom_bear": True},
# #     "config": {
# #         "bull": dict(DEFAULT_CONFIG["bull"]),
# #         "bear": dict(DEFAULT_CONFIG["bear"]),
# #         "mom_bull": dict(DEFAULT_CONFIG["mom_bull"]),
# #         "mom_bear": dict(DEFAULT_CONFIG["mom_bear"]),
# #     },
# #     "manual_exits": set(),
# #     "data_connected": {"breakout": False, "momentum": False},
# # }

# # def _now_ist() -> datetime:
# #     return datetime.now(IST)

# # def _safe_int(x, default=0) -> int:
# #     try: return int(x)
# #     except Exception: return default

# # def _safe_float(x, default=0.0) -> float:
# #     try: return float(x)
# #     except Exception: return default

# # def _compute_pnl() -> Dict[str, float]:
# #     pnl = {k: 0.0 for k in ["bull","bear","mom_bull","mom_bear"]}
# #     for side in pnl.keys():
# #         pnl[side] = float(sum(float(t.get("pnl", 0.0) or 0.0) for t in RAM_STATE["trades"].get(side, [])))
# #     pnl["total"] = float(sum(pnl[s] for s in ["bull","bear","mom_bull","mom_bear"]))
# #     return pnl

# # # -----------------------------
# # # CENTRALIZED CANDLE AGGREGATION
# # # -----------------------------
# # def _update_candle(stock: dict, ltp: float, vol: int):
# #     now = datetime.now(IST)
# #     bucket = now.replace(second=0, microsecond=0)

# #     c = stock.get("candle")
# #     if not c:
# #         stock["candle"] = {
# #             "bucket": bucket,
# #             "open": float(ltp),
# #             "high": float(ltp),
# #             "low": float(ltp),
# #             "close": float(ltp),
# #             "volume": 0,
# #         }
# #         stock["last_vol"] = int(vol)
# #         return None  # no closed candle

# #     if c["bucket"] != bucket:
# #         closed = c
# #         stock["candle"] = {
# #             "bucket": bucket,
# #             "open": float(ltp),
# #             "high": float(ltp),
# #             "low": float(ltp),
# #             "close": float(ltp),
# #             "volume": 0,
# #         }
# #         stock["last_vol"] = int(vol)
# #         return closed

# #     # same candle
# #     c["high"] = max(float(c["high"]), float(ltp))
# #     c["low"] = min(float(c["low"]), float(ltp))
# #     c["close"] = float(ltp)

# #     last_vol = int(stock.get("last_vol", 0) or 0)
# #     if last_vol > 0:
# #         c["volume"] += max(0, int(vol) - last_vol)
# #     stock["last_vol"] = int(vol)
# #     return None

# # # -----------------------------
# # # TICK WORKER
# # # -----------------------------
# # async def tick_worker():
# #     q: asyncio.Queue = RAM_STATE["tick_queue"]
# #     logger.info("🧵 tick_worker running")

# #     while True:
# #         ticks = await q.get()
# #         try:
# #             for tick in ticks:
# #                 token = _safe_int(tick.get("instrument_token", 0), 0)
# #                 if token <= 0:
# #                     continue

# #                 stock = RAM_STATE["stocks"].get(token)
# #                 if not stock:
# #                     continue

# #                 ltp = _safe_float(tick.get("last_price", 0.0), 0.0)
# #                 vol = _safe_int(tick.get("volume_traded", 0), 0)
# #                 if ltp <= 0:
# #                     continue

# #                 stock["ltp"] = ltp

# #                 # ✅ centralized candle aggregation (always)
# #                 closed = _update_candle(stock, ltp, vol)
# #                 if closed:
# #                     # pass same closed candle to both engines
# #                     asyncio.create_task(BreakoutEngine.on_candle_close(token, closed, RAM_STATE))
# #                     asyncio.create_task(MomentumEngine.on_candle_close(token, closed, RAM_STATE))

# #                 # ✅ always run both engines (they self-gate entries by toggle)
# #                 await BreakoutEngine.run(token, ltp, vol, RAM_STATE)
# #                 await MomentumEngine.run(token, ltp, vol, RAM_STATE)

# #             RAM_STATE["data_connected"]["breakout"] = True
# #             RAM_STATE["data_connected"]["momentum"] = True

# #         except Exception:
# #             logger.exception("❌ tick_worker crashed while processing ticks")
# #         finally:
# #             q.task_done()

# # # def on_ticks(ws, ticks):
# # #     loop = RAM_STATE.get("main_loop")
# # #     q = RAM_STATE.get("tick_queue")
# # #     if not loop or not q:
# # #         return

# # #     def _put():
# # #         try:
# # #             q.put_nowait(ticks)
# # #         except asyncio.QueueFull:
# # #             try:
# # #                 _ = q.get_nowait()
# # #                 q.task_done()
# # #             except Exception:
# # #                 pass
# # #             try:
# # #                 q.put_nowait(ticks)
# # #             except Exception:
# # #                 pass

# # #     loop.call_soon_threadsafe(_put)
# # # add near TICK_STATS
# # TICK_STATS = {
# #     "received": 0,
# #     "dropped_batches": 0,
# #     "last_hb": datetime.now(IST),
# # }

# # def on_ticks(ws, ticks):
# #     global TICK_STATS

# #     loop = RAM_STATE.get("main_loop")
# #     q = RAM_STATE.get("tick_queue")
# #     if not loop or not q:
# #         return

# #     # heartbeat stats
# #     try:
# #         TICK_STATS["received"] += len(ticks)
# #         now = datetime.now(IST)
# #         if (now - TICK_STATS["last_hb"]).total_seconds() >= 5:
# #             logger.info(
# #                 f"💓 TICKS: +{TICK_STATS['received']} in last 5s | "
# #                 f"queue={q.qsize()} | dropped_batches={TICK_STATS['dropped_batches']}"
# #             )
# #             TICK_STATS["received"] = 0
# #             TICK_STATS["last_hb"] = now
# #     except Exception:
# #         pass

# #     def _put():
# #         try:
# #             q.put_nowait(ticks)
# #         except asyncio.QueueFull:
# #             TICK_STATS["dropped_batches"] += 1
# #             # Drop one old batch and insert newest
# #             try:
# #                 _ = q.get_nowait()
# #             except Exception:
# #                 pass
# #             try:
# #                 q.put_nowait(ticks)
# #             except Exception:
# #                 pass
# #             # ✅ Log drops (not on every drop to avoid spam)
# #             if TICK_STATS["dropped_batches"] % 10 == 0:
# #                 logger.warning(f"⚠️ tick_queue FULL -> dropped_batches={TICK_STATS['dropped_batches']} (consider raising maxsize)")

# #     loop.call_soon_threadsafe(_put)

# # def on_connect(ws, response):
# #     logger.info("✅ TICKER: Connected. Subscribing to universe tokens...")

# #     async def _sub():
# #         tokens = await TradeControl.get_subscribe_universe_tokens()
# #         tokens = [int(x) for x in tokens if int(x) > 0]
# #         if not tokens:
# #             tokens = list(RAM_STATE["stocks"].keys())
# #             logger.warning(f"⚠️ WS: universe empty; fallback subscribe RAM stocks = {len(tokens)}")

# #         tokens = [t for t in tokens if t in RAM_STATE["stocks"]]
# #         if not tokens:
# #             logger.error("❌ WS: No valid tokens to subscribe (RAM_STATE empty).")
# #             return

# #         ws.subscribe(tokens)
# #         ws.set_mode(ws.MODE_FULL, tokens)
# #         logger.info(f"📡 WS: Subscribed {len(tokens)} tokens (MODE_FULL).")

# #         RAM_STATE["data_connected"]["breakout"] = True
# #         RAM_STATE["data_connected"]["momentum"] = True

# #     loop = RAM_STATE.get("main_loop")
# #     if loop:
# #         asyncio.run_coroutine_threadsafe(_sub(), loop)

# # def on_error(ws, code, reason):
# #     logger.error(f"❌ TICKER ERROR: {code} - {reason}")
# #     RAM_STATE["data_connected"]["breakout"] = False
# #     RAM_STATE["data_connected"]["momentum"] = False

# # def on_close(ws, code, reason):
# #     logger.warning(f"⚠️ TICKER CLOSED: ({code}) {reason}")
# #     RAM_STATE["data_connected"]["breakout"] = False
# #     RAM_STATE["data_connected"]["momentum"] = False

# # def _start_kiteticker():
# #     # Close old
# #     try:
# #         kws = RAM_STATE.get("kws")
# #         if kws:
# #             kws.close()
# #     except Exception:
# #         pass

# #     api_key = RAM_STATE.get("api_key") or ""
# #     access_token = RAM_STATE.get("access_token") or ""
# #     if not api_key or not access_token:
# #         return False

# #     try:
# #         kws = KiteTicker(api_key, access_token)
# #         kws.on_ticks = on_ticks
# #         kws.on_connect = on_connect
# #         kws.on_error = on_error
# #         kws.on_close = on_close
# #         kws.connect(threaded=True)
# #         RAM_STATE["kws"] = kws
# #         logger.info("🛰️ WS: KiteTicker started (threaded=True).")
# #         return True
# #     except Exception as e:
# #         logger.exception(f"❌ WS START FAILED: {e}")
# #         RAM_STATE["kws"] = None
# #         return False

# # @app.on_event("startup")
# # async def startup_event():
# #     logger.info("--- 🚀 NEXUS BOOT ---")
# #     RAM_STATE["main_loop"] = asyncio.get_running_loop()
# #     RAM_STATE["tick_queue"] = asyncio.Queue(maxsize=2000)
# #     RAM_STATE["tick_worker_task"] = asyncio.create_task(tick_worker())

# #     key_redis, sec_redis = await TradeControl.get_config()
# #     RAM_STATE["api_key"] = str(key_redis or os.getenv("KITE_API_KEY", "") or "")
# #     RAM_STATE["api_secret"] = str(sec_redis or os.getenv("KITE_API_SECRET", "") or "")

# #     token = await TradeControl.get_access_token()
# #     RAM_STATE["access_token"] = str(token or "")

# #     cached = await TradeControl.get_all_market_data()
# #     stocks: Dict[int, Dict[str, Any]] = {}

# #     for token_str, md in (cached or {}).items():
# #         try:
# #             t_id = int(token_str)
# #         except Exception:
# #             continue

# #         stocks[t_id] = {
# #             "token": t_id,
# #             "symbol": md.get("symbol"),
# #             "sma": _safe_float(md.get("sma", 0), 0.0),
# #             "pdh": _safe_float(md.get("pdh", 0), 0.0),
# #             "pdl": _safe_float(md.get("pdl", 0), 0.0),
# #             "prev_close": _safe_float(md.get("prev_close", 0), 0.0),
# #             "sync_time": md.get("sync_time", ""),

# #             # shared market runtime
# #             "ltp": 0.0,
# #             "candle": None,
# #             "last_vol": 0,

# #             # breakout engine state
# #             "brk_status": "WAITING",
# #             "brk_active_trade": None,
# #             "brk_side_latch": None,
# #             "brk_trigger_px": None,
# #             "brk_trigger_set_ts": None,
# #             "brk_trigger_candle": None,
# #             "brk_scan_seen_ts": None,
# #             "brk_scan_seen_time": None,
# #             "brk_scan_vol": 0,
# #             "brk_scan_reason": None,

# #             # momentum engine state
# #             "mom_status": "WAITING",
# #             "mom_active_trade": None,
# #             "mom_side_latch": None,
# #             "mom_trigger_px": None,
# #             "mom_trigger_candle": None,
# #             "mom_scan_seen_ts": None,
# #             "mom_scan_seen_time": None,
# #             "mom_scan_vol": 0,
# #             "mom_scan_reason": None,
# #         }

# #     RAM_STATE["stocks"] = stocks
# #     logger.info(f"✅ STARTUP: Loaded {len(stocks)} stocks from Redis market_cache.")

# #     for side in ["bull", "bear", "mom_bull", "mom_bear"]:
# #         saved = await TradeControl.get_strategy_settings(side)
# #         if saved:
# #             RAM_STATE["config"][side].update(saved)

# #     if RAM_STATE["api_key"] and RAM_STATE["access_token"]:
# #         try:
# #             RAM_STATE["kite"] = KiteConnect(api_key=RAM_STATE["api_key"])
# #             RAM_STATE["kite"].set_access_token(RAM_STATE["access_token"])
# #         except Exception:
# #             RAM_STATE["kite"] = None

# #         _start_kiteticker()
# #     else:
# #         logger.warning("⚠️ WS: Not started (missing api_key or access_token).")

# # @app.on_event("shutdown")
# # async def shutdown_event():
# #     try:
# #         kws = RAM_STATE.get("kws")
# #         if kws:
# #             kws.close()
# #     except Exception:
# #         pass

# # @app.get("/", response_class=HTMLResponse)
# # async def home():
# #     try:
# #         return FileResponse("index.html")
# #     except Exception:
# #         return HTMLResponse("<h3>Nexus backend is running. Place index.html next to main.py</h3>")

# # # @app.get("/login/")
# # # async def login(request_token: Optional[str] = None):
# # #     api_key = RAM_STATE["api_key"] or os.getenv("KITE_API_KEY", "")
# # #     api_secret = RAM_STATE["api_secret"] or os.getenv("KITE_API_SECRET", "")

# # #     if not api_key:
# # #         k, s = await TradeControl.get_config()
# # #         api_key = api_key or (k or "")
# # #         api_secret = api_secret or (s or "")

# # #     if not api_key:
# # #         return HTMLResponse("<h3>Save API KEY first (API Gateway button)</h3>")

# # #     if not request_token:
# # #         try:
# # #             url = KiteConnect(api_key=str(api_key)).login_url()
# # #             return RedirectResponse(url=url)
# # #         except Exception as e:
# # #             return HTMLResponse(f"<h3>Login URL error: {e}</h3>")

# # #     try:
# # #         kite = KiteConnect(api_key=str(api_key))
# # #         data = await asyncio.to_thread(kite.generate_session, request_token, api_secret=str(api_secret))
# # #         access_token = data["access_token"]

# # #         await TradeControl.save_access_token(access_token)

# # #         RAM_STATE["access_token"] = str(access_token)
# # #         RAM_STATE["api_key"] = str(api_key)
# # #         RAM_STATE["api_secret"] = str(api_secret)

# # #         RAM_STATE["kite"] = kite
# # #         RAM_STATE["kite"].set_access_token(access_token)

# # #         # ✅ Restart WS immediately after login
# # #         _start_kiteticker()

# # #         logger.info("✅ AUTH: Session established & saved to Redis.")
# # #         return RedirectResponse(url="/")
# # #     except Exception as e:
# # #         logger.exception(f"❌ AUTH ERROR: {e}")
# # #         return HTMLResponse(f"<h3>Auth failed: {e}</h3>")

# # @app.get("/login", include_in_schema=False)
# # @app.get("/login/", include_in_schema=False)
# # async def login(request: Request, request_token: Optional[str] = None, status: Optional[str] = None):
# #     """
# #     Handles both:
# #       /login
# #       /login/
# #     and accepts Zerodha callback params safely.
# #     """

# #     # If Zerodha returned non-success
# #     if status and str(status).lower() != "success":
# #         return HTMLResponse(f"<h3>Login not successful. status={status}</h3>")

# #     api_key = RAM_STATE["api_key"] or os.getenv("KITE_API_KEY", "")
# #     api_secret = RAM_STATE["api_secret"] or os.getenv("KITE_API_SECRET", "")

# #     if not api_key:
# #         k, s = await TradeControl.get_config()
# #         api_key = api_key or (k or "")
# #         api_secret = api_secret or (s or "")

# #     if not api_key:
# #         return HTMLResponse("<h3>Save API KEY first (API Gateway button)</h3>")

# #     # Step-1: Redirect to Kite login if no request_token
# #     if not request_token:
# #         try:
# #             url = KiteConnect(api_key=str(api_key)).login_url()
# #             return RedirectResponse(url=url)
# #         except Exception as e:
# #             return HTMLResponse(f"<h3>Login URL error: {e}</h3>")

# #     # Step-2: Exchange request_token -> access_token
# #     try:
# #         kite = KiteConnect(api_key=str(api_key))
# #         data = await asyncio.to_thread(kite.generate_session, request_token, api_secret=str(api_secret))
# #         access_token = data["access_token"]

# #         await TradeControl.save_access_token(access_token)

# #         RAM_STATE["access_token"] = str(access_token)
# #         RAM_STATE["api_key"] = str(api_key)
# #         RAM_STATE["api_secret"] = str(api_secret)

# #         RAM_STATE["kite"] = kite
# #         RAM_STATE["kite"].set_access_token(access_token)

# #         # ✅ IMPORTANT: restart websocket immediately after login
# #         try:
# #             old = RAM_STATE.get("kws")
# #             if old:
# #                 old.close()
# #         except Exception:
# #             pass

# #         try:
# #             RAM_STATE["kws"] = KiteTicker(RAM_STATE["api_key"], RAM_STATE["access_token"])
# #             RAM_STATE["kws"].on_ticks = on_ticks
# #             RAM_STATE["kws"].on_connect = on_connect
# #             RAM_STATE["kws"].on_error = on_error
# #             RAM_STATE["kws"].on_close = on_close
# #             RAM_STATE["kws"].connect(threaded=True)
# #             logger.info("🛰️ WS: Restarted KiteTicker after login.")
# #         except Exception as e:
# #             logger.exception(f"❌ WS restart failed after login: {e}")

# #         logger.info("✅ AUTH: Session established & saved to Redis.")
# #         return RedirectResponse(url="/")

# #     except Exception as e:
# #         logger.exception(f"❌ AUTH ERROR: {e}")
# #         return HTMLResponse(f"<h3>Auth failed: {e}</h3>")
    
# # @app.get("/api/stats")
# # async def get_stats():
# #     pnl = _compute_pnl()
# #     return {
# #         "pnl": pnl,
# #         "engine_status": {k: "1" if bool(v) else "0" for k, v in RAM_STATE["engine_live"].items()},
# #         "data_connected": RAM_STATE.get("data_connected", {"breakout": False, "momentum": False}),
# #         "stock_count": len(RAM_STATE["stocks"]),
# #         "server_time": _now_ist().strftime("%H:%M:%S"),
# #     }

# # @app.get("/api/orders")
# # async def get_orders():
# #     return RAM_STATE["trades"]

# # @app.get("/api/settings/engine/{side}")
# # async def get_engine_settings(side: str):
# #     return RAM_STATE["config"].get(side, {})

# # @app.post("/api/settings/engine/{side}")
# # async def save_engine_settings(side: str, data: Dict[str, Any]):
# #     if side in RAM_STATE["config"]:
# #         RAM_STATE["config"][side].update(data)
# #         await TradeControl.save_strategy_settings(side, RAM_STATE["config"][side])
# #     return {"status": "success"}

# # @app.get("/api/scanner")
# # async def get_scanner():
# #     signals = {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]}
# #     now = _now_ist()

# #     for _, s in RAM_STATE["stocks"].items():
# #         sym = s.get("symbol") or ""
# #         if not sym:
# #             continue

# #         # breakout scanner
# #         if s.get("brk_status") == "TRIGGER_WATCH":
# #             side = (s.get("brk_side_latch") or "").lower()
# #             if side in ("bull", "bear"):
# #                 if not s.get("brk_scan_seen_ts"):
# #                     s["brk_scan_seen_ts"] = int(now.timestamp())
# #                     s["brk_scan_seen_time"] = now.strftime("%H:%M:%S")

# #                 trig = s.get("brk_trigger_px", 0)
# #                 vol = int(s.get("brk_scan_vol", 0) or 0)
# #                 sma = float(s.get("sma", 0) or 0)
# #                 rel = (float(vol) / sma) if sma > 0 else None

# #                 prev_close = float(s.get("prev_close", 0) or 0)
# #                 ltp = float(s.get("ltp", 0) or 0)
# #                 pct = ((ltp - prev_close) / prev_close * 100.0) if prev_close > 0 and ltp > 0 else None

# #                 signals[side].append({
# #                     "symbol": sym,
# #                     "trigger_px": trig,
# #                     "seen_time": s.get("brk_scan_seen_time"),
# #                     "seen_ts": s.get("brk_scan_seen_ts"),
# #                     "volume": vol,
# #                     "rel_vol": rel,
# #                     "pct_change": pct,
# #                     "chart_url": f"https://www.tradingview.com/chart/?symbol=NSE:{sym}",
# #                     "reason": s.get("brk_scan_reason", "Candle Confirmed"),
# #                 })

# #         # momentum scanner
# #         if s.get("mom_status") == "TRIGGER_WATCH":
# #             side = (s.get("mom_side_latch") or "").lower()
# #             if side in ("mom_bull", "mom_bear"):
# #                 if not s.get("mom_scan_seen_ts"):
# #                     s["mom_scan_seen_ts"] = int(now.timestamp())
# #                     s["mom_scan_seen_time"] = now.strftime("%H:%M:%S")

# #                 trig = s.get("mom_trigger_px", 0)
# #                 vol = int(s.get("mom_scan_vol", 0) or 0)
# #                 sma = float(s.get("sma", 0) or 0)
# #                 rel = (float(vol) / sma) if sma > 0 else None

# #                 prev_close = float(s.get("prev_close", 0) or 0)
# #                 ltp = float(s.get("ltp", 0) or 0)
# #                 pct = ((ltp - prev_close) / prev_close * 100.0) if prev_close > 0 and ltp > 0 else None

# #                 signals[side].append({
# #                     "symbol": sym,
# #                     "trigger_px": trig,
# #                     "seen_time": s.get("mom_scan_seen_time"),
# #                     "seen_ts": s.get("mom_scan_seen_ts"),
# #                     "volume": vol,
# #                     "rel_vol": rel,
# #                     "pct_change": pct,
# #                     "chart_url": f"https://www.tradingview.com/chart/?symbol=NSE:{sym}",
# #                     "reason": s.get("mom_scan_reason", "Candle Confirmed"),
# #                 })

# #     for side in signals:
# #         signals[side].sort(key=lambda x: x.get("seen_ts", 0), reverse=True)

# #     return signals

# # @app.post("/api/control")
# # async def control(request: Request):
# #     data = await request.json()
# #     action = data.get("action")

# #     if action == "toggle_engine":
# #         side = data.get("side")
# #         enabled = bool(data.get("enabled"))
# #         if side in RAM_STATE["engine_live"]:
# #             RAM_STATE["engine_live"][side] = enabled
# #         return {"status": "ok"}

# #     if action == "save_api":
# #         api_key = data.get("api_key", "") or ""
# #         api_secret = data.get("api_secret", "") or ""
# #         RAM_STATE["api_key"] = str(api_key)
# #         RAM_STATE["api_secret"] = str(api_secret)
# #         await TradeControl.save_config(api_key, api_secret)
# #         return {"status": "ok"}

# #     if action == "manual_exit":
# #         symbol = data.get("symbol")
# #         if symbol:
# #             RAM_STATE["manual_exits"].add(symbol)
# #         return {"status": "ok"}

# #     if action == "square_off_one":
# #         side = data.get("side")
# #         symbol = data.get("symbol")
# #         if not side or not symbol:
# #             return {"status": "error", "message": "side/symbol missing"}

# #         stock = next((s for s in RAM_STATE["stocks"].values() if s.get("symbol") == symbol), None)
# #         if not stock:
# #             return {"status": "error", "message": "symbol not found"}

# #         try:
# #             if side in ("bull", "bear") and stock.get("brk_status") == "OPEN" and (stock.get("brk_side_latch") or "").lower() == side:
# #                 await BreakoutEngine.close_position(stock, RAM_STATE, "UI_EXIT_ONE")
# #             elif side in ("mom_bull", "mom_bear") and stock.get("mom_status") == "OPEN" and (stock.get("mom_side_latch") or "").lower() == side:
# #                 await MomentumEngine.close_position(stock, RAM_STATE, "UI_EXIT_ONE")
# #             else:
# #                 return {"status": "ok", "message": "no open position for that side"}
# #         except Exception as e:
# #             return {"status": "error", "message": str(e)}

# #         return {"status": "ok"}

# #     if action == "square_off_all":
# #         side = data.get("side")
# #         if side not in ("bull", "bear", "mom_bull", "mom_bear"):
# #             return {"status": "error", "message": "invalid side"}

# #         try:
# #             if side in ("bull", "bear"):
# #                 for s in list(RAM_STATE["stocks"].values()):
# #                     if s.get("brk_status") == "OPEN" and (s.get("brk_side_latch") or "").lower() == side:
# #                         await BreakoutEngine.close_position(s, RAM_STATE, "UI_EXIT_ALL")
# #             else:
# #                 for s in list(RAM_STATE["stocks"].values()):
# #                     if s.get("mom_status") == "OPEN" and (s.get("mom_side_latch") or "").lower() == side:
# #                         await MomentumEngine.close_position(s, RAM_STATE, "UI_EXIT_ALL")
# #         except Exception as e:
# #             return {"status": "error", "message": str(e)}

# #         return {"status": "ok"}

# #     return {"status": "error", "message": "unknown action"}

# # @app.post("/api/tick")
# # async def ingest_tick(data: Dict[str, Any]):
# #     token = int(data.get("token", 0) or 0)
# #     ltp = float(data.get("ltp", 0) or 0)
# #     vol = int(data.get("vol", 0) or 0)

# #     stock = RAM_STATE["stocks"].get(token)
# #     if not stock:
# #         return {"status": "error", "message": "token not found"}

# #     stock["ltp"] = ltp

# #     closed = _update_candle(stock, ltp, vol)
# #     if closed:
# #         asyncio.create_task(BreakoutEngine.on_candle_close(token, closed, RAM_STATE))
# #         asyncio.create_task(MomentumEngine.on_candle_close(token, closed, RAM_STATE))

# #     await BreakoutEngine.run(token, ltp, vol, RAM_STATE)
# #     await MomentumEngine.run(token, ltp, vol, RAM_STATE)

# #     RAM_STATE["data_connected"]["breakout"] = True
# #     RAM_STATE["data_connected"]["momentum"] = True
# #     return {"status": "ok"}

# import asyncio
# import os
# import logging
# import json
# from datetime import datetime
# from typing import Dict, Any, Optional, List

# import pytz
# from fastapi import FastAPI, Request
# from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
# from fastapi.middleware.cors import CORSMiddleware
# from kiteconnect import KiteConnect, KiteTicker

# # Internal Component Imports
# from redis_manager import TradeControl, IST
# from breakout_engine import BreakoutEngine
# from momentum_engine import MomentumEngine

# # --- LOGGING SETUP ---
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] Nexus_Main: %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S"
# )
# logger = logging.getLogger("Nexus_Core")

# # --- FASTAPI APP CONFIG ---
# app = FastAPI(title="Nexus Core HFT", version="4.5.0")

# # Enable CORS for Heroku/Cloud Environment
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # --- GLOBAL RAM STATE ---
# # This dictionary holds all runtime data for low-latency access.
# RAM_STATE: Dict[str, Any] = {
#     "main_loop": None,
#     "tick_queue": None,
#     "kite": None,
#     "kws": None,
#     "api_key": "",
#     "api_secret": "",
#     "access_token": "",
#     "stocks": {},  # Stores instrument data, tech levels, and engine statuses
#     "trades": {
#         "bull": [], "bear": [], 
#         "mom_bull": [], "mom_bear": []
#     },
#     "engine_live": {
#         "bull": True, "bear": True, 
#         "mom_bull": True, "mom_bear": True
#     },
#     "config": {
#         "bull": {}, "bear": {}, 
#         "mom_bull": {}, "mom_bear": {}
#     },
#     "data_connected": {"breakout": False, "momentum": False},
# }

# # --- CANDLE AGGREGATOR ---
# def _update_candle(stock: dict, ltp: float, vol: int):
#     """
#     Ticks ko 1-minute candles mein aggregate karta hai.
#     Sahi PnL aur Strategy trigger ke liye zaroori hai.
#     """
#     now = datetime.now(IST)
#     bucket = now.replace(second=0, microsecond=0)
#     c = stock.get("candle")

#     if not c:
#         stock["candle"] = {
#             "bucket": bucket, "open": ltp, "high": ltp, 
#             "low": ltp, "close": ltp, "volume": 0
#         }
#         stock["last_vol"] = vol
#         return None

#     if c["bucket"] != bucket:
#         closed = c
#         stock["candle"] = {
#             "bucket": bucket, "open": ltp, "high": ltp, 
#             "low": ltp, "close": ltp, "volume": 0
#         }
#         stock["last_vol"] = vol
#         return closed

#     # Same candle updates
#     c["high"] = max(c["high"], ltp)
#     c["low"] = min(c["low"], ltp)
#     c["close"] = ltp
    
#     last_vol = stock.get("last_vol", 0)
#     if last_vol > 0:
#         c["volume"] += max(0, vol - last_vol)
#     stock["last_vol"] = vol
#     return None

# # --- HIGH-CONCURRENCY TICK WORKER ---
# async def tick_worker():
#     """
#     CORE ENGINE LOOP:
#     1. Queues se tick batches uthata hai.
#     2. Dono strategies (Breakout & Momentum) ko PARALLEL chalaata hai.
#     3. Latency fix: asyncio.gather ensures Strategy A doesn't wait for Strategy B.
#     """
#     logger.info("🧵 Tick Worker Starting: Parallel Execution Mode")
#     q: asyncio.Queue = RAM_STATE["tick_queue"]

#     while True:
#         batch = await q.get()
#         try:
#             processing_tasks = []
#             for tick in batch:
#                 token = tick.get("instrument_token")
#                 stock = RAM_STATE["stocks"].get(token)
#                 if not stock: continue

#                 ltp = float(tick.get("last_price", 0))
#                 vol = int(tick.get("volume_traded", 0))
#                 if ltp <= 0: continue

#                 stock["ltp"] = ltp

#                 # 1. Candle Formation
#                 closed_candle = _update_candle(stock, ltp, vol)
#                 if closed_candle:
#                     # Strategy Qualification on Candle Close (Parallel tasks)
#                     processing_tasks.append(BreakoutEngine.on_candle_close(token, closed_candle, RAM_STATE))
#                     processing_tasks.append(MomentumEngine.on_candle_close(token, closed_candle, RAM_STATE))

#                 # 2. Strategy Execution on every tick (Parallel tasks)
#                 processing_tasks.append(BreakoutEngine.run(token, ltp, vol, RAM_STATE))
#                 processing_tasks.append(MomentumEngine.run(token, ltp, vol, RAM_STATE))

#             if processing_tasks:
#                 # Execution of all strategy checks for the batch simultaneously
#                 await asyncio.gather(*processing_tasks)

#         except Exception as e:
#             logger.error(f"❌ Worker Execution Error: {e}", exc_info=True)
#         finally:
#             q.task_done()

# # --- KITE TICKER HANDLERS ---
# def on_ticks(ws, ticks):
#     """Bridge: Kite (Thread) -> FastAPI (Async Loop)"""
#     loop = RAM_STATE.get("main_loop")
#     q = RAM_STATE.get("tick_queue")
#     if loop and q:
#         # thread-safe injection into async queue
#         loop.call_soon_threadsafe(q.put_nowait, ticks)

# def on_connect(ws, response):
#     logger.info("📡 WS: Connection successful. Mapping tokens...")
#     tokens = list(RAM_STATE["stocks"].keys())
#     if tokens:
#         ws.subscribe(tokens)
#         ws.set_mode(ws.MODE_FULL, tokens)
#         RAM_STATE["data_connected"] = {"breakout": True, "momentum": True}

# def on_close(ws, code, reason):
#     logger.warning(f"⚠️ WS: Closed ({code}) - {reason}")
#     RAM_STATE["data_connected"] = {"breakout": False, "momentum": False}

# def on_error(ws, code, reason):
#     logger.error(f"❌ WS: Error ({code}) - {reason}")

# # --- API ENDPOINTS ---
# @app.on_event("startup")
# async def startup_event():
#     logger.info("--- 🚀 NEXUS SYSTEM INITIALIZING ---")
#     RAM_STATE["main_loop"] = asyncio.get_running_loop()
#     # Large queue size to handle Heroku bursts
#     RAM_STATE["tick_queue"] = asyncio.Queue(maxsize=30000)
#     asyncio.create_task(tick_worker())

#     # 1. Load Keys & Tokens
#     api_key, api_secret = await TradeControl.get_config()
#     access_token = await TradeControl.get_access_token()
#     RAM_STATE.update({
#         "api_key": api_key, "api_secret": api_secret, "access_token": access_token
#     })

#     # 2. Populate RAM from Redis Morning Sync
#     market_cache = await TradeControl.get_all_market_data()
#     for t_str, data in market_cache.items():
#         t_id = int(t_str)
#         data.update({
#             "token": t_id, "ltp": 0.0, "candle": None, "last_vol": 0,
#             "brk_status": "WAITING", "mom_status": "WAITING"
#         })
#         RAM_STATE["stocks"][t_id] = data
    
#     # 3. Load Engine Configs
#     for side in ["bull", "bear", "mom_bull", "mom_bear"]:
#         cfg = await TradeControl.get_strategy_settings(side)
#         if cfg: RAM_STATE["config"][side] = cfg

#     # 4. Kite Handshake
#     if RAM_STATE["api_key"] and RAM_STATE["access_token"]:
#         try:
#             RAM_STATE["kite"] = KiteConnect(api_key=RAM_STATE["api_key"])
#             RAM_STATE["kite"].set_access_token(RAM_STATE["access_token"])
            
#             RAM_STATE["kws"] = KiteTicker(RAM_STATE["api_key"], RAM_STATE["access_token"])
#             RAM_STATE["kws"].on_ticks = on_ticks
#             RAM_STATE["kws"].on_connect = on_connect
#             RAM_STATE["kws"].on_close = on_close
#             RAM_STATE["kws"].on_error = on_error
#             RAM_STATE["kws"].connect(threaded=True)
#             logger.info(f"✅ Startup Complete: {len(RAM_STATE['stocks'])} stocks live.")
#         except Exception as e:
#             logger.error(f"❌ Kite Start Error: {e}")

# @app.get("/", response_class=HTMLResponse)
# async def home():
#     try:
#         return FileResponse("index.html")
#     except Exception:
#         return HTMLResponse("<h3>Nexus dashboard file (index.html) missing in root.</h3>")

# @app.get("/login")
# async def login(request_token: Optional[str] = None):
#     """Zerodha OAuth Flow: Handles Heroku redirects."""
#     api_key = RAM_STATE["api_key"] or os.getenv("KITE_API_KEY")
#     if not api_key:
#         return HTMLResponse("<h3>Error: Save API KEY via API Gateway first.</h3>")

#     if not request_token:
#         url = KiteConnect(api_key=api_key).login_url()
#         return RedirectResponse(url=url)
    
#     try:
#         kite = KiteConnect(api_key=api_key)
#         data = kite.generate_session(request_token, api_secret=RAM_STATE["api_secret"])
#         new_token = data["access_token"]
#         await TradeControl.save_access_token(new_token)
#         RAM_STATE["access_token"] = new_token
#         # Trigger Restart of Websocket
#         if RAM_STATE["kws"]: RAM_STATE["kws"].close()
#         return RedirectResponse(url="/")
#     except Exception as e:
#         return HTMLResponse(f"<h3>Login Failed: {e}</h3>")

# @app.get("/api/stats")
# async def get_stats():
#     """PnL calculation and system health for Dashboard."""
#     pnl = {"total": 0.0, "bull": 0.0, "bear": 0.0, "mom_bull": 0.0, "mom_bear": 0.0}
#     for side in ["bull", "bear", "mom_bull", "mom_bear"]:
#         # Only sum pnl of trades belonging to that side
#         side_sum = sum(float(t.get("pnl", 0)) for t in RAM_STATE["trades"].get(side, []))
#         pnl[side] = round(side_sum, 2)
#     pnl["total"] = round(sum(v for k,v in pnl.items() if k != "total"), 2)

#     return {
#         "connected": all(RAM_STATE["data_connected"].values()),
#         "data_connected": RAM_STATE["data_connected"],
#         "stock_count": len(RAM_STATE["stocks"]),
#         "queue_size": RAM_STATE["tick_queue"].qsize(),
#         "server_time": datetime.now(IST).strftime("%H:%M:%S"),
#         "engine_status": {k: "1" if v else "0" for k, v in RAM_STATE["engine_live"].items()},
#         "pnl": pnl
#     }

# @app.get("/api/orders")
# async def get_orders():
#     """Returns raw trade history for frontend table."""
#     return RAM_STATE["trades"]

# @app.get("/api/scanner")
# async def get_scanner():
#     """Real-time signals before execution."""
#     signals = {"bull": [], "bear": [], "mom_bull": [], "mom_bear": []}
#     for t_id, s in RAM_STATE["stocks"].items():
#         # Breakout scan results
#         if s.get("brk_status") == "TRIGGER_WATCH":
#             side = s.get("brk_side_latch")
#             signals[side].append({
#                 "symbol": s["symbol"], "trigger_px": s["brk_trigger_px"],
#                 "reason": s.get("brk_scan_reason", "Breakout Zone"),
#                 "seen_time": datetime.now(IST).strftime("%H:%M:%S"),
#                 "seen_ts": s.get("brk_set_ts")
#             })
#         # Momentum scan results
#         if s.get("mom_status") == "TRIGGER_WATCH":
#             side = s.get("mom_side_latch")
#             signals[side].append({
#                 "symbol": s["symbol"], "trigger_px": s["mom_trigger_px"],
#                 "reason": s.get("mom_scan_reason", "Momentum Spike"),
#                 "seen_time": datetime.now(IST).strftime("%H:%M:%S")
#             })
#     return signals

# @app.get("/api/settings/engine/{side}")
# async def get_engine_settings(side: str):
#     return RAM_STATE["config"].get(side, {})

# @app.post("/api/settings/engine/{side}")
# async def save_engine_settings(side: str, data: Dict[str, Any]):
#     RAM_STATE["config"][side].update(data)
#     await TradeControl.save_strategy_settings(side, RAM_STATE["config"][side])
#     return {"status": "success"}

# @app.post("/api/control")
# async def control(request: Request):
#     """User actions from Dashboard (Toggles, Manual Exits)."""
#     data = await request.json()
#     action = data.get("action")
    
#     if action == "toggle_engine":
#         side, enabled = data.get("side"), data.get("enabled")
#         if side in RAM_STATE["engine_live"]:
#             RAM_STATE["engine_live"][side] = bool(enabled)
#         return {"status": "ok"}
    
#     if action == "square_off_one":
#         side, symbol = data.get("side"), data.get("symbol")
#         stock = next((s for s in RAM_STATE["stocks"].values() if s["symbol"] == symbol), None)
#         if stock:
#             # Route exit request to the correct engine module
#             if "mom" in side: await MomentumEngine.close_position(stock, RAM_STATE, "USER_EXIT")
#             else: await BreakoutEngine.close_position(stock, RAM_STATE, "USER_EXIT")
#         return {"status": "ok"}

#     if action == "save_api":
#         api_key, api_secret = data.get("api_key"), data.get("api_secret")
#         await TradeControl.save_config(api_key, api_secret)
#         RAM_STATE["api_key"] = api_key
#         RAM_STATE["api_secret"] = api_secret
#         return {"status": "ok"}

#     return {"status": "error", "message": "Unknown action"}

# # --- BOOTSTRAP (For Heroku & Gunicorn) ---
# if __name__ == "__main__":
#     import uvicorn
#     # Use environment $PORT for Heroku binding
#     port = int(os.environ.get("PORT", 8000))
#     uvicorn.run(app, host="0.0.0.0", port=port)

# main.py
import asyncio
import os
import logging
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from collections import defaultdict

import pytz
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

from kiteconnect import KiteConnect, KiteTicker

from redis_manager import TradeControl
from breakout_engine import BreakoutEngine
from momentum_engine import MomentumEngine

# -----------------------------
# ✅ HEROKU / GUNICORN TWISTED SIGNAL BYPASS (SAFE)
# -----------------------------
try:
    from twisted.internet import reactor  # type: ignore
    _original_run = reactor.run

    def _patched_reactor_run(*args, **kwargs):
        kwargs["installSignalHandlers"] = False
        return _original_run(*args, **kwargs)

    reactor.run = _patched_reactor_run
except Exception:
    pass

# -----------------------------
# LOGGING / TIMEZONE
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MAIN] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Nexus_Main")
IST = pytz.timezone("Asia/Kolkata")


# -----------------------------
# FASTAPI APP
# -----------------------------
app = FastAPI(title="Nexus Core", version="2.0.0", strict_slashes=False)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# DEFAULTS (SYNC WITH index.html)
# -----------------------------
def _default_volume_matrix() -> list:
    multipliers = [20, 18, 16, 14, 12, 10, 8, 6, 4, 2]
    out = []
    for i in range(10):
        out.append(
            {
                "min_vol_price_cr": float(i + 1),
                "sma_multiplier": float(multipliers[i]),
                "min_sma_avg": int(1000),
            }
        )
    return out


DEFAULT_CONFIG: Dict[str, Dict[str, Any]] = {
    "bull": {
        "risk_reward": "1:2",
        "trailing_sl": "1:1.5",
        "total_trades": 5,
        "risk_trade_1": 2000,
        "volume_criteria": _default_volume_matrix(),
        "trade_start": "09:15",
        "trade_end": "15:10",
    },
    "bear": {
        "risk_reward": "1:2",
        "trailing_sl": "1:1.5",
        "total_trades": 5,
        "risk_trade_1": 2000,
        "volume_criteria": _default_volume_matrix(),
        "trade_start": "09:15",
        "trade_end": "15:10",
    },
    "mom_bull": {
        "risk_reward": "1:2",
        "trailing_sl": "1:1.5",
        "total_trades": 5,
        "risk_trade_1": 2000,
        "volume_criteria": _default_volume_matrix(),
        "trade_start": "09:15",
        "trade_end": "09:17",
    },
    "mom_bear": {
        "risk_reward": "1:2",
        "trailing_sl": "1:1.5",
        "total_trades": 5,
        "risk_trade_1": 2000,
        "volume_criteria": _default_volume_matrix(),
        "trade_start": "09:15",
        "trade_end": "09:17",
    },
}

# -----------------------------
# RAM STATE
# -----------------------------
RAM_STATE: Dict[str, Any] = {
    "main_loop": None,              # asyncio loop
    "tick_queue": None,             # asyncio.Queue
    "tick_worker_task": None,       # task handle
    "kite": None,                   # KiteConnect
    "kws": None,                    # KiteTicker
    "api_key": "",
    "api_secret": "",
    "access_token": "",
    "stocks": {},                   # token -> stock dict
    "trades": {                     # trades by side
        "bull": [],
        "bear": [],
        "mom_bull": [],
        "mom_bear": [],
    },
    "engine_live": {                # engine toggles
        "bull": True,
        "bear": True,
        "mom_bull": True,
        "mom_bear": True,
    },
    "config": {
        "bull": dict(DEFAULT_CONFIG["bull"]),
        "bear": dict(DEFAULT_CONFIG["bear"]),
        "mom_bull": dict(DEFAULT_CONFIG["mom_bull"]),
        "mom_bear": dict(DEFAULT_CONFIG["mom_bear"]),
    },
    "manual_exits": set(),
    "data_connected": {"breakout": False, "momentum": False},

    # ---- parallel processing controls ----
    "token_locks": None,            # defaultdict(asyncio.Lock)
    "engine_sem": None,             # asyncio.Semaphore
    "inflight": set(),              # set[asyncio.Task]
    "max_inflight": 4000,
    "dropped_batches": 0,
    "tick_batches_enqueued": 0,
    "tick_batches_dropped": 0,

    # ---- candle close events ----
    "candle_close_queue": None,     # asyncio.Queue[(token, candle)]
    "candle_worker_task": None,
}

# -----------------------------
# HELPERS
# -----------------------------
def _now_ist() -> datetime:
    return datetime.now(IST)

def _safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def _safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _compute_pnl() -> Dict[str, float]:
    """
    Total PnL = realized + unrealized together.
    This assumes CLOSED trades retain their final pnl.
    """
    pnl = {k: 0.0 for k in ["bull", "bear", "mom_bull", "mom_bear"]}
    for side in pnl:
        pnl[side] = float(sum(float(t.get("pnl", 0.0) or 0.0) for t in RAM_STATE["trades"].get(side, [])))
    pnl["total"] = float(sum(pnl[s] for s in ["bull", "bear", "mom_bull", "mom_bear"]))
    return pnl

def _trade_is_open(t: dict) -> bool:
    st = str(t.get("status", "OPEN") or "OPEN").upper()
    return st == "OPEN"

def _stock_has_open_for_side(stock: dict, side: str) -> bool:
    # Supports both NEW (brk_/mom_) and LEGACY keys
    if side in ("bull", "bear"):
        brk_status = (stock.get("brk_status") or "").upper()
        if brk_status == "OPEN":
            return True
        if (stock.get("status") or "").upper() == "OPEN":
            return True
        return False

    if side in ("mom_bull", "mom_bear"):
        mom_status = (stock.get("mom_status") or "").upper()
        if mom_status == "OPEN":
            return True
        # legacy
        if (stock.get("status") or "").upper() == "MOM_OPEN":
            return True
        return False

    return False

async def _stop_kws():
    kws = RAM_STATE.get("kws")
    if not kws:
        return
    try:
        kws.close()
    except Exception:
        pass
    RAM_STATE["kws"] = None

async def _start_kws_if_possible():
    """
    Starts KiteTicker (threaded) if api_key + access_token present.
    Safe to call multiple times (restarts stream).
    """
    api_key = RAM_STATE.get("api_key") or ""
    access_token = RAM_STATE.get("access_token") or ""
    if not api_key or not access_token:
        logger.warning("⚠️ WS: Not started (missing api_key or access_token).")
        return

    await _stop_kws()

    try:
        kws = KiteTicker(str(api_key), str(access_token))
        kws.on_ticks = on_ticks
        kws.on_connect = on_connect
        kws.on_error = on_error
        kws.on_close = on_close
        RAM_STATE["kws"] = kws

        kws.connect(threaded=True)
        logger.info("🛰️ WS: KiteTicker started/restarted (threaded=True).")
    except Exception as e:
        logger.exception(f"❌ WS START FAILED: {e}")
        RAM_STATE["kws"] = None

# -----------------------------
# CENTRALIZED 1-MIN CANDLE (LOW-LATENCY)
# -----------------------------
def _update_1m_candle_and_maybe_close(stock: dict, ltp: float, cum_vol: int) -> Optional[dict]:
    """
    Central candle aggregation to avoid engine freezes.
    Returns a CLOSED candle dict when bucket changes; else None.
    Uses per-stock "candle_1m" and "candle_last_cum_vol".
    """
    now = _now_ist()
    bucket = now.replace(second=0, microsecond=0)

    c = stock.get("candle_1m")
    last_cum = int(stock.get("candle_last_cum_vol", 0) or 0)

    # init
    if not c:
        stock["candle_1m"] = {
            "bucket": bucket,
            "open": float(ltp),
            "high": float(ltp),
            "low": float(ltp),
            "close": float(ltp),
            "volume": 0,
        }
        stock["candle_last_cum_vol"] = int(cum_vol)
        return None

    # bucket rollover -> close
    if c.get("bucket") != bucket:
        closed = dict(c)

        # start new candle
        stock["candle_1m"] = {
            "bucket": bucket,
            "open": float(ltp),
            "high": float(ltp),
            "low": float(ltp),
            "close": float(ltp),
            "volume": 0,
        }
        stock["candle_last_cum_vol"] = int(cum_vol)
        return closed

    # update candle
    c["high"] = max(float(c["high"]), float(ltp))
    c["low"] = min(float(c["low"]), float(ltp))
    c["close"] = float(ltp)

    if last_cum > 0:
        c["volume"] += max(0, int(cum_vol) - last_cum)

    stock["candle_last_cum_vol"] = int(cum_vol)
    return None


# -----------------------------
# PARALLEL TICK PROCESSING
# -----------------------------
async def _process_tick(token: int, ltp: float, cum_vol: int):
    """
    Per-token serialized (lock), but overall parallel across tokens.
    BreakoutEngine + MomentumEngine run concurrently (non-blocking).
    Candle-close events are queued to candle_close_queue.
    """
    sem: asyncio.Semaphore = RAM_STATE["engine_sem"]
    locks = RAM_STATE["token_locks"]

    async with sem:
        lock: asyncio.Lock = locks[token]
        async with lock:
            stock = RAM_STATE["stocks"].get(token)
            if not stock:
                return

            stock["ltp"] = float(ltp)

            # Central candle handling
            closed = _update_1m_candle_and_maybe_close(stock, ltp, cum_vol)
            if closed:
                try:
                    RAM_STATE["candle_close_queue"].put_nowait((token, closed))
                except asyncio.QueueFull:
                    # candle close queue overflow is bad; log it
                    logger.warning(f"⚠️ CandleCloseQ FULL: dropping candle close for {stock.get('symbol')}")

            # Run both engines concurrently for same tick
            # NOTE: Engines MUST use separate keys (brk_*/mom_*) to be safe under parallelism.
            results = await asyncio.gather(
                BreakoutEngine.run(token, ltp, cum_vol, RAM_STATE),
                MomentumEngine.run(token, ltp, cum_vol, RAM_STATE),
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, Exception):
                    logger.exception("❌ Engine tick handler crashed", exc_info=r)

async def tick_worker_parallel():
    """
    Reads tick batches from queue and schedules per-tick tasks in parallel
    with inflight backpressure + drop-oldest behavior at enqueue.
    """
    q: asyncio.Queue = RAM_STATE["tick_queue"]
    inflight: set = RAM_STATE["inflight"]
    max_inflight: int = RAM_STATE["max_inflight"]

    logger.info("🧵 tick_worker_parallel running")
    last_log = time.time()

    while True:
        ticks = await q.get()
        batch_ts = time.time()
        try:
            # backpressure: wait if too many inflight
            if len(inflight) >= max_inflight:
                done, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                inflight.difference_update(done)

            for tick in ticks:
                token = _safe_int(tick.get("instrument_token", 0), 0)
                if token <= 0:
                    continue

                stock = RAM_STATE["stocks"].get(token)
                if not stock:
                    continue

                ltp = _safe_float(tick.get("last_price", 0.0), 0.0)
                cum_vol = _safe_int(tick.get("volume_traded", 0), 0)
                if ltp <= 0:
                    continue

                t = asyncio.create_task(_process_tick(token, ltp, cum_vol))
                inflight.add(t)
                t.add_done_callback(lambda tt: RAM_STATE["inflight"].discard(tt))

            RAM_STATE["data_connected"]["breakout"] = True
            RAM_STATE["data_connected"]["momentum"] = True

            now = time.time()
            if now - last_log >= 2.0:
                last_log = now
                logger.info(
                    f"📊 Q={q.qsize()} inflight={len(inflight)} "
                    f"enq={RAM_STATE.get('tick_batches_enqueued',0)} "
                    f"drop={RAM_STATE.get('tick_batches_dropped',0)} "
                    f"batch_age_ms={(now - batch_ts)*1000:.0f}"
                )

        except Exception:
            logger.exception("❌ tick_worker_parallel crashed")
        finally:
            q.task_done()

async def candle_close_worker():
    """
    Processes closed candles (1m) and calls engine candle-close logic.
    This keeps candle qualification decoupled from tick hot-path.
    """
    cq: asyncio.Queue = RAM_STATE["candle_close_queue"]
    locks = RAM_STATE["token_locks"]
    sem: asyncio.Semaphore = RAM_STATE["engine_sem"]

    logger.info("🕯️ candle_close_worker running")
    while True:
        token, candle = await cq.get()
        try:
            async with sem:
                lock: asyncio.Lock = locks[token]
                async with lock:
                    stock = RAM_STATE["stocks"].get(token)
                    if not stock:
                        continue

                    # Call whichever candle-close API your engines implement.
                    # - Preferred: on_candle_close(token, candle, state)
                    # - Legacy: analyze_candle_logic(token, candle, state)
                    tasks = []

                    if hasattr(BreakoutEngine, "on_candle_close"):
                        tasks.append(BreakoutEngine.on_candle_close(token, candle, RAM_STATE))
                    elif hasattr(BreakoutEngine, "analyze_candle_logic"):
                        tasks.append(BreakoutEngine.analyze_candle_logic(token, candle, RAM_STATE))

                    if hasattr(MomentumEngine, "on_candle_close"):
                        tasks.append(MomentumEngine.on_candle_close(token, candle, RAM_STATE))
                    elif hasattr(MomentumEngine, "analyze_candle_logic"):
                        tasks.append(MomentumEngine.analyze_candle_logic(token, candle, RAM_STATE))

                    if tasks:
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        for r in results:
                            if isinstance(r, Exception):
                                logger.exception("❌ Engine candle handler crashed", exc_info=r)

        except Exception:
            logger.exception("❌ candle_close_worker crashed")
        finally:
            cq.task_done()

# -----------------------------
# KITE WS CALLBACKS
# -----------------------------
def on_ticks(ws, ticks):
    """
    KiteTicker callback (runs in background thread).
    Enqueue to asyncio queue (drop oldest if full).
    """
    loop = RAM_STATE.get("main_loop")
    q = RAM_STATE.get("tick_queue")
    if not loop or not q:
        return

    def _put():
        try:
            q.put_nowait(ticks)
            RAM_STATE["tick_batches_enqueued"] = RAM_STATE.get("tick_batches_enqueued", 0) + 1
        except asyncio.QueueFull:
            # Drop oldest batch, keep latest
            try:
                _ = q.get_nowait()
                q.task_done()
            except Exception:
                pass
            try:
                q.put_nowait(ticks)
                RAM_STATE["tick_batches_enqueued"] = RAM_STATE.get("tick_batches_enqueued", 0) + 1
                RAM_STATE["tick_batches_dropped"] = RAM_STATE.get("tick_batches_dropped", 0) + 1
            except Exception:
                RAM_STATE["tick_batches_dropped"] = RAM_STATE.get("tick_batches_dropped", 0) + 1

    loop.call_soon_threadsafe(_put)

def on_connect(ws, response):
    logger.info("✅ TICKER: Connected. Subscribing to universe tokens...")

    async def _sub():
        tokens = await TradeControl.get_subscribe_universe_tokens()
        tokens = [int(x) for x in tokens if int(x) > 0]

        if not tokens:
            tokens = list(RAM_STATE["stocks"].keys())
            logger.warning(f"⚠️ WS: universe empty; fallback subscribe RAM stocks = {len(tokens)}")

        tokens = [t for t in tokens if t in RAM_STATE["stocks"]]
        if not tokens:
            logger.error("❌ WS: No valid tokens to subscribe (RAM_STATE empty).")
            return

        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
        logger.info(f"📡 WS: Subscribed {len(tokens)} tokens (MODE_FULL).")

        RAM_STATE["data_connected"]["breakout"] = True
        RAM_STATE["data_connected"]["momentum"] = True

    loop = RAM_STATE.get("main_loop")
    if loop:
        asyncio.run_coroutine_threadsafe(_sub(), loop)

def on_error(ws, code, reason):
    logger.error(f"❌ TICKER ERROR: {code} - {reason}")
    RAM_STATE["data_connected"]["breakout"] = False
    RAM_STATE["data_connected"]["momentum"] = False

def on_close(ws, code, reason):
    logger.warning(f"⚠️ TICKER CLOSED: ({code}) {reason}")
    RAM_STATE["data_connected"]["breakout"] = False
    RAM_STATE["data_connected"]["momentum"] = False

# -----------------------------
# STARTUP / SHUTDOWN
# -----------------------------
@app.on_event("startup")
async def startup_event():
    logger.info("--- 🚀 NEXUS BOOT ---")

    RAM_STATE["main_loop"] = asyncio.get_running_loop()

    # parallel infra
    RAM_STATE["tick_queue"] = asyncio.Queue(maxsize=2500)
    RAM_STATE["candle_close_queue"] = asyncio.Queue(maxsize=4000)

    RAM_STATE["token_locks"] = defaultdict(asyncio.Lock)
    # Tune semaphore for your dyno capacity.
    # If your universe is small, 200-400 is fine. If larger, 400-900.
    RAM_STATE["engine_sem"] = asyncio.Semaphore(int(os.getenv("ENGINE_CONCURRENCY", "500")))
    RAM_STATE["max_inflight"] = int(os.getenv("MAX_INFLIGHT", "5000"))

    RAM_STATE["tick_worker_task"] = asyncio.create_task(tick_worker_parallel())
    RAM_STATE["candle_worker_task"] = asyncio.create_task(candle_close_worker())

    # 1) Restore API creds from Redis or Env
    key_redis, sec_redis = await TradeControl.get_config()
    RAM_STATE["api_key"] = str(key_redis or os.getenv("KITE_API_KEY", "") or "")
    RAM_STATE["api_secret"] = str(sec_redis or os.getenv("KITE_API_SECRET", "") or "")
    if RAM_STATE["api_key"]:
        logger.info(f"🔑 API KEY loaded ({RAM_STATE['api_key'][:4]}***)")

    # 2) Restore access token
    token = await TradeControl.get_access_token()
    RAM_STATE["access_token"] = str(token or "")

    # 3) Load market cache into RAM_STATE["stocks"]
    cached = await TradeControl.get_all_market_data()
    stocks: Dict[int, Dict[str, Any]] = {}

    for token_str, md in (cached or {}).items():
        try:
            t_id = int(token_str)
        except Exception:
            continue

        stocks[t_id] = {
            "token": t_id,
            "symbol": md.get("symbol"),
            "sma": _safe_float(md.get("sma", 0), 0.0),
            "pdh": _safe_float(md.get("pdh", 0), 0.0),
            "pdl": _safe_float(md.get("pdl", 0), 0.0),
            "prev_close": _safe_float(md.get("prev_close", 0), 0.0),
            "sync_time": md.get("sync_time", ""),

            # runtime
            "ltp": 0.0,

            # centralized candle
            "candle_1m": None,
            "candle_last_cum_vol": 0,

            # scanner timing (generic)
            "scan_seen_ts": None,
            "scan_seen_time": None,

            # NEW per-engine state placeholders (safe defaults)
            "brk_status": "WAITING",
            "mom_status": "WAITING",
        }

    RAM_STATE["stocks"] = stocks
    logger.info(f"✅ STARTUP: Loaded {len(stocks)} stocks from Redis market_cache.")

    # 4) Restore persisted strategy settings
    for side in ["bull", "bear", "mom_bull", "mom_bear"]:
        saved = await TradeControl.get_strategy_settings(side)
        if saved:
            # ensure correct keys (trade_start/trade_end)
            if "trade_start_time" in saved and "trade_start" not in saved:
                saved["trade_start"] = saved.pop("trade_start_time")
            if "trade_end_time" in saved and "trade_end" not in saved:
                saved["trade_end"] = saved.pop("trade_end_time")

            RAM_STATE["config"][side].update(saved)
            logger.info(f"🔁 SETTINGS: Restored {side} settings from Redis.")

    # 5) Build KiteConnect session
    if RAM_STATE["api_key"] and RAM_STATE["access_token"]:
        try:
            RAM_STATE["kite"] = KiteConnect(api_key=RAM_STATE["api_key"])
            RAM_STATE["kite"].set_access_token(RAM_STATE["access_token"])
            logger.info("✅ KITE: Session ready.")
        except Exception as e:
            logger.exception(f"❌ KITE: Session setup failed: {e}")
            RAM_STATE["kite"] = None

    # 6) Start WebSocket
    await _start_kws_if_possible()

@app.on_event("shutdown")
async def shutdown_event():
    try:
        await _stop_kws()
    except Exception:
        pass


# -----------------------------
# UI ROUTES
# -----------------------------
@app.get("/", response_class=HTMLResponse)
async def home():
    try:
        return FileResponse("index.html")
    except Exception:
        return HTMLResponse("<h3>Nexus backend is running. Place index.html next to main.py</h3>")


# -----------------------------
# AUTH (Support both /login and /login/)
# Zerodha redirect often hits /login?request_token=...
# -----------------------------
@app.get("/login")
@app.get("/login/")
async def login(
    request_token: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    action: Optional[str] = Query(default=None),
    type: Optional[str] = Query(default=None),
):
    """
    - If request_token missing => redirect to Kite login URL
    - If present => generate session, store access token, start WS immediately
    """
    api_key = RAM_STATE["api_key"] or os.getenv("KITE_API_KEY", "")
    api_secret = RAM_STATE["api_secret"] or os.getenv("KITE_API_SECRET", "")

    if not api_key:
        k, s = await TradeControl.get_config()
        api_key = api_key or (k or "")
        api_secret = api_secret or (s or "")

    if not api_key:
        return HTMLResponse("<h3>Save API KEY first (API Gateway button)</h3>")

    if not request_token:
        try:
            url = KiteConnect(api_key=str(api_key)).login_url()
            return RedirectResponse(url=url)
        except Exception as e:
            return HTMLResponse(f"<h3>Login URL error: {e}</h3>")

    # callback: exchange request_token -> access_token
    try:
        kite = KiteConnect(api_key=str(api_key))
        data = await asyncio.to_thread(kite.generate_session, request_token, api_secret=str(api_secret))
        access_token = data["access_token"]

        await TradeControl.save_access_token(access_token)

        RAM_STATE["access_token"] = str(access_token)
        RAM_STATE["api_key"] = str(api_key)
        RAM_STATE["api_secret"] = str(api_secret)

        RAM_STATE["kite"] = kite
        RAM_STATE["kite"].set_access_token(access_token)

        logger.info("✅ AUTH: Session established & saved to Redis. Restarting WS...")
        await _start_kws_if_possible()

        return RedirectResponse(url="/")
    except Exception as e:
        logger.exception(f"❌ AUTH ERROR: {e}")
        return HTMLResponse(f"<h3>Auth failed: {e}</h3>")


# -----------------------------
# CORE API
# -----------------------------
@app.get("/api/stats")
async def get_stats():
    pnl = _compute_pnl()
    return {
        "pnl": pnl,
        "engine_status": {k: "1" if bool(v) else "0" for k, v in RAM_STATE["engine_live"].items()},
        "data_connected": RAM_STATE.get("data_connected", {"breakout": False, "momentum": False}),
        "stock_count": len(RAM_STATE["stocks"]),
        "server_time": _now_ist().strftime("%H:%M:%S"),
        "queue": {
            "tick_q": RAM_STATE["tick_queue"].qsize() if RAM_STATE.get("tick_queue") else 0,
            "candle_q": RAM_STATE["candle_close_queue"].qsize() if RAM_STATE.get("candle_close_queue") else 0,
            "inflight": len(RAM_STATE.get("inflight", set())),
            "dropped_batches": RAM_STATE.get("tick_batches_dropped", 0),
        },
    }

@app.get("/api/orders")
async def get_orders(open_only: int = 0):
    """
    open_only=1 => return only OPEN trades (recommended for UI tables).
    default => return all trades (open + closed) so total PnL can be audited.
    """
    if not open_only:
        return RAM_STATE["trades"]

    out = {}
    for side, arr in RAM_STATE["trades"].items():
        out[side] = [t for t in arr if _trade_is_open(t)]
    return out

@app.get("/api/settings/engine/{side}")
async def get_engine_settings(side: str):
    if side not in RAM_STATE["config"]:
        return {}
    return RAM_STATE["config"][side]

@app.post("/api/settings/engine/{side}")
async def save_engine_settings(side: str, data: Dict[str, Any]):
    if side in RAM_STATE["config"]:
        # Normalize UI legacy keys -> backend keys
        if "trade_start_time" in data and "trade_start" not in data:
            data["trade_start"] = data.pop("trade_start_time")
        if "trade_end_time" in data and "trade_end" not in data:
            data["trade_end"] = data.pop("trade_end_time")

        RAM_STATE["config"][side].update(data)
        await TradeControl.save_strategy_settings(side, RAM_STATE["config"][side])
        logger.info(f"📝 SETTINGS: Updated & persisted for {side}.")
    return {"status": "success"}

# -----------------------------
# SCANNER (Unified for NEW + LEGACY)
# -----------------------------
@app.get("/api/scanner")
async def get_scanner():
    signals = {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]}
    now = _now_ist()

    for _, s in RAM_STATE["stocks"].items():
        sym = s.get("symbol") or ""
        if not sym:
            continue

        # --- BREAKOUT signal ---
        brk_status = (s.get("brk_status") or "").upper()
        if brk_status == "TRIGGER_WATCH":
            side = (s.get("brk_side_latch") or "").lower()
            if side in ("bull", "bear"):
                if not s.get("brk_scan_seen_ts"):
                    s["brk_scan_seen_ts"] = int(now.timestamp())
                    s["brk_scan_seen_time"] = now.strftime("%H:%M:%S")

                vol = _safe_int(s.get("brk_scan_vol", 0), 0)
                sma = _safe_float(s.get("sma", 0), 0.0)
                rel = (float(vol) / sma) if sma > 0 else None

                prev_close = _safe_float(s.get("prev_close", 0), 0.0)
                ltp = _safe_float(s.get("ltp", 0), 0.0)
                pct = ((ltp - prev_close) / prev_close * 100.0) if prev_close > 0 and ltp > 0 else None

                signals[side].append(
                    {
                        "symbol": sym,
                        "trigger_px": s.get("brk_trigger_px", 0),
                        "seen_time": s.get("brk_scan_seen_time"),
                        "seen_ts": s.get("brk_scan_seen_ts"),
                        "volume": vol,
                        "rel_vol": rel,
                        "pct_change": pct,
                        "chart_url": f"https://www.tradingview.com/chart/?symbol=NSE:{sym}",
                        "reason": s.get("brk_scan_reason", "Candle Confirmed"),
                    }
                )

        # --- MOMENTUM signal ---
        mom_status = (s.get("mom_status") or "").upper()
        if mom_status == "TRIGGER_WATCH":
            side = (s.get("mom_side_latch") or "").lower()
            if side in ("mom_bull", "mom_bear"):
                if not s.get("mom_scan_seen_ts"):
                    s["mom_scan_seen_ts"] = int(now.timestamp())
                    s["mom_scan_seen_time"] = now.strftime("%H:%M:%S")

                vol = _safe_int(s.get("mom_scan_vol", 0), 0)
                sma = _safe_float(s.get("sma", 0), 0.0)
                rel = (float(vol) / sma) if sma > 0 else None

                prev_close = _safe_float(s.get("prev_close", 0), 0.0)
                ltp = _safe_float(s.get("ltp", 0), 0.0)
                pct = ((ltp - prev_close) / prev_close * 100.0) if prev_close > 0 and ltp > 0 else None

                signals[side].append(
                    {
                        "symbol": sym,
                        "trigger_px": s.get("mom_trigger_px", 0),
                        "seen_time": s.get("mom_scan_seen_time"),
                        "seen_ts": s.get("mom_scan_seen_ts"),
                        "volume": vol,
                        "rel_vol": rel,
                        "pct_change": pct,
                        "chart_url": f"https://www.tradingview.com/chart/?symbol=NSE:{sym}",
                        "reason": s.get("mom_scan_reason", "Momentum candle"),
                    }
                )

        # --- LEGACY fallback (if you still use old engines) ---
        legacy_status = (s.get("status") or "").upper()
        if legacy_status in ("TRIGGER_WATCH", "MOM_TRIGGER_WATCH"):
            side = (s.get("side_latch") or "").lower()
            if side in signals:
                if not s.get("scan_seen_ts"):
                    s["scan_seen_ts"] = int(now.timestamp())
                    s["scan_seen_time"] = now.strftime("%H:%M:%S")

                vol = _safe_int(s.get("scan_vol", 0), 0)
                sma = _safe_float(s.get("sma", 0), 0.0)
                rel = (float(vol) / sma) if sma > 0 else None

                prev_close = _safe_float(s.get("prev_close", 0), 0.0)
                ltp = _safe_float(s.get("ltp", 0), 0.0)
                pct = ((ltp - prev_close) / prev_close * 100.0) if prev_close > 0 and ltp > 0 else None

                signals[side].append(
                    {
                        "symbol": sym,
                        "trigger_px": s.get("trigger_px", s.get("trigger_price", 0)),
                        "seen_time": s.get("scan_seen_time"),
                        "seen_ts": s.get("scan_seen_ts"),
                        "volume": vol,
                        "rel_vol": rel,
                        "pct_change": pct,
                        "chart_url": f"https://www.tradingview.com/chart/?symbol=NSE:{sym}",
                        "reason": s.get("scan_reason", "Candle Confirmed"),
                    }
                )

    for side in signals:
        signals[side].sort(key=lambda x: x.get("seen_ts", 0), reverse=True)
    return signals

# -----------------------------
# CONTROL ENDPOINT
# -----------------------------
@app.post("/api/control")
async def control(request: Request):
    data = await request.json()
    action = data.get("action")

    if action == "toggle_engine":
        side = data.get("side")
        enabled = bool(data.get("enabled"))
        if side in RAM_STATE["engine_live"]:
            RAM_STATE["engine_live"][side] = enabled
            logger.info(f"ENGINE TOGGLE: {side} -> {enabled}")
        return {"status": "ok"}

    if action == "save_api":
        api_key = data.get("api_key", "") or ""
        api_secret = data.get("api_secret", "") or ""
        RAM_STATE["api_key"] = str(api_key)
        RAM_STATE["api_secret"] = str(api_secret)
        await TradeControl.save_config(api_key, api_secret)
        logger.info("🔐 API credentials saved to Redis.")
        return {"status": "ok"}

    # legacy manual exit
    if action == "manual_exit":
        symbol = data.get("symbol")
        if symbol:
            RAM_STATE["manual_exits"].add(symbol)
        return {"status": "ok"}

    # square off one
    if action == "square_off_one":
        side = data.get("side")
        symbol = data.get("symbol")
        if not side or not symbol:
            return {"status": "error", "message": "side/symbol missing"}

        stock = next((s for s in RAM_STATE["stocks"].values() if s.get("symbol") == symbol), None)
        if not stock:
            return {"status": "error", "message": "symbol not found"}

        try:
            if side in ("bull", "bear"):
                # allow close if either new or legacy open
                if _stock_has_open_for_side(stock, side):
                    await BreakoutEngine.close_position(stock, RAM_STATE, "UI_EXIT_ONE")
                else:
                    return {"status": "ok", "message": "no open position for that side"}
            elif side in ("mom_bull", "mom_bear"):
                if _stock_has_open_for_side(stock, side):
                    await MomentumEngine.close_position(stock, RAM_STATE, "UI_EXIT_ONE")
                else:
                    return {"status": "ok", "message": "no open position for that side"}
            else:
                return {"status": "error", "message": "invalid side"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

        return {"status": "ok"}

    # square off all for one side
    if action == "square_off_all":
        side = data.get("side")
        if side not in ("bull", "bear", "mom_bull", "mom_bear"):
            return {"status": "error", "message": "invalid side"}

        try:
            for s in list(RAM_STATE["stocks"].values()):
                if (s.get("symbol") or "") == "":
                    continue
                if _stock_has_open_for_side(s, side):
                    if side in ("bull", "bear"):
                        await BreakoutEngine.close_position(s, RAM_STATE, "UI_EXIT_ALL")
                    else:
                        await MomentumEngine.close_position(s, RAM_STATE, "UI_EXIT_ALL")
        except Exception as e:
            return {"status": "error", "message": str(e)}

        return {"status": "ok"}

    # optional: restart websocket manually
    if action == "restart_ws":
        await _start_kws_if_possible()
        return {"status": "ok"}

    return {"status": "error", "message": "unknown action"}

# -----------------------------
# OPTIONAL: TICK INGEST (TESTING)
# -----------------------------
@app.post("/api/tick")
async def ingest_tick(data: Dict[str, Any]):
    token = int(data.get("token", 0) or 0)
    ltp = float(data.get("ltp", 0) or 0)
    cum_vol = int(data.get("vol", 0) or 0)

    stock = RAM_STATE["stocks"].get(token)
    if not stock:
        return {"status": "error", "message": "token not found"}

    # feed through the same processing pipeline (parallel-safe)
    t = asyncio.create_task(_process_tick(token, ltp, cum_vol))
    RAM_STATE["inflight"].add(t)
    t.add_done_callback(lambda tt: RAM_STATE["inflight"].discard(tt))

    RAM_STATE["data_connected"]["breakout"] = True
    RAM_STATE["data_connected"]["momentum"] = True
    return {"status": "ok"}

