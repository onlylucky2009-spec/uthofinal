# # import logging
# # import asyncio
# # from datetime import datetime
# # from math import floor
# # import pytz
# # from redis_manager import TradeControl

# # # --- LOGGING SETUP ---
# # logger = logging.getLogger("Nexus_Momentum")
# # IST = pytz.timezone("Asia/Kolkata")

# # class MomentumEngine:
    
# #     @staticmethod
# #     async def run(token: int, ltp: float, vol: int, state: dict):
# #         """
# #         Main entry point for Momentum processing.
# #         Aggregates ticks into 1-minute candles and monitors active momentum trades.
# #         """
# #         stock = state["stocks"].get(token)
# #         if not stock: return

# #         # 1. MONITOR ACTIVE MOMENTUM TRADES (PnL & Exits)
# #         if stock['status'] == 'MOM_OPEN':
# #             await MomentumEngine.monitor_active_trade(stock, ltp, state)
# #             return

# #         # 2. TRIGGER WATCH (Wait for price to breach the momentum candle trigger)
# #         if stock['status'] == 'MOM_TRIGGER_WATCH':
# #             # Bullish Momentum Trigger (Breach High)
# #             if stock['side_latch'] == 'MOM_BULL' and ltp >= stock['trigger_px']:
# #                 logger.info(f"⚡ [MOM-TRIGGER] {stock['symbol']} Bullish trigger hit @ {ltp}")
# #                 await MomentumEngine.open_trade(token, stock, ltp, state, 'mom_bull')
            
# #             # Bearish Momentum Trigger (Breach Low)
# #             elif stock['side_latch'] == 'MOM_BEAR' and ltp <= stock['trigger_px']:
# #                 logger.info(f"⚡ [MOM-TRIGGER] {stock['symbol']} Bearish trigger hit @ {ltp}")
# #                 await MomentumEngine.open_trade(token, stock, ltp, state, 'mom_bear')
# #             return

# #         # 3. 1-MINUTE ASYNC CANDLE FORMATION
# #         now = datetime.now(IST)
# #         bucket = now.replace(second=0, microsecond=0)

# #         if stock['candle'] and stock['candle']['bucket'] != bucket:
# #             # Candle closed: Process logic in background
# #             asyncio.create_task(MomentumEngine.analyze_momentum_logic(token, stock['candle'], state))
# #             # Reset for new minute
# #             stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
# #         elif not stock['candle']:
# #             stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
# #         else:
# #             c = stock['candle']
# #             c['high'] = max(c['high'], ltp)
# #             c['low'] = min(c['low'], ltp)
# #             c['close'] = ltp
# #             # Cumulative Volume Delta
# #             if stock['last_vol'] > 0:
# #                 c['volume'] += max(0, vol - stock['last_vol'])
        
# #         stock['last_vol'] = vol

# #     @staticmethod
# #     async def analyze_momentum_logic(token: int, candle: dict, state: dict):
# #         """Checks for price velocity and volume surges against Dashboard settings."""
# #         stock = state["stocks"][token]
# #         symbol = stock['symbol']
# #         body_size = abs(candle['close'] - candle['open'])
# #         body_pct = (body_size / candle['open']) * 100 if candle['open'] > 0 else 0

# #         # --- BULLISH MOMENTUM ---
# #         if state["engine_live"].get("mom_bull") and candle['close'] > candle['open']:
# #             if body_pct > 0.25: # Requirement: Body > 0.25%
# #                 is_qualified, detail = await MomentumEngine.check_vol_matrix(stock, candle, 'mom_bull', state)
# #                 if is_qualified:
# #                     logger.info(f"✅ [MOM-QUALIFIED] {symbol} BULL Surge | Body: {body_pct:.2f}% | {detail}")
# #                     stock['status'] = 'MOM_TRIGGER_WATCH'
# #                     stock['side_latch'] = 'MOM_BULL'
# #                     stock['trigger_px'] = round(candle['high'] + (body_size * 0.1), 2)
# #                 else:
# #                     logger.info(f"❌ [MOM-REJECT] {symbol} BULL | {detail}")

# #         # --- BEARISH MOMENTUM ---
# #         elif state["engine_live"].get("mom_bear") and candle['close'] < candle['open']:
# #             if body_pct > 0.25:
# #                 is_qualified, detail = await MomentumEngine.check_vol_matrix(stock, candle, 'mom_bear', state)
# #                 if is_qualified:
# #                     logger.info(f"✅ [MOM-QUALIFIED] {symbol} BEAR Crash | Body: {body_pct:.2f}% | {detail}")
# #                     stock['status'] = 'MOM_TRIGGER_WATCH'
# #                     stock['side_latch'] = 'MOM_BEAR'
# #                     stock['trigger_px'] = round(candle['low'] - (body_size * 0.1), 2)
# #                 else:
# #                     logger.info(f"❌ [MOM-REJECT] {symbol} BEAR | {detail}")

# #     @staticmethod
# #     async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
# #         """Asynchronous Volume tier check."""
# #         matrix = state["config"][side].get('volume_criteria', [])
# #         c_vol = candle['volume']
# #         s_sma = stock.get('sma', 0)
# #         c_val_cr = (c_vol * candle['close']) / 10000000.0

# #         if not matrix: return True, "No Matrix"

# #         tier_found = None
# #         for i, level in enumerate(matrix):
# #             if s_sma >= float(level.get('min_sma_avg', 0)): tier_found = (i, level)
# #             else: break

# #         if tier_found:
# #             idx, level = tier_found
# #             required_vol = s_sma * float(level.get('sma_multiplier', 1.0))
# #             min_cr = float(level.get('min_vol_price_cr', 0))
# #             if c_vol >= required_vol and c_val_cr >= min_cr:
# #                 return True, f"Tier {idx+1} Pass"
# #             return False, f"Tier {idx+1} Fail (Vol/Value)"
        
# #         return False, f"SMA {s_sma:,.0f} too low"

# #     @staticmethod
# #     async def open_trade(token: int, stock: dict, ltp: float, state: dict, side_key: str):
# #         """Places real BUY or SELL market orders in Zerodha."""
# #         cfg = state["config"][side_key]
# #         kite = state.get("kite")
        
# #         if not kite:
# #             logger.error(f"❌ [KITE ERROR] Session missing for {stock['symbol']}")
# #             return

# #         # 1. Trade Limit Check
# #         if not await TradeControl.can_trade(side_key, int(cfg.get('total_trades', 5))):
# #             logger.warning(f"🚫 [LIMIT] {stock['symbol']} limit reached for {side_key}")
# #             stock['status'] = 'WAITING'
# #             return

# #         # 2. Risk & Position Sizing
# #         is_bull = 'bull' in side_key
# #         sl_px = stock['candle']['low'] if is_bull else stock['candle']['high']
# #         risk_per_share = max(abs(ltp - sl_px), ltp * 0.005) # Min 0.5% risk floor
        
# #         risk_amount = float(cfg.get('risk_trade_1', 2000))
# #         qty = floor(risk_amount / risk_per_share)
        
# #         if qty <= 0:
# #             stock['status'] = 'WAITING'
# #             return

# #         try:
# #             # 3. EXECUTE REAL ORDER
# #             order_id = await asyncio.to_thread(
# #                 kite.place_order,
# #                 variety=kite.VARIETY_REGULAR,
# #                 exchange=kite.EXCHANGE_NSE,
# #                 tradingsymbol=stock['symbol'],
# #                 transaction_type=kite.TRANSACTION_TYPE_BUY if is_bull else kite.TRANSACTION_TYPE_SELL,
# #                 quantity=qty,
# #                 product=kite.PRODUCT_MIS,
# #                 order_type=kite.ORDER_TYPE_MARKET
# #             )

# #             # 4. Save to Live State
# #             rr_val = float(cfg.get('risk_reward', "1:2").split(':')[-1])
# #             trade = {
# #                 "symbol": stock['symbol'],
# #                 "qty": qty,
# #                 "entry_price": ltp,
# #                 "sl_price": sl_px,
# #                 "target_price": round(ltp + (risk_per_share * rr_val) if is_bull else ltp - (risk_per_share * rr_val), 2),
# #                 "order_id": order_id,
# #                 "pnl": 0.0,
# #                 "entry_time": datetime.now(IST).strftime("%H:%M:%S")
# #             }
            
# #             state["trades"][side_key].append(trade)
# #             stock['status'] = 'MOM_OPEN'
# #             stock['active_trade'] = trade
# #             logger.info(f"🚀 [MOM REAL ENTRY] {stock['symbol']} | Qty: {qty} | OrderID: {order_id}")

# #         except Exception as e:
# #             logger.error(f"❌ [KITE ORDER ERROR] {stock['symbol']}: {e}")
# #             stock['status'] = 'WAITING'

# #     @staticmethod
# #     async def monitor_active_trade(stock: dict, ltp: float, state: dict):
# #         """Real-time monitoring of open momentum positions."""
# #         trade = stock.get('active_trade')
# #         if not trade: return
        
# #         side_key = stock['side_latch'].lower()
# #         cfg = state["config"][side_key]
# #         is_bull = 'bull' in side_key

# #         # Live PnL Update
# #         if is_bull:
# #             trade['pnl'] = round((ltp - trade['entry_price']) * trade['qty'], 2)
# #             target_hit = ltp >= trade['target_price']
# #             sl_hit = ltp <= trade['sl_price']
# #         else:
# #             trade['pnl'] = round((trade['entry_price'] - ltp) * trade['qty'], 2)
# #             target_hit = ltp <= trade['target_price']
# #             sl_hit = ltp >= trade['sl_price']

# #         # EXIT SIGNALS
# #         if target_hit:
# #             logger.info(f"🎯 [MOM-TARGET] {stock['symbol']} hit target {trade['target_price']}")
# #             await MomentumEngine.close_position(stock, state, "TARGET")
        
# #         elif sl_hit:
# #             logger.info(f"🛑 [MOM-STOPLOSS] {stock['symbol']} hit SL {trade['sl_price']}")
# #             await MomentumEngine.close_position(stock, state, "SL")
        
# #         # Trailing SL
# #         else:
# #             tsl_ratio = float(cfg.get('trailing_sl', "1:1.5").split(':')[-1])
# #             new_sl = await MomentumEngine.calculate_tsl(trade, ltp, tsl_ratio, is_bull)
# #             if is_bull and new_sl > trade['sl_price']: trade['sl_price'] = new_sl
# #             elif not is_bull and new_sl < trade['sl_price']: trade['sl_price'] = new_sl

# #         # Manual Dashboard Exit
# #         if stock['symbol'] in state['manual_exits']:
# #             logger.info(f"🖱️ [MOM-MANUAL EXIT] {stock['symbol']}")
# #             await MomentumEngine.close_position(stock, state, "MANUAL")
# #             state['manual_exits'].remove(stock['symbol'])

# #     @staticmethod
# #     async def calculate_tsl(trade: dict, ltp: float, ratio: float, is_bull: bool):
# #         entry, sl = trade['entry_price'], trade['sl_price']
# #         risk = abs(entry - sl)
# #         profit = (ltp - entry) if is_bull else (entry - ltp)
# #         if profit > (risk * ratio):
# #             return round(ltp - (risk * 0.9), 2) if is_bull else round(ltp + (risk * 0.9), 2)
# #         return sl

# #     @staticmethod
# #     async def close_position(stock: dict, state: dict, reason: str):
# #         """Places real market order to exit the momentum position."""
# #         trade = stock.get('active_trade')
# #         kite = state.get("kite")
# #         is_bull = 'bull' in stock['side_latch'].lower()
        
# #         if trade and kite:
# #             try:
# #                 # Place Exit Order (Opposite of entry)
# #                 exit_id = await asyncio.to_thread(
# #                     kite.place_order,
# #                     variety=kite.VARIETY_REGULAR,
# #                     exchange=kite.EXCHANGE_NSE,
# #                     tradingsymbol=stock['symbol'],
# #                     transaction_type=kite.TRANSACTION_TYPE_SELL if is_bull else kite.TRANSACTION_TYPE_BUY,
# #                     quantity=trade['qty'],
# #                     product=kite.PRODUCT_MIS,
# #                     order_type=kite.ORDER_TYPE_MARKET
# #                 )
# #                 logger.info(f"🏁 [MOM REAL EXIT] {stock['symbol']} Reason: {reason} | OrderID: {exit_id}")
# #             except Exception as e:
# #                 logger.error(f"❌ [KITE MOM EXIT ERROR] {stock['symbol']}: {e}")

# #         # Finalize State
# #         stock['status'] = 'WAITING'
# #         stock['active_trade'] = None

# import logging
# import asyncio
# from datetime import datetime, timedelta
# from math import floor
# import pytz
# from redis_manager import TradeControl

# # --- LOGGING SETUP ---
# logger = logging.getLogger("Nexus_Momentum")
# IST = pytz.timezone("Asia/Kolkata")


# def _safe_create_task(coro, name: str = "task"):
#     """
#     Create a background task and log exceptions (so they don't fail silently).
#     """
#     task = asyncio.create_task(coro)

#     def _done(t: asyncio.Task):
#         try:
#             _ = t.result()
#         except asyncio.CancelledError:
#             return
#         except Exception:
#             logger.exception(f"❌ Background task failed: {name}")

#     task.add_done_callback(_done)
#     return task


# class MomentumEngine:
#     # Status constants (momentum engine only)
#     ST_WAITING = "WAITING"
#     ST_TRIGGER = "MOM_TRIGGER_WATCH"
#     ST_PENDING = "MOM_PENDING_ENTRY"
#     ST_OPEN = "MOM_OPEN"
#     ST_EXITING = "MOM_EXITING"

#     OWNER = "momentum"

#     @staticmethod
#     async def run(token: int, ltp: float, vol: int, state: dict):
#         """
#         Main entry point for Momentum processing.
#         Aggregates ticks into 1-minute momentum candles and monitors active momentum trades.
#         """
#         stock = state["stocks"].get(token)
#         if not stock:
#             return

#         # If another engine owns this symbol, momentum should not interfere
#         owner = stock.get("owner")
#         if owner and owner != MomentumEngine.OWNER:
#             if stock.get("status") not in (MomentumEngine.ST_OPEN, MomentumEngine.ST_PENDING, MomentumEngine.ST_EXITING):
#                 return

#         # If breakout engine is active on this stock, don't overwrite breakout states
#         if stock.get("status") in ("OPEN", "PENDING_ENTRY", "EXITING", "TRIGGER_WATCH"):
#             return

#         # 1) Monitor active momentum trades
#         if stock.get("status") == MomentumEngine.ST_OPEN:
#             await MomentumEngine.monitor_active_trade(stock, ltp, state)
#             return

#         # 2) Ignore while placing entry or exiting (prevents duplicates)
#         if stock.get("status") in (MomentumEngine.ST_PENDING, MomentumEngine.ST_EXITING):
#             return

#         # 3) Trigger watch (breach trigger price) -> background entry
#         if stock.get("status") == MomentumEngine.ST_TRIGGER:
#             side_latch = stock.get("side_latch")

#             if side_latch == "MOM_BULL" and ltp >= float(stock.get("trigger_px", 0) or 0):
#                 logger.info(f"⚡ [MOM-TRIGGER] {stock['symbol']} Bullish trigger hit @ {ltp}")
#                 stock["status"] = MomentumEngine.ST_PENDING
#                 _safe_create_task(
#                     MomentumEngine.open_trade(token, stock, ltp, state, "mom_bull"),
#                     name=f"mom_open_trade:bull:{stock['symbol']}",
#                 )

#             elif side_latch == "MOM_BEAR" and ltp <= float(stock.get("trigger_px", 0) or 0):
#                 logger.info(f"⚡ [MOM-TRIGGER] {stock['symbol']} Bearish trigger hit @ {ltp}")
#                 stock["status"] = MomentumEngine.ST_PENDING
#                 _safe_create_task(
#                     MomentumEngine.open_trade(token, stock, ltp, state, "mom_bear"),
#                     name=f"mom_open_trade:bear:{stock['symbol']}",
#                 )

#             return

#         # 4) 1-minute candle formation (momentum-specific keys to avoid engine collision)
#         now = datetime.now(IST)
#         bucket = now.replace(second=0, microsecond=0)

#         ckey = "mom_candle"
#         vkey = "mom_last_vol"

#         if stock.get(ckey) and stock[ckey]["bucket"] != bucket:
#             prev_candle = stock[ckey]
#             _safe_create_task(
#                 MomentumEngine.analyze_momentum_logic(token, prev_candle, state),
#                 name=f"analyze_momentum:{stock['symbol']}",
#             )
#             stock[ckey] = {"bucket": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": 0}

#         elif not stock.get(ckey):
#             stock[ckey] = {"bucket": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": 0}

#         else:
#             c = stock[ckey]
#             c["high"] = max(c["high"], ltp)
#             c["low"] = min(c["low"], ltp)
#             c["close"] = ltp

#             last_vol = int(stock.get(vkey, 0) or 0)
#             if last_vol > 0:
#                 c["volume"] += max(0, int(vol) - last_vol)

#         stock[vkey] = int(vol)

#     @staticmethod
#     async def analyze_momentum_logic(token: int, candle: dict, state: dict):
#         """
#         Checks for price velocity and volume surges against Dashboard settings.
#         """
#         stock = state["stocks"].get(token)
#         if not stock:
#             return

#         # If owned by another engine, skip
#         owner = stock.get("owner")
#         if owner and owner != MomentumEngine.OWNER:
#             return

#         # Only scan when idle
#         if stock.get("status") not in (MomentumEngine.ST_WAITING, None, ""):
#             return

#         symbol = stock["symbol"]
#         o = float(candle.get("open", 0) or 0)
#         c = float(candle.get("close", 0) or 0)
#         h = float(candle.get("high", 0) or 0)
#         l = float(candle.get("low", 0) or 0)

#         if o <= 0:
#             return

#         body_size = abs(c - o)
#         body_pct = (body_size / o) * 100

#         # --- BULLISH MOMENTUM ---
#         if state["engine_live"].get("mom_bull") and c > o:
#             if body_pct > 0.25:
#                 is_qualified, detail = await MomentumEngine.check_vol_matrix(stock, candle, "mom_bull", state)
#                 if is_qualified:
#                     logger.info(f"✅ [MOM-QUALIFIED] {symbol} BULL | Body: {body_pct:.2f}% | {detail}")
#                     stock["status"] = MomentumEngine.ST_TRIGGER
#                     stock["side_latch"] = "MOM_BULL"
#                     stock["trigger_px"] = round(h + (body_size * 0.1), 2)

#                     # ✅ Store SL reference from qualifying candle (not future candle)
#                     stock["mom_trigger_sl"] = l
#                     stock["mom_trigger_bucket"] = candle.get("bucket")
#                 else:
#                     logger.info(f"❌ [MOM-REJECT] {symbol} BULL | {detail}")

#         # --- BEARISH MOMENTUM ---
#         elif state["engine_live"].get("mom_bear") and c < o:
#             if body_pct > 0.25:
#                 is_qualified, detail = await MomentumEngine.check_vol_matrix(stock, candle, "mom_bear", state)
#                 if is_qualified:
#                     logger.info(f"✅ [MOM-QUALIFIED] {symbol} BEAR | Body: {body_pct:.2f}% | {detail}")
#                     stock["status"] = MomentumEngine.ST_TRIGGER
#                     stock["side_latch"] = "MOM_BEAR"
#                     stock["trigger_px"] = round(l - (body_size * 0.1), 2)

#                     # ✅ Store SL reference from qualifying candle (not future candle)
#                     stock["mom_trigger_sl"] = h
#                     stock["mom_trigger_bucket"] = candle.get("bucket")
#                 else:
#                     logger.info(f"❌ [MOM-REJECT] {symbol} BEAR | {detail}")

#     @staticmethod
#     async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
#         """
#         Asynchronous Volume tier check.
#         """
#         matrix = state["config"][side].get("volume_criteria", [])
#         c_vol = float(candle.get("volume", 0) or 0)
#         s_sma = float(stock.get("sma", 0) or 0)
#         close_px = float(candle.get("close", 0) or 0)
#         c_val_cr = (c_vol * close_px) / 10000000.0 if close_px > 0 else 0

#         if not matrix:
#             return True, "No Matrix"

#         tier_found = None
#         for i, level in enumerate(matrix):
#             if s_sma >= float(level.get("min_sma_avg", 0) or 0):
#                 tier_found = (i, level)
#             else:
#                 break

#         if tier_found:
#             idx, level = tier_found
#             required_vol = s_sma * float(level.get("sma_multiplier", 1.0) or 1.0)
#             min_cr = float(level.get("min_vol_price_cr", 0) or 0)
#             if c_vol >= required_vol and c_val_cr >= min_cr:
#                 return True, f"Tier {idx+1} Pass"
#             return False, f"Tier {idx+1} Fail (Vol/Value)"

#         return False, f"SMA {s_sma:,.0f} too low"

#     @staticmethod
#     async def open_trade(token: int, stock: dict, ltp: float, state: dict, side_key: str):
#         """
#         Places real BUY or SELL market orders in Zerodha.
#         Runs in background task (non-blocking for tick pipeline).
#         """
#         try:
#             # Ownership lock
#             stock["owner"] = MomentumEngine.OWNER

#             cfg = state["config"].get(side_key, {})
#             kite = state.get("kite")

#             if not kite:
#                 logger.error(f"❌ [KITE ERROR] Session missing for {stock.get('symbol')}")
#                 stock["status"] = MomentumEngine.ST_WAITING
#                 stock.pop("owner", None)
#                 return

#             # 1) Trade Limit Check (atomic via Redis lua)
#             if not await TradeControl.can_trade(side_key, int(cfg.get("total_trades", 5) or 5)):
#                 logger.warning(f"🚫 [LIMIT] {stock['symbol']} limit reached for {side_key}")
#                 stock["status"] = MomentumEngine.ST_WAITING
#                 stock.pop("owner", None)
#                 return

#             is_bull = "bull" in side_key

#             # 2) SL from qualifying candle + clamp away from entry to prevent immediate exit
#             raw_sl = float(stock.get("mom_trigger_sl") or ltp)
#             min_gap = max(ltp * 0.005, 0.10)  # 0.5% or 10p

#             if is_bull:
#                 sl_px = min(raw_sl, ltp - min_gap)  # always below entry
#                 risk_per_share = max(ltp - sl_px, min_gap)
#             else:
#                 sl_px = max(raw_sl, ltp + min_gap)  # always above entry
#                 risk_per_share = max(sl_px - ltp, min_gap)

#             risk_amount = float(cfg.get("risk_trade_1", 2000) or 2000)
#             qty = floor(risk_amount / risk_per_share)

#             if qty <= 0:
#                 stock["status"] = MomentumEngine.ST_WAITING
#                 stock.pop("owner", None)
#                 return

#             # 3) Place Entry Order (blocking -> thread)
#             order_id = await asyncio.to_thread(
#                 kite.place_order,
#                 variety=kite.VARIETY_REGULAR,
#                 exchange=kite.EXCHANGE_NSE,
#                 tradingsymbol=stock["symbol"],
#                 transaction_type=kite.TRANSACTION_TYPE_BUY if is_bull else kite.TRANSACTION_TYPE_SELL,
#                 quantity=qty,
#                 product=kite.PRODUCT_MIS,
#                 order_type=kite.ORDER_TYPE_MARKET,
#             )

#             rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
#             target_price = (
#                 round(ltp + (risk_per_share * rr_val), 2) if is_bull else round(ltp - (risk_per_share * rr_val), 2)
#             )

#             now = datetime.now(IST)
#             trade = {
#                 "symbol": stock["symbol"],
#                 "qty": qty,
#                 "entry_price": float(ltp),
#                 "sl_price": float(sl_px),
#                 "target_price": float(target_price),
#                 "order_id": order_id,
#                 "pnl": 0.0,
#                 "entry_time": now.strftime("%H:%M:%S"),
#                 # ✅ arming window
#                 "filled_at": now,
#                 "armed_at": now + timedelta(seconds=0.5),
#                 # ✅ trailing helpers
#                 "peak": float(ltp),    # for bull
#                 "trough": float(ltp),  # for bear
#                 "side_key": side_key,
#             }

#             state["trades"][side_key].append(trade)
#             stock["active_trade"] = trade
#             stock["status"] = MomentumEngine.ST_OPEN

#             logger.info(
#                 f"🚀 [MOM ENTRY] {stock['symbol']} {side_key.upper()} Qty:{qty} "
#                 f"| OID:{order_id} | SL:{sl_px} | TGT:{target_price}"
#             )

#         except Exception as e:
#             logger.exception(f"❌ [MOM ENTRY ERROR] {stock.get('symbol')}: {e}")
#             stock["status"] = MomentumEngine.ST_WAITING
#             stock["active_trade"] = None
#             stock.pop("owner", None)
#         finally:
#             stock.pop("mom_trigger_sl", None)
#             stock.pop("mom_trigger_bucket", None)

#     @staticmethod
#     async def monitor_active_trade(stock: dict, ltp: float, state: dict):
#         """
#         Real-time monitoring of open momentum positions.
#         """
#         trade = stock.get("active_trade")
#         if not trade:
#             stock["status"] = MomentumEngine.ST_WAITING
#             stock.pop("owner", None)
#             return

#         if stock.get("status") == MomentumEngine.ST_EXITING:
#             return

#         side_key = str(trade.get("side_key") or stock.get("side_latch", "")).lower()
#         cfg = state["config"].get(side_key, {})
#         is_bull = "bull" in side_key

#         entry = float(trade.get("entry_price", 0) or 0)
#         qty = int(trade.get("qty", 0) or 0)

#         # Manual exit first
#         if stock.get("symbol") in state.get("manual_exits", set()):
#             state["manual_exits"].discard(stock["symbol"])
#             logger.info(f"🖱️ [MOM-MANUAL EXIT] {stock['symbol']}")
#             _safe_create_task(
#                 MomentumEngine.close_position(stock, state, "MANUAL"),
#                 name=f"mom_close:MANUAL:{stock['symbol']}",
#             )
#             return

#         # Arming window skip
#         now = datetime.now(IST)
#         armed_at = trade.get("armed_at")
#         if armed_at and isinstance(armed_at, datetime) and now < armed_at:
#             return

#         # Peak / trough tracking
#         if is_bull:
#             trade["peak"] = max(float(trade.get("peak", entry) or entry), float(ltp))
#         else:
#             trade["trough"] = min(float(trade.get("trough", entry) or entry), float(ltp))

#         # PnL + exit conditions
#         sl = float(trade.get("sl_price", 0) or 0)
#         target = float(trade.get("target_price", 0) or 0)

#         if is_bull:
#             trade["pnl"] = round((ltp - entry) * qty, 2)
#             target_hit = (target > 0 and ltp >= target)
#             sl_hit = (ltp <= sl)
#         else:
#             trade["pnl"] = round((entry - ltp) * qty, 2)
#             target_hit = (target > 0 and ltp <= target)
#             sl_hit = (ltp >= sl)

#         if target_hit:
#             logger.info(f"🎯 [MOM-TARGET] {stock['symbol']} hit target {target} (LTP {ltp})")
#             _safe_create_task(
#                 MomentumEngine.close_position(stock, state, "TARGET"),
#                 name=f"mom_close:TARGET:{stock['symbol']}",
#             )
#             return

#         if sl_hit:
#             logger.info(f"🛑 [MOM-SL] {stock['symbol']} hit SL {sl} (LTP {ltp})")
#             _safe_create_task(
#                 MomentumEngine.close_position(stock, state, "SL"),
#                 name=f"mom_close:SL:{stock['symbol']}",
#             )
#             return

#         # Trailing SL (peak/trough-based + clamp away from LTP)
#         tsl_ratio = float(str(cfg.get("trailing_sl", "1:1.5")).split(":")[-1])
#         new_sl = await MomentumEngine.calculate_tsl(trade, tsl_ratio, is_bull)

#         buffer = max(ltp * 0.001, 0.10)

#         if is_bull:
#             new_sl = min(float(new_sl), float(ltp) - buffer)
#             if new_sl > float(trade["sl_price"]):
#                 trade["sl_price"] = round(new_sl, 2)
#         else:
#             new_sl = max(float(new_sl), float(ltp) + buffer)
#             if new_sl < float(trade["sl_price"]):
#                 trade["sl_price"] = round(new_sl, 2)

#     @staticmethod
#     async def calculate_tsl(trade: dict, ratio: float, is_bull: bool):
#         """
#         Peak/trough-based TSL:
#         - starts trailing only after profit > risk * ratio
#         - trails at peak - risk*0.9 (bull) or trough + risk*0.9 (bear)
#         """
#         entry = float(trade.get("entry_price", 0) or 0)
#         sl = float(trade.get("sl_price", 0) or 0)

#         if is_bull:
#             peak = float(trade.get("peak", entry) or entry)
#             risk = max(entry - sl, entry * 0.005)
#             profit = peak - entry
#             if profit > (risk * ratio):
#                 return peak - (risk * 0.9)
#             return sl
#         else:
#             trough = float(trade.get("trough", entry) or entry)
#             risk = max(sl - entry, entry * 0.005)
#             profit = entry - trough
#             if profit > (risk * ratio):
#                 return trough + (risk * 0.9)
#             return sl

#     @staticmethod
#     async def close_position(stock: dict, state: dict, reason: str):
#         """
#         Places real market order to exit the momentum position.
#         Runs in background task to avoid blocking tick pipeline.
#         """
#         if stock.get("status") == MomentumEngine.ST_EXITING:
#             return
#         stock["status"] = MomentumEngine.ST_EXITING

#         trade = stock.get("active_trade")
#         kite = state.get("kite")

#         # Determine side from trade/side_latch
#         side_key = str(trade.get("side_key") if trade else stock.get("side_latch", "")).lower()
#         is_bull = "bull" in side_key

#         if trade and kite:
#             try:
#                 exit_id = await asyncio.to_thread(
#                     kite.place_order,
#                     variety=kite.VARIETY_REGULAR,
#                     exchange=kite.EXCHANGE_NSE,
#                     tradingsymbol=stock["symbol"],
#                     transaction_type=kite.TRANSACTION_TYPE_SELL if is_bull else kite.TRANSACTION_TYPE_BUY,
#                     quantity=int(trade["qty"]),
#                     product=kite.PRODUCT_MIS,
#                     order_type=kite.ORDER_TYPE_MARKET,
#                 )
#                 logger.info(f"🏁 [MOM EXIT] {stock['symbol']} Reason:{reason} | OrderID:{exit_id}")
#             except Exception as e:
#                 logger.exception(f"❌ [KITE MOM EXIT ERROR] {stock.get('symbol')}: {e}")

#         # Cleanup
#         stock["status"] = MomentumEngine.ST_WAITING
#         stock["active_trade"] = None
#         stock.pop("owner", None)
#         stock.pop("trigger_px", None)
#         stock.pop("side_latch", None)
#         stock.pop("mom_trigger_sl", None)
#         stock.pop("mom_trigger_bucket", None)
# momentum_engine.py
import asyncio
import logging
from datetime import datetime, time as dtime
from math import floor
import pytz

from redis_manager import TradeControl

logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")


class MomentumEngine:
    """
    Momentum (Bull + Bear) Engine

    Status flow:
      WAITING -> (momentum candle qualified) -> MOM_TRIGGER_WATCH -> (ltp breaks trigger) -> MOM_OPEN -> (target/sl/manual) -> WAITING

    Implemented requirements:
      ✅ Bull + Bear momentum logic (uses pdh/pdl + 1m candle confirmation)
      ✅ Entry buffer removed:
           - Long entry: ltp > trigger_high
           - Short entry: ltp < trigger_low
      ✅ Exit buffer only: 0.01% (applied to target/SL hits)
      ✅ Step trailing SL (uses cfg trailing_sl ratio)
      ✅ Volume matrix filter (same matrix structure as breakout)
      ✅ Scanner enrichment when qualified:
           stock["scan_vol"]    = candle["volume"]
           stock["scan_reason"] = "Momentum candle + Vol OK"
      ✅ Candle volume fix:
           stock["last_vol"] initialized on first tick of new candle
      ✅ Engine toggle + trade window gating for new entries (existing positions always monitored)
    """

    EXIT_BUFFER_PCT = 0.0001  # 0.01%

    # -----------------------------
    # MAIN LOOP
    # -----------------------------
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        stock = state["stocks"].get(token)
        if not stock:
            return

        # Keep last price for scanner / % change calc
        stock["ltp"] = float(ltp or 0.0)

        # 1) Monitor open positions always
        if stock.get("status") == "MOM_OPEN":
            await MomentumEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2) Trigger watch: enter on break (obey engine toggle + time window)
        if stock.get("status") == "MOM_TRIGGER_WATCH":
            side = (stock.get("side_latch") or "").lower()
            if side not in ("mom_bull", "mom_bear"):
                MomentumEngine._reset_waiting(stock)
                return

            if not bool(state["engine_live"].get(side, True)):
                return

            if not MomentumEngine._within_trade_window(state["config"].get(side, {})):
                MomentumEngine._reset_waiting(stock)
                return

            trig = float(stock.get("trigger_px", 0.0) or 0.0)
            if trig <= 0:
                MomentumEngine._reset_waiting(stock)
                return

            if side == "mom_bull":
                if ltp > trig:
                    logger.info(f"⚡ [MOM-TRIGGER] {stock['symbol']} BULL break @ {ltp} > {trig}")
                    await MomentumEngine.open_trade(token, stock, ltp, state, "mom_bull")
            else:
                if ltp < trig:
                    logger.info(f"⚡ [MOM-TRIGGER] {stock['symbol']} BEAR break @ {ltp} < {trig}")
                    await MomentumEngine.open_trade(token, stock, ltp, state, "mom_bear")
            return

        # 3) 1-minute candle aggregation
        now = datetime.now(IST)
        bucket = now.replace(second=0, microsecond=0)

        if stock.get("candle") and stock["candle"]["bucket"] != bucket:
            closed = stock["candle"]
            asyncio.create_task(MomentumEngine.analyze_candle_logic(token, closed, state))

            stock["candle"] = {
                "bucket": bucket,
                "open": float(ltp),
                "high": float(ltp),
                "low": float(ltp),
                "close": float(ltp),
                "volume": 0,
            }

            # ✅ init last_vol for new candle
            stock["last_vol"] = int(vol)

        elif not stock.get("candle"):
            stock["candle"] = {
                "bucket": bucket,
                "open": float(ltp),
                "high": float(ltp),
                "low": float(ltp),
                "close": float(ltp),
                "volume": 0,
            }

            # ✅ init last_vol so first-minute deltas work
            stock["last_vol"] = int(vol)

        else:
            c = stock["candle"]
            c["high"] = max(float(c["high"]), float(ltp))
            c["low"] = min(float(c["low"]), float(ltp))
            c["close"] = float(ltp)

            last_vol = int(stock.get("last_vol", 0) or 0)
            if last_vol > 0:
                c["volume"] += max(0, int(vol) - last_vol)

            stock["last_vol"] = int(vol)

    # -----------------------------
    # CANDLE ANALYSIS (MOMENTUM QUALIFICATION)
    # -----------------------------
    @staticmethod
    async def analyze_candle_logic(token: int, candle: dict, state: dict):
        stock = state["stocks"].get(token)
        if not stock:
            return

        symbol = stock.get("symbol")
        if not symbol:
            return

        # do not overwrite trigger watch
        if stock.get("status") == "MOM_TRIGGER_WATCH":
            return

        now = datetime.now(IST)

        pdh = float(stock.get("pdh", 0) or 0)
        pdl = float(stock.get("pdl", 0) or 0)
        if pdh <= 0 or pdl <= 0:
            return

        high = float(candle.get("high", 0) or 0)
        low = float(candle.get("low", 0) or 0)
        close = float(candle.get("close", 0) or 0)
        c_vol = int(candle.get("volume", 0) or 0)

        if close <= 0 or high <= 0 or low <= 0:
            return

        # Momentum rule (simple + practical):
        # - Bull: candle closes above PDH (strength)
        # - Bear: candle closes below PDL (weakness)
        # You can tighten later (e.g., close near high/low, body%, etc.)
        if close > pdh:
            side = "mom_bull"
            if not bool(state["engine_live"].get(side, True)):
                return
            if not MomentumEngine._within_trade_window(state["config"].get(side, {}), now=now):
                return

            ok, detail = await MomentumEngine.check_vol_matrix(stock, candle, side, state)
            if not ok:
                logger.info(f"❌ [MOM-REJECT] {symbol} BULL | {detail}")
                return

            stock["status"] = "MOM_TRIGGER_WATCH"
            stock["side_latch"] = side
            stock["trigger_px"] = float(high)  # no entry buffer

            # ✅ Scanner enrichment requested
            stock["scan_vol"] = int(c_vol)
            stock["scan_reason"] = "Momentum candle + Vol OK"

            logger.info(f"✅ [MOM-QUALIFIED] {symbol} BULL | Trigger @ {high} | {detail}")
            return

        if close < pdl:
            side = "mom_bear"
            if not bool(state["engine_live"].get(side, True)):
                return
            if not MomentumEngine._within_trade_window(state["config"].get(side, {}), now=now):
                return

            ok, detail = await MomentumEngine.check_vol_matrix(stock, candle, side, state)
            if not ok:
                logger.info(f"❌ [MOM-REJECT] {symbol} BEAR | {detail}")
                return

            stock["status"] = "MOM_TRIGGER_WATCH"
            stock["side_latch"] = side
            stock["trigger_px"] = float(low)

            # ✅ Scanner enrichment requested
            stock["scan_vol"] = int(c_vol)
            stock["scan_reason"] = "Momentum candle + Vol OK"

            logger.info(f"✅ [MOM-QUALIFIED] {symbol} BEAR | Trigger @ {low} | {detail}")
            return

    # -----------------------------
    # VOLUME MATRIX (same structure)
    # -----------------------------
    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
        cfg = state["config"].get(side, {})
        matrix = cfg.get("volume_criteria", []) or []

        c_vol = int(candle.get("volume", 0) or 0)
        s_sma = float(stock.get("sma", 0) or 0)
        close = float(candle.get("close", 0) or 0)

        c_val_cr = (c_vol * close) / 10000000.0 if close > 0 else 0.0

        if not matrix:
            return True, "No Matrix"

        tier_found = None
        for i, level in enumerate(matrix):
            try:
                min_sma_avg = float(level.get("min_sma_avg", 0) or 0)
            except Exception:
                min_sma_avg = 0.0

            if s_sma >= min_sma_avg:
                tier_found = (i, level)
            else:
                break

        if not tier_found:
            return False, f"SMA {s_sma:,.0f} too low"

        idx, level = tier_found
        required_vol = s_sma * float(level.get("sma_multiplier", 1.0) or 1.0)
        min_cr = float(level.get("min_vol_price_cr", 0) or 0)

        if c_vol >= required_vol and c_val_cr >= min_cr:
            return True, f"Tier {idx+1} Pass"
        return False, f"Tier {idx+1} Fail (Vol/Value)"

    # -----------------------------
    # ORDER EXECUTION
    # -----------------------------
    @staticmethod
    async def open_trade(token: int, stock: dict, ltp: float, state: dict, side_key: str):
        cfg = state["config"].get(side_key, {})
        kite = state.get("kite")

        if not kite:
            logger.error(f"❌ [KITE ERROR] Session missing for {stock.get('symbol')}")
            MomentumEngine._reset_waiting(stock)
            return

        if not bool(state["engine_live"].get(side_key, True)):
            MomentumEngine._reset_waiting(stock)
            return
        if not MomentumEngine._within_trade_window(cfg):
            MomentumEngine._reset_waiting(stock)
            return

        # Trade limit check (Redis)
        limit = int(cfg.get("total_trades", 5) or 5)
        if not await TradeControl.can_trade(side_key, limit):
            logger.warning(f"🚫 [LIMIT] {stock['symbol']} limit reached for {side_key}")
            MomentumEngine._reset_waiting(stock)
            return

        is_bull = (side_key == "mom_bull")

        # Initial SL based on momentum candle opposite extreme
        trig_candle = stock.get("candle") or {}
        if is_bull:
            sl_px = float(trig_candle.get("low", 0) or 0)
        else:
            sl_px = float(trig_candle.get("high", 0) or 0)

        entry = float(ltp)
        if sl_px <= 0:
            sl_px = round(entry * (0.995 if is_bull else 1.005), 2)

        risk_per_share = max(abs(entry - sl_px), entry * 0.005)
        risk_amount = float(cfg.get("risk_trade_1", 2000) or 2000)
        qty = floor(risk_amount / risk_per_share)

        if qty <= 0:
            MomentumEngine._reset_waiting(stock)
            return

        try:
            order_id = await asyncio.to_thread(
                kite.place_order,
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=stock["symbol"],
                transaction_type=(kite.TRANSACTION_TYPE_BUY if is_bull else kite.TRANSACTION_TYPE_SELL),
                quantity=qty,
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET,
            )

            rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
            target = round(entry + (risk_per_share * rr_val), 2) if is_bull else round(entry - (risk_per_share * rr_val), 2)

            tsl_ratio = float(str(cfg.get("trailing_sl", "1:1.5")).split(":")[-1])
            trail_step = float(risk_per_share * tsl_ratio) if tsl_ratio > 0 else float(risk_per_share)

            trade = {
                "symbol": stock["symbol"],
                "qty": int(qty),
                "entry_price": float(entry),
                "sl_price": float(sl_px),
                "target_price": float(target),
                "order_id": order_id,
                "pnl": 0.0,
                "entry_time": datetime.now(IST).strftime("%H:%M:%S"),
                "init_risk": float(risk_per_share),
                "trail_step": float(trail_step),
            }

            state["trades"][side_key].append(trade)
            stock["status"] = "MOM_OPEN"
            stock["active_trade"] = trade
            stock["side_latch"] = side_key

            # Clear scanner fields once trade is live
            stock["scan_seen_ts"] = None
            stock["scan_seen_time"] = None
            stock["scan_vol"] = 0
            stock["scan_reason"] = None

            logger.info(
                f"🚀 [MOM REAL ENTRY] {stock['symbol']} {side_key.upper()} | Qty: {qty} | OrderID: {order_id}"
            )

        except Exception as e:
            logger.error(f"❌ [KITE ORDER ERROR] {stock['symbol']}: {e}")
            MomentumEngine._reset_waiting(stock)

    # -----------------------------
    # MONITOR + EXIT + STEP TRAIL
    # -----------------------------
    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        trade = stock.get("active_trade")
        if not trade:
            return

        side_key = (stock.get("side_latch") or "").lower()
        is_bull = (side_key == "mom_bull")

        entry = float(trade.get("entry_price", 0) or 0)
        qty = int(trade.get("qty", 0) or 0)
        sl = float(trade.get("sl_price", 0) or 0)
        target = float(trade.get("target_price", 0) or 0)

        if entry <= 0 or qty <= 0:
            await MomentumEngine.close_position(stock, state, "BAD_TRADE_STATE")
            return

        if is_bull:
            trade["pnl"] = round((float(ltp) - entry) * qty, 2)
        else:
            trade["pnl"] = round((entry - float(ltp)) * qty, 2)

        b = MomentumEngine.EXIT_BUFFER_PCT
        if is_bull:
            target_hit = float(ltp) >= (target * (1.0 - b))
            sl_hit = float(ltp) <= (sl * (1.0 + b))
        else:
            target_hit = float(ltp) <= (target * (1.0 + b))
            sl_hit = float(ltp) >= (sl * (1.0 - b))

        if target_hit:
            logger.info(f"🎯 [MOM-TARGET] {stock['symbol']} hit target {target}")
            await MomentumEngine.close_position(stock, state, "TARGET")
            return

        if sl_hit:
            logger.info(f"🛑 [MOM-STOPLOSS] {stock['symbol']} hit SL {sl}")
            await MomentumEngine.close_position(stock, state, "SL")
            return

        new_sl = MomentumEngine._step_trail_sl(trade, float(ltp), is_bull)
        if new_sl is not None:
            if is_bull and new_sl > float(trade.get("sl_price", 0) or 0):
                trade["sl_price"] = float(new_sl)
            elif (not is_bull) and new_sl < float(trade.get("sl_price", 0) or 0):
                trade["sl_price"] = float(new_sl)

        # Legacy manual exit list
        if stock.get("symbol") in state.get("manual_exits", set()):
            logger.info(f"🖱️ [MOM-MANUAL EXIT] {stock['symbol']}")
            await MomentumEngine.close_position(stock, state, "MANUAL")
            state["manual_exits"].remove(stock["symbol"])

    @staticmethod
    def _step_trail_sl(trade: dict, ltp: float, is_bull: bool):
        entry = float(trade.get("entry_price", 0) or 0)
        step = float(trade.get("trail_step", 0) or 0)
        if entry <= 0 or step <= 0:
            return None

        profit = (ltp - entry) if is_bull else (entry - ltp)
        if profit <= 0:
            return None

        k = int(profit // step)
        if k < 1:
            return None

        if is_bull:
            return round(entry + ((k - 1) * step), 2)
        return round(entry - ((k - 1) * step), 2)

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        trade = stock.get("active_trade")
        kite = state.get("kite")
        side_key = (stock.get("side_latch") or "").lower()
        is_bull = (side_key == "mom_bull")

        if trade and kite:
            try:
                exit_id = await asyncio.to_thread(
                    kite.place_order,
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=stock["symbol"],
                    transaction_type=(kite.TRANSACTION_TYPE_SELL if is_bull else kite.TRANSACTION_TYPE_BUY),
                    quantity=int(trade["qty"]),
                    product=kite.PRODUCT_MIS,
                    order_type=kite.ORDER_TYPE_MARKET,
                )
                logger.info(f"🏁 [MOM REAL EXIT] {stock['symbol']} Reason: {reason} | OrderID: {exit_id}")
            except Exception as e:
                logger.error(f"❌ [KITE MOM EXIT ERROR] {stock['symbol']}: {e}")

        try:
            if side_key in state["trades"] and trade:
                state["trades"][side_key] = [t for t in state["trades"][side_key] if t is not trade]
        except Exception:
            pass

        MomentumEngine._reset_waiting(stock)

    # -----------------------------
    # HELPERS
    # -----------------------------
    @staticmethod
    def _reset_waiting(stock: dict):
        stock["status"] = "WAITING"
        stock["active_trade"] = None

        stock.pop("trigger_px", None)
        stock.pop("side_latch", None)

        stock["scan_seen_ts"] = None
        stock["scan_seen_time"] = None
        stock["scan_vol"] = 0
        stock["scan_reason"] = None

    @staticmethod
    def _within_trade_window(cfg: dict, now: datetime | None = None) -> bool:
        """
        cfg: {"trade_start":"HH:MM", "trade_end":"HH:MM"}
        """
        try:
            now = now or datetime.now(IST)
            start_s = str(cfg.get("trade_start", "09:15"))
            end_s = str(cfg.get("trade_end", "09:17"))

            sh, sm = int(start_s.split(":")[0]), int(start_s.split(":")[1])
            eh, em = int(end_s.split(":")[0]), int(end_s.split(":")[1])

            start_t = dtime(sh, sm)
            end_t = dtime(eh, em)
            nt = now.time()

            return (nt >= start_t) and (nt <= end_t)
        except Exception:
            return True
