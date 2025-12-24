# import asyncio
# import logging
# from datetime import datetime
# from math import floor
# from typing import Optional
# import pytz

# from redis_manager import TradeControl

# logger = logging.getLogger("Nexus_Momentum")
# IST = pytz.timezone("Asia/Kolkata")


# class MomentumEngine:
#     """
#     Momentum Engine (Bull/Bear)

#     Fixes implemented:
#       ✅ Uses per-engine keys (mom_*) to avoid collision with BreakoutEngine
#       ✅ Per-symbol trades/day cap = 2 (Redis)
#       ✅ 2nd trade only after 1st closed (Redis open-lock)
#       ✅ Side limit total_trades is atomic & rollback-safe
#       ✅ Trades are marked CLOSED (not removed) so PnL stays correct
#       ✅ Candle logic is called from main tick_worker on candle-close
#     """

#     EXIT_BUFFER_PCT = 0.0001
#     MAX_TRADES_PER_SYMBOL = 2

#     @staticmethod
#     async def run(token: int, ltp: float, vol: int, state: dict):
#         stock = state["stocks"].get(token)
#         if not stock:
#             return

#         stock["ltp"] = float(ltp or 0.0)

#         mom_status = stock.get("mom_status", "WAITING")

#         # monitor open trade regardless of engine toggle
#         if mom_status == "OPEN":
#             await MomentumEngine.monitor_active_trade(stock, ltp, state)
#             return

#         if mom_status == "TRIGGER_WATCH":
#             side = (stock.get("mom_side_latch") or "").lower()
#             if side not in ("mom_bull", "mom_bear"):
#                 MomentumEngine._reset_waiting(stock)
#                 return

#             if not bool(state["engine_live"].get(side, True)):
#                 return

#             if not MomentumEngine._within_trade_window(state["config"].get(side, {})):
#                 MomentumEngine._reset_waiting(stock)
#                 return

#             trig = float(stock.get("mom_trigger_px", 0.0) or 0.0)
#             if trig <= 0:
#                 MomentumEngine._reset_waiting(stock)
#                 return

#             if side == "mom_bull" and ltp > trig:
#                 await MomentumEngine.open_trade(stock, ltp, state, "mom_bull")
#             elif side == "mom_bear" and ltp < trig:
#                 await MomentumEngine.open_trade(stock, ltp, state, "mom_bear")
#             return

#     # candle-close qualification called by main
#     @staticmethod
#     async def on_candle_close(token: int, candle: dict, state: dict):
#         stock = state["stocks"].get(token)
#         if not stock:
#             return

#         symbol = stock.get("symbol")
#         if not symbol:
#             return

#         if stock.get("mom_status") in ("OPEN", "TRIGGER_WATCH"):
#             return

#         taken = await TradeControl.get_symbol_trade_count(symbol)
#         if taken >= MomentumEngine.MAX_TRADES_PER_SYMBOL:
#             return

#         pdh = float(stock.get("pdh", 0) or 0)
#         pdl = float(stock.get("pdl", 0) or 0)
#         if pdh <= 0 or pdl <= 0:
#             return

#         high = float(candle.get("high", 0) or 0)
#         low = float(candle.get("low", 0) or 0)
#         close = float(candle.get("close", 0) or 0)
#         c_vol = int(candle.get("volume", 0) or 0)
#         if close <= 0 or high <= 0 or low <= 0:
#             return

#         now = datetime.now(IST)

#         if close > pdh:
#             side = "mom_bull"
#             if not bool(state["engine_live"].get(side, True)):
#                 return
#             if not MomentumEngine._within_trade_window(state["config"].get(side, {}), now=now):
#                 return

#             ok, detail = await MomentumEngine.check_vol_matrix(stock, candle, side, state)
#             if not ok:
#                 return

#             stock["mom_status"] = "TRIGGER_WATCH"
#             stock["mom_side_latch"] = side
#             stock["mom_trigger_px"] = float(high)
#             stock["mom_trigger_candle"] = dict(candle)

#             stock["mom_scan_vol"] = int(c_vol)
#             stock["mom_scan_reason"] = f"Momentum candle + Vol OK ({detail})"
#             return

#         if close < pdl:
#             side = "mom_bear"
#             if not bool(state["engine_live"].get(side, True)):
#                 return
#             if not MomentumEngine._within_trade_window(state["config"].get(side, {}), now=now):
#                 return

#             ok, detail = await MomentumEngine.check_vol_matrix(stock, candle, side, state)
#             if not ok:
#                 return

#             stock["mom_status"] = "TRIGGER_WATCH"
#             stock["mom_side_latch"] = side
#             stock["mom_trigger_px"] = float(low)
#             stock["mom_trigger_candle"] = dict(candle)

#             stock["mom_scan_vol"] = int(c_vol)
#             stock["mom_scan_reason"] = f"Momentum candle + Vol OK ({detail})"
#             return

#     @staticmethod
#     async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
#         cfg = state["config"].get(side, {})
#         matrix = cfg.get("volume_criteria", []) or []

#         c_vol = int(candle.get("volume", 0) or 0)
#         s_sma = float(stock.get("sma", 0) or 0)
#         close = float(candle.get("close", 0) or 0)
#         c_val_cr = (c_vol * close) / 10000000.0 if close > 0 else 0.0

#         if not matrix:
#             return True, "NoMatrix"

#         tier_found = None
#         for i, level in enumerate(matrix):
#             min_sma_avg = float(level.get("min_sma_avg", 0) or 0)
#             if s_sma >= min_sma_avg:
#                 tier_found = (i, level)
#             else:
#                 break

#         if not tier_found:
#             return False, f"SMA {s_sma:,.0f} too low"

#         idx, level = tier_found
#         required_vol = s_sma * float(level.get("sma_multiplier", 1.0) or 1.0)
#         min_cr = float(level.get("min_vol_price_cr", 0) or 0)

#         if c_vol >= required_vol and c_val_cr >= min_cr:
#             return True, f"Tier{idx+1}Pass"
#         return False, f"Tier{idx+1}Fail"

#     @staticmethod
#     async def open_trade(stock: dict, ltp: float, state: dict, side_key: str):
#         symbol = stock.get("symbol") or ""
#         if not symbol:
#             MomentumEngine._reset_waiting(stock)
#             return

#         cfg = state["config"].get(side_key, {})
#         kite = state.get("kite")
#         if not kite:
#             MomentumEngine._reset_waiting(stock)
#             return

#         if not bool(state["engine_live"].get(side_key, True)):
#             MomentumEngine._reset_waiting(stock)
#             return
#         if not MomentumEngine._within_trade_window(cfg):
#             MomentumEngine._reset_waiting(stock)
#             return

#         # side limit reservation
#         side_limit = int(cfg.get("total_trades", 5) or 5)
#         if not await TradeControl.reserve_side_trade(side_key, side_limit):
#             MomentumEngine._reset_waiting(stock)
#             return

#         # symbol reservation (2/day + open lock)
#         ok, reason = await TradeControl.reserve_symbol_trade(symbol, max_trades=MomentumEngine.MAX_TRADES_PER_SYMBOL)
#         if not ok:
#             await TradeControl.rollback_side_trade(side_key)
#             MomentumEngine._reset_waiting(stock)
#             return

#         is_bull = (side_key == "mom_bull")

#         trig_candle = stock.get("mom_trigger_candle") or {}
#         sl_px = float(trig_candle.get("low", 0) or 0) if is_bull else float(trig_candle.get("high", 0) or 0)

#         entry = float(ltp)
#         if sl_px <= 0:
#             sl_px = round(entry * (0.995 if is_bull else 1.005), 2)

#         risk_per_share = max(abs(entry - sl_px), entry * 0.005)
#         risk_amount = float(cfg.get("risk_trade_1", 2000) or 2000)
#         qty = floor(risk_amount / risk_per_share)

#         if qty <= 0:
#             await TradeControl.rollback_symbol_trade(symbol)
#             await TradeControl.rollback_side_trade(side_key)
#             MomentumEngine._reset_waiting(stock)
#             return

#         try:
#             order_id = await asyncio.to_thread(
#                 kite.place_order,
#                 variety=kite.VARIETY_REGULAR,
#                 exchange=kite.EXCHANGE_NSE,
#                 tradingsymbol=symbol,
#                 transaction_type=(kite.TRANSACTION_TYPE_BUY if is_bull else kite.TRANSACTION_TYPE_SELL),
#                 quantity=qty,
#                 product=kite.PRODUCT_MIS,
#                 order_type=kite.ORDER_TYPE_MARKET,
#             )

#             rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
#             target = round(entry + (risk_per_share * rr_val), 2) if is_bull else round(entry - (risk_per_share * rr_val), 2)

#             tsl_ratio = float(str(cfg.get("trailing_sl", "1:1.5")).split(":")[-1])
#             trail_step = float(risk_per_share * tsl_ratio) if tsl_ratio > 0 else float(risk_per_share)

#             trade = {
#                 "engine": "momentum",
#                 "side": side_key,
#                 "symbol": symbol,
#                 "qty": int(qty),
#                 "entry_price": float(entry),
#                 "sl_price": float(sl_px),
#                 "target_price": float(target),
#                 "order_id": order_id,
#                 "pnl": 0.0,
#                 "status": "OPEN",
#                 "entry_time": datetime.now(IST).strftime("%H:%M:%S"),
#                 "init_risk": float(risk_per_share),
#                 "trail_step": float(trail_step),
#             }

#             state["trades"][side_key].append(trade)

#             stock["mom_status"] = "OPEN"
#             stock["mom_active_trade"] = trade
#             stock["mom_side_latch"] = side_key

#             stock["mom_scan_seen_ts"] = None
#             stock["mom_scan_seen_time"] = None

#         except Exception as e:
#             logger.error(f"❌ [MOM ORDER ERROR] {symbol}: {e}")
#             await TradeControl.rollback_symbol_trade(symbol)
#             await TradeControl.rollback_side_trade(side_key)
#             MomentumEngine._reset_waiting(stock)

#     @staticmethod
#     async def monitor_active_trade(stock: dict, ltp: float, state: dict):
#         trade = stock.get("mom_active_trade")
#         if not trade:
#             return

#         side_key = (stock.get("mom_side_latch") or "").lower()
#         is_bull = (side_key == "mom_bull")

#         entry = float(trade.get("entry_price", 0) or 0)
#         qty = int(trade.get("qty", 0) or 0)
#         sl = float(trade.get("sl_price", 0) or 0)
#         target = float(trade.get("target_price", 0) or 0)

#         if entry <= 0 or qty <= 0:
#             await MomentumEngine.close_position(stock, state, "BAD_TRADE_STATE")
#             return

#         trade["pnl"] = round((float(ltp) - entry) * qty, 2) if is_bull else round((entry - float(ltp)) * qty, 2)

#         b = MomentumEngine.EXIT_BUFFER_PCT
#         if is_bull:
#             target_hit = float(ltp) >= (target * (1.0 - b))
#             sl_hit = float(ltp) <= (sl * (1.0 + b))
#         else:
#             target_hit = float(ltp) <= (target * (1.0 + b))
#             sl_hit = float(ltp) >= (sl * (1.0 - b))

#         if target_hit:
#             await MomentumEngine.close_position(stock, state, "TARGET")
#             return
#         if sl_hit:
#             await MomentumEngine.close_position(stock, state, "SL")
#             return

#         new_sl = MomentumEngine._step_trail_sl(trade, float(ltp), is_bull)
#         if new_sl is not None:
#             cur = float(trade.get("sl_price", 0) or 0)
#             if is_bull and new_sl > cur:
#                 trade["sl_price"] = float(new_sl)
#             elif (not is_bull) and new_sl < cur:
#                 trade["sl_price"] = float(new_sl)

#         if stock.get("symbol") in state.get("manual_exits", set()):
#             await MomentumEngine.close_position(stock, state, "MANUAL")
#             state["manual_exits"].remove(stock["symbol"])

#     @staticmethod
#     def _step_trail_sl(trade: dict, ltp: float, is_bull: bool):
#         entry = float(trade.get("entry_price", 0) or 0)
#         step = float(trade.get("trail_step", 0) or 0)
#         if entry <= 0 or step <= 0:
#             return None

#         profit = (ltp - entry) if is_bull else (entry - ltp)
#         if profit <= 0:
#             return None

#         k = int(profit // step)
#         if k < 1:
#             return None

#         return round(entry + ((k - 1) * step), 2) if is_bull else round(entry - ((k - 1) * step), 2)

#     @staticmethod
#     async def close_position(stock: dict, state: dict, reason: str):
#         trade = stock.get("mom_active_trade")
#         kite = state.get("kite")
#         symbol = stock.get("symbol") or ""
#         side_key = (stock.get("mom_side_latch") or "").lower()
#         is_bull = (side_key == "mom_bull")

#         if trade and kite and symbol:
#             try:
#                 exit_id = await asyncio.to_thread(
#                     kite.place_order,
#                     variety=kite.VARIETY_REGULAR,
#                     exchange=kite.EXCHANGE_NSE,
#                     tradingsymbol=symbol,
#                     transaction_type=(kite.TRANSACTION_TYPE_SELL if is_bull else kite.TRANSACTION_TYPE_BUY),
#                     quantity=int(trade["qty"]),
#                     product=kite.PRODUCT_MIS,
#                     order_type=kite.ORDER_TYPE_MARKET,
#                 )
#                 trade["exit_order_id"] = exit_id
#             except Exception as e:
#                 logger.error(f"❌ [MOM EXIT ERROR] {symbol}: {e}")

#         if trade:
#             trade["status"] = "CLOSED"
#             trade["exit_time"] = datetime.now(IST).strftime("%H:%M:%S")
#             trade["exit_reason"] = reason

#         if symbol:
#             await TradeControl.release_symbol_lock(symbol)

#         MomentumEngine._reset_waiting(stock)

#     @staticmethod
#     def _reset_waiting(stock: dict):
#         stock["mom_status"] = "WAITING"
#         stock["mom_active_trade"] = None

#         stock.pop("mom_trigger_px", None)
#         stock.pop("mom_side_latch", None)
#         stock.pop("mom_trigger_candle", None)

#         stock["mom_scan_seen_ts"] = None
#         stock["mom_scan_seen_time"] = None
#         stock["mom_scan_vol"] = 0
#         stock["mom_scan_reason"] = None

#     @staticmethod
#     def _within_trade_window(cfg: dict, now: Optional[datetime] = None) -> bool:
#         from datetime import time as dtime
#         try:
#             now = now or datetime.now(IST)
#             start_s = str(cfg.get("trade_start", "09:15"))
#             end_s = str(cfg.get("trade_end", "09:17"))
#             sh, sm = map(int, start_s.split(":"))
#             eh, em = map(int, end_s.split(":"))
#             start_t = dtime(sh, sm)
#             end_t = dtime(eh, em)
#             nt = now.time()
#             return (nt >= start_t) and (nt <= end_t)
#         except Exception:
#             return True
import asyncio
import logging
from datetime import datetime, time
from math import floor
from typing import Tuple

from redis_manager import TradeControl, IST

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Nexus_Momentum")

class MomentumEngine:
    """
    Opening Momentum Engine (9:16 - 9:17 Window)
    -------------------------------------------
    ✅ Qualifies ONLY 09:15 AM Candle.
    ✅ Close vs PrevClose Difference check (Max 3%).
    ✅ Entry window: Strictly 09:16:00 to 09:17:59.
    ✅ No Candle Size/Range filters (As per request).
    ✅ High Break = BUY | Low Break = SELL.
    """

    MAX_TRADES_PER_SYMBOL = 1 
    # Entry Window Constants
    ENTRY_START_TIME = time(9, 16)
    ENTRY_END_TIME = time(9, 17, 59)

    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        """
        Har tick par momentum engine scan karta hai.
        Sirf 9:16:00 se 9:17:59 ke beech trade trigger karega.
        """
        stock = state["stocks"].get(token)
        if not stock: return

        # LTP update dashboard ke liye
        stock["ltp"] = float(ltp or 0.0)
        status = stock.get("mom_status", "WAITING")

        # 1. Active position monitor (PnL update)
        if status == "OPEN":
            await MomentumEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2. Entry Execution Check (Window: 09:16 - 09:17)
        if status == "TRIGGER_WATCH":
            now = datetime.now(IST).time()
            
            # Agar 9:18:00 ho gaya, toh trigger cancel
            if now > MomentumEngine.ENTRY_END_TIME:
                logger.info(f"⏳ [MOM] {stock['symbol']} entry window closed. Resetting.")
                MomentumEngine._reset_waiting(stock)
                return

            # Agar abhi 9:16 nahi baje, toh entry nahi leni
            if now < MomentumEngine.ENTRY_START_TIME:
                return

            # Side check and Trigger price check
            side = (stock.get("mom_side_latch") or "").lower()
            trig_high = float(stock.get("mom_high_trig", 0.0))
            trig_low = float(stock.get("mom_low_trig", 0.0))

            # --- BREAKOUT / BREAKDOWN EXECUTION ---
            if ltp > trig_high:
                await MomentumEngine.open_trade(stock, ltp, state, "mom_bull")
            elif ltp < trig_low:
                await MomentumEngine.open_trade(stock, ltp, state, "mom_bear")
            
            return

    @staticmethod
    async def on_candle_close(token: int, candle: dict, state: dict):
        """
        Sirf 09:15 ki candle close hone par filtration karta hai.
        3% Close vs PrevClose rule check hota hai.
        """
        stock = state["stocks"].get(token)
        if not stock: return

        # Check: Kya ye 09:15:00 ki candle hai?
        c_time = candle.get("bucket")
        if not c_time or c_time.hour != 9 or c_time.minute != 15:
            return

        if stock.get("mom_status") in ("OPEN", "TRIGGER_WATCH"): return

        symbol = stock.get("symbol")
        prev_close = float(stock.get("prev_close", 0))
        c_close = float(candle.get("close", 0))
        c_high = float(candle.get("high", 0))
        c_low = float(candle.get("low", 0))

        if prev_close <= 0: return

        # --- ✅ 3% PRICE DIFFERENCE RULE ---
        # 9:15 Close aur Yesterday Close ka diff 3% se zyada nahi hona chahiye
        price_diff_pct = abs(c_close - prev_close) / prev_close * 100.0
        
        if price_diff_pct > 3.0:
            logger.warning(f"🚫 [MOM-REJECT] {symbol} Gap too large: {price_diff_pct:.2f}%")
            return

        # Qualification Success: Set high and low triggers from 9:15 candle
        stock.update({
            "mom_status": "TRIGGER_WATCH",
            "mom_high_trig": c_high,
            "mom_low_trig": c_low,
            "mom_trigger_candle": dict(candle),
            "mom_set_ts": datetime.now(IST).timestamp(),
            "mom_scan_reason": f"9:15 Candle Valid (Diff: {price_diff_pct:.1f}%)"
        })
        
        logger.info(f"🚀 [MOM-QUALIFIED] {symbol} watching High: {c_high} Low: {c_low}")

    @staticmethod
    async def open_trade(stock, ltp: float, state: dict, side_key: str):
        """Heroku Optimized Order Placement."""
        symbol = stock["symbol"]
        kite = state.get("kite")
        cfg = state["config"].get(side_key, {})

        # Atomic Redis Lock (Double trade protection)
        ok, _ = await TradeControl.reserve_symbol_trade(symbol, 1)
        if not ok: 
            stock["mom_status"] = "WAITING"
            return

        is_bull = (side_key == "mom_bull")
        trig_c = stock.get("mom_trigger_candle") or {}
        
        # Stoploss setup
        sl_px = float(trig_c.get("low" if is_bull else "high", 0))
        risk = max(abs(ltp - sl_px), ltp * 0.005) # Min 0.5% risk
        qty = floor(float(cfg.get("risk_trade_1", 2000)) / risk)

        if qty <= 0:
            await TradeControl.rollback_symbol_trade(symbol)
            return

        try:
            # Transaction Mapping
            t_type = kite.TRANSACTION_TYPE_BUY if is_bull else kite.TRANSACTION_TYPE_SELL
            
            order_id = await asyncio.to_thread(
                kite.place_order,
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=symbol,
                transaction_type=t_type,
                quantity=int(qty),
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET
            )

            # Target calculation (RR from dashboard)
            rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
            target_px = ltp + (risk * rr_val) if is_bull else ltp - (risk * rr_val)

            trade = {
                "symbol": symbol, "side": side_key, "qty": qty, "entry_price": ltp,
                "sl_price": sl_px, "target_price": target_px, "oid": order_id,
                "status": "OPEN", "engine": "momentum", "pnl": 0.0,
                "time": datetime.now(IST).strftime("%H:%M:%S")
            }
            stock["mom_status"] = "OPEN"
            stock["mom_active_trade"] = trade
            state["trades"][side_key].append(trade)
            
            logger.info(f"🔥 [MOM-EXEC] {symbol} {side_key.upper()} OrderId: {order_id}")

        except Exception as e:
            logger.error(f"❌ [MOM] Execution Failed: {e}")
            await TradeControl.rollback_symbol_trade(symbol)
            stock["mom_status"] = "WAITING"

    @staticmethod
    async def monitor_active_trade(stock, ltp: float, state: dict):
        """Active PnL sync with Dashboard."""
        trade = stock.get("mom_active_trade")
        if not trade: return
        
        is_bull = (trade["side"] == "mom_bull")
        entry, qty = trade["entry_price"], trade["qty"]
        sl, target = trade["sl_price"], trade["target_price"]

        # Calculate PnL for index.html
        trade["pnl"] = round((ltp - entry) * qty if is_bull else (entry - ltp) * qty, 2)

        # Exit conditions
        if (is_bull and ltp <= sl) or (not is_bull and ltp >= sl):
            await MomentumEngine.close_position(stock, state, "MOM_SL")
        elif (is_bull and ltp >= target) or (not is_bull and ltp <= target):
            await MomentumEngine.close_position(stock, state, "MOM_TGT")

    @staticmethod
    async def close_position(stock, state, reason):
        trade = stock.get("mom_active_trade")
        if not trade or trade["status"] != "OPEN": return
        
        kite = state["kite"]
        try:
            # Reverse transaction to exit
            t_type = kite.TRANSACTION_TYPE_SELL if trade["side"] == "mom_bull" else kite.TRANSACTION_TYPE_BUY
            await asyncio.to_thread(
                kite.place_order, variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                tradingsymbol=stock["symbol"], transaction_type=t_type, quantity=trade["qty"],
                product=kite.PRODUCT_MIS, order_type=kite.ORDER_TYPE_MARKET
            )
            trade["status"] = "CLOSED"
            stock["mom_status"] = "CLOSED"
            await TradeControl.release_symbol_lock(stock["symbol"])
            logger.info(f"⏹️ [MOM-EXIT] {stock['symbol']} | {reason}")
        except Exception as e:
            logger.error(f"❌ [MOM] Exit Error: {e}")

    @staticmethod
    def _reset_waiting(stock: dict):
        stock["mom_status"] = "WAITING"
        stock["mom_active_trade"] = None
        stock.pop("mom_high_trig", None)
        stock.pop("mom_low_trig", None)
        stock.pop("mom_set_ts", None)

    @staticmethod
    def _within_trade_window(cfg: dict) -> bool:
        """Standard 9:16 to 9:17 window check."""
        now = datetime.now(IST).time()
        return MomentumEngine.ENTRY_START_TIME <= now <= MomentumEngine.ENTRY_END_TIME