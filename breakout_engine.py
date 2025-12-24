import asyncio
import logging
import os
from datetime import datetime
from math import floor
from typing import Optional, Tuple

from redis_manager import TradeControl, IST

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] BreakoutEngine: %(message)s"
)
logger = logging.getLogger("Nexus_Breakout")

class BreakoutEngine:
    """
    Advanced Breakout Engine (Bull/Bear)
    -----------------------------------
    ✅ Open < PDH (Bull) / Open > PDL (Bear) check.
    ✅ 0.5% Candle Size Filter.
    ✅ 0.5% Extension Limit (if candle size > 0.5%).
    ✅ 10-Tier Volume Matrix Confirmation.
    ✅ Strict Directional Latch (Side Latch).
    ✅ Redis Atomic Locking for Heroku Concurrency.
    """

    MAX_TRADES_PER_SYMBOL = 2
    TRIGGER_VALID_SECONDS = 6 * 60  # Trigger valid for 6 minutes

    # --- 1. TICK HANDLER (High Frequency) ---
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        """Processes every incoming tick for the specific stock."""
        stock = state["stocks"].get(token)
        if not stock:
            return

        stock["ltp"] = float(ltp or 0.0)
        status = stock.get("brk_status", "WAITING")

        # Case A: Monitor Active Trade
        if status == "OPEN":
            await BreakoutEngine.monitor_active_trade(stock, ltp, state)
            return

        # Case B: Execute Pending Trigger
        if status == "TRIGGER_WATCH":
            side = (stock.get("brk_side_latch") or "").lower()
            
            # Validity Check
            if side not in ("bull", "bear"):
                BreakoutEngine._reset_waiting(stock)
                return

            # Expiry Check (6 minutes TTL)
            now_ts = datetime.now(IST).timestamp()
            set_ts = float(stock.get("brk_set_ts") or 0)
            if (now_ts - set_ts) > BreakoutEngine.TRIGGER_VALID_SECONDS:
                logger.info(f"⏳ [BRK] {stock['symbol']} trigger expired.")
                BreakoutEngine._reset_waiting(stock)
                return

            # Engine Toggle & Time Check
            if not state["engine_live"].get(side, True): return
            if not BreakoutEngine._within_trade_window(state["config"].get(side, {})):
                BreakoutEngine._reset_waiting(stock)
                return

            trigger_px = float(stock.get("brk_trigger_px", 0.0))

            # --- EXECUTION WITH DIRECTIONAL LATCH ---
            # Bull latch checks for high breakout, Bear latch checks for low breakdown
            if side == "bull" and ltp > trigger_px:
                await BreakoutEngine.open_trade(stock, ltp, state, "bull")
            elif side == "bear" and ltp < trigger_px:
                await BreakoutEngine.open_trade(stock, ltp, state, "bear")
            
            return

    # --- 2. CANDLE QUALIFICATION (1m Interval) ---
    @staticmethod
    async def on_candle_close(token: int, candle: dict, state: dict):
        """
        Called when a 1-minute candle closes. 
        Implements specific entry filters (Open, Size, Extension).
        """
        stock = state["stocks"].get(token)
        if not stock: return

        if stock.get("brk_status") in ("OPEN", "TRIGGER_WATCH"): return

        symbol = stock.get("symbol")
        pdh = float(stock.get("pdh", 0))
        pdl = float(stock.get("pdl", 0))
        
        c_open = float(candle.get("open", 0))
        c_high = float(candle.get("high", 0))
        c_low = float(candle.get("low", 0))
        c_close = float(candle.get("close", 0))

        if pdh <= 0 or pdl <= 0: return

        # --- 🟢 BULLISH QUALIFICATION ---
        if c_close > pdh:
            side = "bull"
            if not state["engine_live"].get(side, True): return

            # Filter 1: Open must be less than PDH (Under-resistance)
            if c_open >= pdh:
                return

            candle_size_pct = ((c_high - c_low) / c_close) * 100.0
            
            # Filter 2: Size & Extension Check
            if candle_size_pct > 0.5:
                # If candle is large, it shouldn't extend more than 0.5% beyond PDH
                extension_pct = ((c_high - pdh) / pdh) * 100.0
                if extension_pct > 0.5:
                    logger.warning(f"🚫 {symbol} BULL Rejected: Large candle extension {extension_pct:.2f}% > 0.5%")
                    return
            
            # Volume Confirmation (10-Tier Matrix)
            v_ok, v_detail = await BreakoutEngine.check_vol_matrix(stock, candle, side, state)
            if not v_ok: return

            stock.update({
                "brk_status": "TRIGGER_WATCH",
                "brk_side_latch": "bull",
                "brk_trigger_px": float(c_high),
                "brk_trigger_candle": dict(candle),
                "brk_set_ts": datetime.now(IST).timestamp(),
                "brk_scan_reason": f"PDH Break + Size OK ({v_detail})"
            })
            logger.info(f"✅ [BRK-QUALIFY] {symbol} BULL | High: {c_high}")

        # --- 🔴 BEARISH QUALIFICATION ---
        elif c_close < pdl:
            side = "bear"
            if not state["engine_live"].get(side, True): return

            # Filter 1: Open must be greater than PDL (Above-support)
            if c_open <= pdl:
                return

            candle_size_pct = ((c_high - c_low) / c_close) * 100.0

            # Filter 2: Size & Extension Check
            if candle_size_pct > 0.5:
                # If candle is large, it shouldn't extend more than 0.5% below PDL
                extension_pct = ((pdl - c_low) / pdl) * 100.0
                if extension_pct > 0.5:
                    logger.warning(f"🚫 {symbol} BEAR Rejected: Large candle extension {extension_pct:.2f}% > 0.5%")
                    return

            v_ok, v_detail = await BreakoutEngine.check_vol_matrix(stock, candle, side, state)
            if not v_ok: return

            stock.update({
                "brk_status": "TRIGGER_WATCH",
                "brk_side_latch": "bear",
                "brk_trigger_px": float(c_low),
                "brk_trigger_candle": dict(candle),
                "brk_set_ts": datetime.now(IST).timestamp(),
                "brk_scan_reason": f"PDL Break + Size OK ({v_detail})"
            })
            logger.info(f"✅ [BRK-QUALIFY] {symbol} BEAR | Low: {c_low}")

    # --- 3. VOLUME MATRIX LOGIC ---
    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict) -> Tuple[bool, str]:
        """Scans the 10-tier volume multiplier matrix configured in Dashboard."""
        cfg = state["config"].get(side, {})
        matrix = cfg.get("volume_criteria", [])
        if not matrix: return True, "NoMatrix"

        c_vol = int(candle.get("volume", 0))
        s_sma = float(stock.get("sma", 0))
        c_val_cr = (c_vol * candle['close']) / 10000000.0

        tier_found = None
        for i, level in enumerate(matrix):
            if s_sma >= float(level.get("min_sma_avg", 0)):
                tier_found = (i, level)
            else:
                break

        if not tier_found: return False, "SMA_Low"

        idx, level = tier_found
        req_vol = s_sma * float(level.get("sma_multiplier", 1.0))
        min_cr = float(level.get("min_vol_price_cr", 0))

        if c_vol >= req_vol and c_val_cr >= min_cr:
            return True, f"T{idx+1}_OK"
        
        return False, f"T{idx+1}_Fail"

    # --- 4. TRADE EXECUTION (API CALLS) ---
    @staticmethod
    async def open_trade(stock, ltp: float, state: dict, side_key: str):
        """Reserves slot in Redis and places market order via Kite."""
        # Atomic Directional Validation
        if side_key == "bull" and ltp <= stock["brk_trigger_px"]: return
        if side_key == "bear" and ltp >= stock["brk_trigger_px"]: return

        symbol = stock["symbol"]
        kite = state.get("kite")
        cfg = state["config"].get(side_key, {})

        if not kite:
            logger.error(f"❌ [BRK] Kite session missing for {symbol}")
            return

        # Redis Reservation (Atomic Lock)
        ok, reason = await TradeControl.reserve_symbol_trade(symbol, BreakoutEngine.MAX_TRADES_PER_SYMBOL)
        if not ok:
            stock["brk_status"] = "WAITING"
            return

        is_bull = (side_key == "bull")
        trig_candle = stock.get("brk_trigger_candle") or {}
        
        # SL and Quantity Calculation
        sl_px = float(trig_candle.get("low" if is_bull else "high", 0))
        if sl_px <= 0: sl_px = ltp * (0.995 if is_bull else 1.005)

        risk_per_share = max(abs(ltp - sl_px), ltp * 0.003)
        risk_capital = float(cfg.get("risk_trade_1", 2000))
        qty = floor(risk_capital / risk_per_share)

        if qty <= 0:
            await TradeControl.rollback_symbol_trade(symbol)
            stock["brk_status"] = "WAITING"
            return

        try:
            # Strictly Map Transaction Type to Latch Side
            t_type = kite.TRANSACTION_TYPE_BUY if is_bull else kite.TRANSACTION_TYPE_SELL
            
            order_id = await asyncio.to_thread(
                kite.place_order,
                variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                tradingsymbol=symbol, transaction_type=t_type, quantity=int(qty),
                product=kite.PRODUCT_MIS, order_type=kite.ORDER_TYPE_MARKET
            )

            # RR Target Calculation
            rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
            target_px = ltp + (risk_per_share * rr_val) if is_bull else ltp - (risk_per_share * rr_val)

            trade_obj = {
                "engine": "breakout", "symbol": symbol, "side": side_key, "qty": int(qty),
                "entry_price": float(ltp), "sl_price": round(float(sl_px), 2),
                "target_price": round(float(target_px), 2), "status": "OPEN",
                "oid": order_id, "pnl": 0.0, "time": datetime.now(IST).strftime("%H:%M:%S")
            }

            stock["brk_status"] = "OPEN"
            stock["brk_active_trade"] = trade_obj
            state["trades"][side_key].append(trade_obj)
            
            logger.info(f"🔥 [BRK-EXEC] {symbol} {side_key.upper()} @ {ltp} | OID: {order_id}")

        except Exception as e:
            logger.error(f"❌ [BRK-ORDER-FAIL] {symbol}: {e}")
            await TradeControl.rollback_symbol_trade(symbol)
            stock["brk_status"] = "WAITING"

    # --- 5. MONITORING & EXIT ---
    @staticmethod
    async def monitor_active_trade(stock, ltp: float, state: dict):
        """Calculates live PnL and monitors SL/Target breaches."""
        trade = stock.get("brk_active_trade")
        if not trade: return

        is_bull = (trade["side"] == "bull")
        entry, qty = trade["entry_price"], trade["qty"]
        sl, target = trade["sl_price"], trade["target_price"]

        # Live PnL Update (Dashboard compatible)
        trade["pnl"] = round((ltp - entry) * qty if is_bull else (entry - ltp) * qty, 2)

        # Breach Detection
        target_hit = (ltp >= target) if is_bull else (ltp <= target)
        sl_hit = (ltp <= sl) if is_bull else (ltp >= sl)

        if target_hit:
            await BreakoutEngine.close_position(stock, state, "TARGET")
        elif sl_hit:
            await BreakoutEngine.close_position(stock, state, "SL")

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        """Places exit order and releases Redis locks."""
        trade = stock.get("brk_active_trade")
        if not trade or trade["status"] != "OPEN": return
        
        kite, symbol = state.get("kite"), stock["symbol"]
        is_bull = (trade["side"] == "bull")

        try:
            # Exit transaction is opposite of entry
            t_type = kite.TRANSACTION_TYPE_SELL if is_bull else kite.TRANSACTION_TYPE_BUY
            
            exit_id = await asyncio.to_thread(
                kite.place_order,
                variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                tradingsymbol=symbol, transaction_type=t_type, quantity=int(trade["qty"]),
                product=kite.PRODUCT_MIS, order_type=kite.ORDER_TYPE_MARKET
            )
            
            trade["status"] = "CLOSED"
            trade["exit_reason"] = reason
            stock["brk_status"] = "CLOSED"
            
            await TradeControl.release_symbol_lock(symbol)
            logger.info(f"⏹️ [BRK-EXIT] {symbol} | {reason}")

        except Exception as e:
            logger.error(f"❌ [BRK-EXIT-ERROR] {symbol}: {e}")

    # --- 6. UTILS ---
    @staticmethod
    def _reset_waiting(stock: dict):
        stock["brk_status"] = "WAITING"
        stock["brk_active_trade"] = None
        stock.pop("brk_trigger_px", None)
        stock.pop("brk_side_latch", None)
        stock.pop("brk_set_ts", None)

    @staticmethod
    def _within_trade_window(cfg: dict) -> bool:
        try:
            now = datetime.now(IST).time()
            start = datetime.strptime(cfg.get("trade_start", "09:15"), "%H:%M").time()
            end = datetime.strptime(cfg.get("trade_end", "15:10"), "%H:%M").time()
            return start <= now <= end
        except: return True