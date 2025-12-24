# momentum_engine.py
import asyncio
import logging
from datetime import datetime
from math import floor
from typing import Optional, Tuple
import pytz

from redis_manager import TradeControl

logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")


class MomentumEngine:
    """
    Momentum Engine (mom_bull / mom_bear) — Parallel-safe

    ✅ Parallel + non-blocking ready:
      - Engine only touches mom_* keys (no collisions with Breakout)
      - run() is tick-fast (no candle aggregation here)
      - on_candle_close() is called by main.py candle_close_worker
      - Per-symbol atomic lock + daily cap via Redis (TradeControl helpers)
      - Side-level trade cap (mom_bull/mom_bear) via Redis reserve_side_trade
      - Rollback-safe reservations (if order fails)
      - Direction safety: mom_bull -> BUY, mom_bear -> SELL (hard-mapped)
      - Trade records never removed; marked CLOSED so PnL stays correct
      - Detailed logs for debugging missed ticks / wrong direction

    Required RedisManager methods:
      - reserve_side_trade(side, limit) -> bool
      - rollback_side_trade(side) -> bool
      - reserve_symbol_trade(symbol, max_trades=2, lock_ttl_sec=...) -> (bool, reason)
      - rollback_symbol_trade(symbol) -> bool
      - release_symbol_lock(symbol) -> bool
      - get_symbol_trade_count(symbol) -> int
    """

    EXIT_BUFFER_PCT = 0.0001
    MAX_TRADES_PER_SYMBOL = 2

    # -----------------------------
    # TICK FAST-PATH (called each tick)
    # -----------------------------
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        """
        Tick handler
          - Monitors MOM_OPEN
          - Monitors MOM_TRIGGER_WATCH for entry
          - Does NOT aggregate candles (main.py does that centrally)
        """
        stock = state["stocks"].get(token)
        if not stock:
            return

        symbol = stock.get("symbol") or ""
        if not symbol:
            return

        stock["ltp"] = float(ltp or 0.0)

        mom_status = (stock.get("mom_status") or "WAITING").upper()

        # 1) Always monitor open trade (even if engine toggle off)
        if mom_status == "OPEN":
            await MomentumEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2) Trigger watch -> enter if break happens (gates)
        if mom_status == "TRIGGER_WATCH":
            side = (stock.get("mom_side_latch") or "").lower()
            if side not in ("mom_bull", "mom_bear"):
                logger.warning(f"[MOM] {symbol} invalid mom_side_latch; resetting.")
                MomentumEngine._reset_waiting(stock)
                return

            # Engine toggle gates new entry only
            if not bool(state["engine_live"].get(side, True)):
                return

            # Trade window gate
            if not MomentumEngine._within_trade_window(state["config"].get(side, {})):
                logger.info(f"🕒 [MOM-WINDOW] {symbol} {side.upper()} outside trade window; reset.")
                MomentumEngine._reset_waiting(stock)
                return

            trig = float(stock.get("mom_trigger_px", 0.0) or 0.0)
            if trig <= 0:
                MomentumEngine._reset_waiting(stock)
                return

            if side == "mom_bull":
                if float(ltp) > trig:
                    logger.info(f"⚡ [MOM-TRIGGER] {symbol} MOM_BULL ltp {ltp:.2f} > {trig:.2f}")
                    await MomentumEngine.open_trade(stock, float(ltp), state, "mom_bull")
            else:
                if float(ltp) < trig:
                    logger.info(f"⚡ [MOM-TRIGGER] {symbol} MOM_BEAR ltp {ltp:.2f} < {trig:.2f}")
                    await MomentumEngine.open_trade(stock, float(ltp), state, "mom_bear")
            return

        # WAITING: nothing to do in tick path
        return

    # -----------------------------
    # CANDLE CLOSE QUALIFICATION (called by main.py)
    # -----------------------------
    @staticmethod
    async def on_candle_close(token: int, candle: dict, state: dict):
        """
        Momentum qualification rule:
          - mom_bull: candle close > PDH
          - mom_bear: candle close < PDL
        plus volume matrix filter.
        """
        stock = state["stocks"].get(token)
        if not stock:
            return

        symbol = stock.get("symbol") or ""
        if not symbol:
            return

        # Do not qualify if momentum already busy
        if (stock.get("mom_status") or "WAITING").upper() in ("OPEN", "TRIGGER_WATCH"):
            return

        # Per-symbol cap check (Redis)
        try:
            taken = await TradeControl.get_symbol_trade_count(symbol)
            if int(taken) >= MomentumEngine.MAX_TRADES_PER_SYMBOL:
                return
        except Exception as e:
            logger.warning(f"[MOM] {symbol} trade_count check failed: {e}")

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

        now = datetime.now(IST)

        # MOM BULL
        if close > pdh:
            side = "mom_bull"
            if not bool(state["engine_live"].get(side, True)):
                logger.debug(f"[MOM] {symbol} mom_bull OFF; skip qualify")
                return

            if not MomentumEngine._within_trade_window(state["config"].get(side, {}), now=now):
                return

            ok, detail = await MomentumEngine.check_vol_matrix(stock, candle, side, state)
            if not ok:
                logger.info(f"❌ [MOM-REJECT] {symbol} MOM_BULL | {detail}")
                return

            stock["mom_status"] = "TRIGGER_WATCH"
            stock["mom_side_latch"] = "mom_bull"
            stock["mom_trigger_px"] = float(high)
            stock["mom_trigger_candle"] = dict(candle)

            stock["mom_scan_vol"] = int(c_vol)
            stock["mom_scan_reason"] = f"Momentum bull + Vol OK ({detail})"
            stock["mom_scan_seen_ts"] = None
            stock["mom_scan_seen_time"] = None

            logger.info(f"✅ [MOM-QUALIFIED] {symbol} MOM_BULL trigger@{high:.2f} {detail}")
            return

        # MOM BEAR
        if close < pdl:
            side = "mom_bear"
            if not bool(state["engine_live"].get(side, True)):
                logger.debug(f"[MOM] {symbol} mom_bear OFF; skip qualify")
                return

            if not MomentumEngine._within_trade_window(state["config"].get(side, {}), now=now):
                return

            ok, detail = await MomentumEngine.check_vol_matrix(stock, candle, side, state)
            if not ok:
                logger.info(f"❌ [MOM-REJECT] {symbol} MOM_BEAR | {detail}")
                return

            stock["mom_status"] = "TRIGGER_WATCH"
            stock["mom_side_latch"] = "mom_bear"
            stock["mom_trigger_px"] = float(low)
            stock["mom_trigger_candle"] = dict(candle)

            stock["mom_scan_vol"] = int(c_vol)
            stock["mom_scan_reason"] = f"Momentum bear + Vol OK ({detail})"
            stock["mom_scan_seen_ts"] = None
            stock["mom_scan_seen_time"] = None

            logger.info(f"✅ [MOM-QUALIFIED] {symbol} MOM_BEAR trigger@{low:.2f} {detail}")
            return

    # -----------------------------
    # VOLUME MATRIX (same schema)
    # -----------------------------
    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict) -> Tuple[bool, str]:
        cfg = state["config"].get(side, {}) or {}
        matrix = cfg.get("volume_criteria", []) or []

        c_vol = int(candle.get("volume", 0) or 0)
        s_sma = float(stock.get("sma", 0) or 0)
        close = float(candle.get("close", 0) or 0)
        c_val_cr = (c_vol * close) / 10000000.0 if close > 0 else 0.0

        if not matrix:
            return True, "NoMatrix"

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
            return True, f"Tier{idx+1}Pass"
        return False, f"Tier{idx+1}Fail (Vol/Value)"

    # -----------------------------
    # OPEN TRADE (direction-safe + reservation-safe)
    # -----------------------------
    @staticmethod
    async def open_trade(stock: dict, ltp: float, state: dict, side_key: str):
        symbol = stock.get("symbol") or ""
        if not symbol:
            MomentumEngine._reset_waiting(stock)
            return

        side_key = side_key.lower().strip()
        if side_key not in ("mom_bull", "mom_bear"):
            logger.error(f"[MOM] {symbol} invalid side_key in open_trade: {side_key}")
            MomentumEngine._reset_waiting(stock)
            return

        cfg = state["config"].get(side_key, {}) or {}
        kite = state.get("kite")
        if not kite:
            logger.error(f"❌ [MOM] {symbol} kite session missing")
            MomentumEngine._reset_waiting(stock)
            return

        if not bool(state["engine_live"].get(side_key, True)):
            logger.info(f"[MOM] {symbol} {side_key} engine OFF at entry; reset")
            MomentumEngine._reset_waiting(stock)
            return

        if not MomentumEngine._within_trade_window(cfg):
            logger.info(f"[MOM] {symbol} {side_key} outside window at entry; reset")
            MomentumEngine._reset_waiting(stock)
            return

        # ✅ Direction hard-map (prevents wrong BUY/SELL)
        txn_type = kite.TRANSACTION_TYPE_BUY if side_key == "mom_bull" else kite.TRANSACTION_TYPE_SELL

        # -------------------- reservations (atomic) --------------------
        side_limit = int(cfg.get("total_trades", 5) or 5)

        # 1) reserve side trade (mom_bull/mom_bear) with cap
        if not await TradeControl.reserve_side_trade(side_key, side_limit):
            logger.warning(f"🚫 [MOM-LIMIT] {symbol} side limit hit for {side_key}")
            MomentumEngine._reset_waiting(stock)
            return

        # 2) reserve per-symbol lock + daily count (2/day, 2nd only after close)
        ok, reason = await TradeControl.reserve_symbol_trade(symbol, max_trades=MomentumEngine.MAX_TRADES_PER_SYMBOL)
        if not ok:
            logger.warning(f"🚫 [MOM-SYMBOL] {symbol} reserve failed: {reason}")
            await TradeControl.rollback_side_trade(side_key)
            MomentumEngine._reset_waiting(stock)
            return

        # SL derived from trigger candle opposite extreme
        trig_candle = stock.get("mom_trigger_candle") or {}
        entry = float(ltp)

        if side_key == "mom_bull":
            sl_px = float(trig_candle.get("low", 0) or 0)
        else:
            sl_px = float(trig_candle.get("high", 0) or 0)

        if sl_px <= 0:
            sl_px = round(entry * (0.995 if side_key == "mom_bull" else 1.005), 2)

        # qty sizing
        risk_per_share = max(abs(entry - sl_px), entry * 0.005)
        risk_amount = float(cfg.get("risk_trade_1", 2000) or 2000)
        qty = floor(risk_amount / risk_per_share)

        if qty <= 0:
            logger.warning(f"[MOM] {symbol} qty<=0 (risk calc). rollback reservations.")
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            MomentumEngine._reset_waiting(stock)
            return

        # Targets / trail
        try:
            rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
        except Exception:
            rr_val = 2.0

        if side_key == "mom_bull":
            target = round(entry + (risk_per_share * rr_val), 2)
        else:
            target = round(entry - (risk_per_share * rr_val), 2)

        try:
            tsl_ratio = float(str(cfg.get("trailing_sl", "1:1.5")).split(":")[-1])
        except Exception:
            tsl_ratio = 1.5

        trail_step = float(risk_per_share * tsl_ratio) if tsl_ratio > 0 else float(risk_per_share)

        # -------------------- place order --------------------
        try:
            logger.info(
                f"🧾 [MOM-ORDER] {symbol} {side_key.upper()} "
                f"txn={txn_type} qty={qty} entry={entry:.2f} sl={sl_px:.2f} tgt={target:.2f}"
            )

            order_id = await asyncio.to_thread(
                kite.place_order,
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=symbol,
                transaction_type=txn_type,
                quantity=int(qty),
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET,
            )

            trade = {
                "engine": "momentum",
                "side": side_key,
                "symbol": symbol,
                "qty": int(qty),
                "entry_price": float(entry),
                "sl_price": float(sl_px),
                "target_price": float(target),
                "order_id": order_id,
                "pnl": 0.0,
                "status": "OPEN",
                "entry_time": datetime.now(IST).strftime("%H:%M:%S"),
                "init_risk": float(risk_per_share),
                "trail_step": float(trail_step),
            }

            state["trades"][side_key].append(trade)

            stock["mom_status"] = "OPEN"
            stock["mom_active_trade"] = trade
            stock["mom_side_latch"] = side_key

            # clear trigger so we don't re-enter
            stock.pop("mom_trigger_px", None)

            # scanner fields reset
            stock["mom_scan_seen_ts"] = None
            stock["mom_scan_seen_time"] = None

            logger.info(f"🚀 [MOM-ENTRY] {symbol} {side_key.upper()} order={order_id} qty={qty}")

        except Exception as e:
            logger.error(f"❌ [MOM-ORDER-FAIL] {symbol}: {e}")
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            MomentumEngine._reset_waiting(stock)

    # -----------------------------
    # MONITOR + EXIT
    # -----------------------------
    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        trade = stock.get("mom_active_trade")
        if not trade:
            return

        symbol = stock.get("symbol") or ""
        side_key = (stock.get("mom_side_latch") or "").lower()
        if side_key not in ("mom_bull", "mom_bear"):
            logger.warning(f"[MOM] {symbol} open trade invalid mom_side_latch; forcing close")
            await MomentumEngine.close_position(stock, state, "BAD_SIDE_LATCH")
            return

        is_bull = (side_key == "mom_bull")

        entry = float(trade.get("entry_price", 0) or 0)
        qty = int(trade.get("qty", 0) or 0)
        sl = float(trade.get("sl_price", 0) or 0)
        target = float(trade.get("target_price", 0) or 0)

        if entry <= 0 or qty <= 0:
            await MomentumEngine.close_position(stock, state, "BAD_TRADE_STATE")
            return

        trade["pnl"] = round(((float(ltp) - entry) * qty) if is_bull else ((entry - float(ltp)) * qty), 2)

        b = MomentumEngine.EXIT_BUFFER_PCT
        if is_bull:
            target_hit = float(ltp) >= (target * (1.0 - b))
            sl_hit = float(ltp) <= (sl * (1.0 + b))
        else:
            target_hit = float(ltp) <= (target * (1.0 + b))
            sl_hit = float(ltp) >= (sl * (1.0 - b))

        if target_hit:
            logger.info(f"🎯 [MOM-TARGET] {symbol} tgt={target:.2f} ltp={ltp:.2f} pnl={trade['pnl']}")
            await MomentumEngine.close_position(stock, state, "TARGET")
            return

        if sl_hit:
            logger.info(f"🛑 [MOM-SL] {symbol} sl={sl:.2f} ltp={ltp:.2f} pnl={trade['pnl']}")
            await MomentumEngine.close_position(stock, state, "SL")
            return

        # Step trailing SL
        new_sl = MomentumEngine._step_trail_sl(trade, float(ltp), is_bull)
        if new_sl is not None:
            cur = float(trade.get("sl_price", 0) or 0)
            if is_bull and new_sl > cur:
                trade["sl_price"] = float(new_sl)
                logger.debug(f"[MOM-TRAIL] {symbol} SL -> {new_sl:.2f}")
            elif (not is_bull) and new_sl < cur:
                trade["sl_price"] = float(new_sl)
                logger.debug(f"[MOM-TRAIL] {symbol} SL -> {new_sl:.2f}")

        # Manual exit (legacy)
        if symbol in state.get("manual_exits", set()):
            logger.info(f"🖱️ [MOM-MANUAL] {symbol}")
            await MomentumEngine.close_position(stock, state, "MANUAL")
            state["manual_exits"].remove(symbol)

    @staticmethod
    def _step_trail_sl(trade: dict, ltp: float, is_bull: bool) -> Optional[float]:
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

        desired = entry + ((k - 1) * step) if is_bull else entry - ((k - 1) * step)
        return round(desired, 2)

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        trade = stock.get("mom_active_trade")
        kite = state.get("kite")
        symbol = stock.get("symbol") or ""
        side_key = (stock.get("mom_side_latch") or "").lower()

        is_bull = (side_key == "mom_bull")
        txn_type = None
        if kite:
            txn_type = kite.TRANSACTION_TYPE_SELL if is_bull else kite.TRANSACTION_TYPE_BUY

        if trade and kite and symbol and txn_type:
            try:
                logger.info(f"🏁 [MOM-EXIT] {symbol} reason={reason} qty={trade.get('qty')} txn={txn_type}")
                exit_id = await asyncio.to_thread(
                    kite.place_order,
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=symbol,
                    transaction_type=txn_type,
                    quantity=int(trade["qty"]),
                    product=kite.PRODUCT_MIS,
                    order_type=kite.ORDER_TYPE_MARKET,
                )
                trade["exit_order_id"] = exit_id
            except Exception as e:
                logger.error(f"❌ [MOM-EXIT-FAIL] {symbol}: {e}")

        if trade:
            trade["status"] = "CLOSED"
            trade["exit_time"] = datetime.now(IST).strftime("%H:%M:%S")
            trade["exit_reason"] = reason

        # release open lock
        if symbol:
            try:
                await TradeControl.release_symbol_lock(symbol)
            except Exception as e:
                logger.warning(f"[MOM] {symbol} release lock failed: {e}")

        MomentumEngine._reset_waiting(stock)

    # -----------------------------
    # HELPERS
    # -----------------------------
    @staticmethod
    def _reset_waiting(stock: dict):
        stock["mom_status"] = "WAITING"
        stock["mom_active_trade"] = None

        stock.pop("mom_trigger_px", None)
        stock.pop("mom_side_latch", None)
        stock.pop("mom_trigger_candle", None)

        stock["mom_scan_seen_ts"] = None
        stock["mom_scan_seen_time"] = None
        stock["mom_scan_vol"] = 0
        stock["mom_scan_reason"] = None

    @staticmethod
    def _within_trade_window(cfg: dict, now: Optional[datetime] = None) -> bool:
        """
        cfg keys:
          trade_start: "HH:MM"
          trade_end:   "HH:MM"
        """
        from datetime import time as dtime

        try:
            now = now or datetime.now(IST)
            start_s = str(cfg.get("trade_start", "09:15"))
            end_s = str(cfg.get("trade_end", "09:17"))
            sh, sm = map(int, start_s.split(":"))
            eh, em = map(int, end_s.split(":"))
            start_t = dtime(sh, sm)
            end_t = dtime(eh, em)
            nt = now.time()
            return (nt >= start_t) and (nt <= end_t)
        except Exception:
            return True
