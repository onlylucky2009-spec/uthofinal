import logging
import asyncio
from datetime import datetime
from math import floor
import pytz
from redis_manager import TradeControl

# --- LOGGING SETUP ---
logger = logging.getLogger("Nexus_Breakout")
IST = pytz.timezone("Asia/Kolkata")

class BreakoutEngine:
    
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        """
        The main asynchronous entry point for tick processing.
        Called for every price update.
        """
        stock = state["stocks"].get(token)
        if not stock:
            return

        # 1. MONITOR ACTIVE TRADES (PnL & Exits)
        if stock['status'] == 'OPEN':
            await BreakoutEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2. TRIGGER WATCH (Check if price hits the high of the breakout candle)
        if stock['status'] == 'TRIGGER_WATCH':
            if stock['side_latch'] == 'BULL' and ltp >= stock['trigger_px']:
                # Open trade is an async operation due to Redis limit checks
                await BreakoutEngine.open_trade(token, stock, ltp, state)
            return

        # 3. 1-MINUTE ASYNC CANDLE FORMATION
        now = datetime.now(IST)
        bucket = now.replace(second=0, microsecond=0)

        if stock['candle'] and stock['candle']['bucket'] != bucket:
            # Current minute finished. Process the candle in a background task
            # so the main tick loop stays 'superfast'.
            asyncio.create_task(BreakoutEngine.analyze_candle_logic(token, stock['candle'], state))
            
            # Reset for new minute
            stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
        elif not stock['candle']:
            stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
        else:
            # Update OHLC
            c = stock['candle']
            c['high'] = max(c['high'], ltp)
            c['low'] = min(c['low'], ltp)
            c['close'] = ltp
            # Calculate Volume Delta
            if stock['last_vol'] > 0:
                c['volume'] += max(0, vol - stock['last_vol'])
        
        stock['last_vol'] = vol

    @staticmethod
    async def analyze_candle_logic(token: int, candle: dict, state: dict):
        """
        Processes the closed 1-minute candle. 
        Checks against PDH and Volume Matrix.
        """
        stock = state["stocks"][token]
        pdh = stock.get('pdh', 0)
        
        # Guard: Check if PDH exists and if Bull Engine is toggled ON
        if pdh <= 0 or not state["engine_live"].get("bull"):
            return

        # LOGIC: Current Candle crossed PDH from below
        if candle['open'] < pdh and candle['close'] > pdh:
            logger.info(f"🔍 [SCAN] {stock['symbol']} Potential Breakout! Close: {candle['close']} > PDH: {pdh}")
            
            # Validate Volume against the 10-level SMA Matrix
            is_qualified, detail = await BreakoutEngine.check_vol_matrix(stock, candle, 'bull', state)
            
            if is_qualified:
                logger.info(f"✅ [QUALIFIED] {stock['symbol']} | {detail}")
                stock['status'] = 'TRIGGER_WATCH'
                stock['side_latch'] = 'BULL'
                # Set Trigger at High of Breakout Candle + Buffer
                stock['trigger_px'] = round(candle['high'] * 1.0005, 2)
            else:
                logger.info(f"❌ [REJECTED] {stock['symbol']} | {detail}")

    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
        """
        Asynchronously checks if the candle volume matches the SMA Tier settings.
        """
        matrix = state["config"][side].get('volume_criteria', [])
        c_vol = candle['volume']
        s_sma = stock.get('sma', 0) # Historical 20-Day SMA
        c_val_cr = (c_vol * candle['close']) / 10000000.0 # Turnover in Crores

        if not matrix:
            return True, "No Volume Matrix defined (Auto-Pass)"

        # Search for the applicable Tier in the 10-level matrix
        tier_found = None
        for i, level in enumerate(matrix):
            min_sma = float(level.get('min_sma_avg', 0))
            if s_sma >= min_sma:
                tier_found = (i, level)
            else:
                break # Matrix is sorted by Min SMA

        if tier_found:
            idx, level = tier_found
            mult = float(level.get('sma_multiplier', 1.0))
            min_cr = float(level.get('min_vol_price_cr', 0))
            target_vol = s_sma * mult
            
            if c_vol >= target_vol and c_val_cr >= min_cr:
                return True, f"L{idx+1} Match: Vol {c_vol:,.0f} > {target_vol:,.0f} (SMA {s_sma:,.0f} * {mult}) | Value {c_val_cr:.2f}Cr >= {min_cr}Cr"
            else:
                return False, f"L{idx+1} Fail: Vol {c_vol:,.0f}/{target_vol:,.0f} or Value {c_val_cr:.2f}Cr/{min_cr}Cr"
        
        return False, f"Stock SMA {s_sma:,.0f} too low for Matrix Tiers"

    @staticmethod
    async def open_trade(token: int, stock: dict, ltp: float, state: dict):
        """
        Executes the trade entry. Checks Redis limits first.
        """
        side = stock['side_latch'].lower()
        cfg = state["config"][side]
        
        # ASYNC CHECK: Daily trade limit in Redis
        if not await TradeControl.can_trade(side, int(cfg.get('total_trades', 5))):
            logger.warning(f"🚫 [LIMIT] {side.upper()} Engine daily limit reached. Skipping {stock['symbol']}.")
            stock['status'] = 'WAITING'
            return

        # Calculate Stop Loss (Low of breakout candle)
        sl_px = stock['candle']['low']
        risk_per_share = abs(ltp - sl_px)
        if risk_per_share < (ltp * 0.001): risk_per_share = ltp * 0.005 # Safety floor

        # Position Sizing
        total_risk = float(cfg.get('risk_trade_1', 2000))
        qty = floor(total_risk / risk_per_share)
        
        if qty <= 0:
            logger.error(f"⚠️ [SIZE] Qty 0 for {stock['symbol']}. Risk too high?")
            stock['status'] = 'WAITING'
            return

        # Target Calculation (R:R)
        rr_val = float(cfg.get('risk_reward', "1:2").split(':')[-1])
        target_px = round(ltp + (risk_per_share * rr_val), 2)

        # Update State
        trade = {
            "symbol": stock['symbol'],
            "qty": qty,
            "entry_price": ltp,
            "sl_price": sl_px,
            "target_price": target_px,
            "pnl": 0.0,
            "entry_time": datetime.now(IST).strftime("%H:%M:%S")
        }
        
        state["trades"][side].append(trade)
        stock['status'] = 'OPEN'
        stock['active_trade'] = trade
        
        logger.info(f"🚀 [ENTRY] {stock['symbol']} @ {ltp} | Qty: {qty} | SL: {sl_px} | Tgt: {target_px}")

    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        """
        Monitors every tick for an open position.
        Handles Target, SL, and Trailing SL.
        """
        trade = stock.get('active_trade')
        if not trade: return
        
        side = stock['side_latch'].lower()
        cfg = state["config"][side]

        # Update Unrealized PnL
        trade['pnl'] = (ltp - trade['entry_price']) * trade['qty']

        # 1. CHECK TARGET
        if ltp >= trade['target_price']:
            logger.info(f"🎯 [TARGET] {stock['symbol']} Exit @ {ltp}. PnL: +₹{trade['pnl']:.2f}")
            await BreakoutEngine.close_position(stock, state, "TARGET")

        # 2. CHECK STOP LOSS
        elif ltp <= trade['sl_price']:
            logger.info(f"🛑 [STOPLOSS] {stock['symbol']} Exit @ {ltp}. PnL: ₹{trade['pnl']:.2f}")
            await BreakoutEngine.close_position(stock, state, "SL")

        # 3. TRAILING STOP LOSS (TSL)
        else:
            tsl_ratio = float(cfg.get('trailing_sl', "1:1.5").split(':')[-1])
            new_sl = await BreakoutEngine.calculate_tsl(trade, ltp, tsl_ratio)
            if new_sl > trade['sl_price']:
                trade['sl_price'] = new_sl

        # 4. MANUAL EXIT FROM DASHBOARD
        if stock['symbol'] in state['manual_exits']:
            logger.info(f"🖱️ [MANUAL EXIT] Closing {stock['symbol']} as per user command.")
            await BreakoutEngine.close_position(stock, state, "MANUAL")
            state['manual_exits'].remove(stock['symbol'])

    @staticmethod
    async def calculate_tsl(trade: dict, ltp: float, ratio: float):
        """Moves Stop Loss up as the trade moves into profit."""
        entry = trade['entry_price']
        risk = entry - trade['sl_price']
        profit = ltp - entry
        
        # If profit exceeds 'Ratio' times the Risk, trail the SL
        if profit > (risk * ratio):
            # Trail by keeping original risk distance from current price
            return round(ltp - risk, 2)
        return trade['sl_price']

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        """Final cleanup for a trade."""
        # Reset stock status
        stock['status'] = 'WAITING'
        stock['active_trade'] = None
        logger.info(f"🏁 [CLOSED] {stock['symbol']} | Reason: {reason}")