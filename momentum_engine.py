import logging
import asyncio
from datetime import datetime
from math import floor
import pytz
from redis_manager import TradeControl

# --- LOGGING SETUP ---
logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")

class MomentumEngine:
    
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        """
        The main asynchronous entry point for tick processing.
        Handles Candle formation and Momentum detection.
        """
        stock = state["stocks"].get(token)
        if not stock:
            return

        # 1. MONITOR ACTIVE MOMENTUM TRADES
        if stock['status'] == 'MOM_OPEN':
            await MomentumEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2. TRIGGER WATCH (Price confirmation for momentum)
        if stock['status'] == 'MOM_TRIGGER_WATCH':
            # Bullish Momentum Trigger
            if stock['side_latch'] == 'MOM_BULL' and ltp >= stock['trigger_px']:
                await MomentumEngine.open_trade(token, stock, ltp, state, 'mom_bull')
            # Bearish Momentum Trigger
            elif stock['side_latch'] == 'MOM_BEAR' and ltp <= stock['trigger_px']:
                await MomentumEngine.open_trade(token, stock, ltp, state, 'mom_bear')
            return

        # 3. ASYNC CANDLE FORMATION
        now = datetime.now(IST)
        bucket = now.replace(second=0, microsecond=0)

        if stock['candle'] and stock['candle']['bucket'] != bucket:
            # Candle Closed -> Analyze Momentum logic in background
            asyncio.create_task(MomentumEngine.analyze_momentum_logic(token, stock['candle'], state))
            stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
        elif not stock['candle']:
            stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
        else:
            c = stock['candle']
            c['high'] = max(c['high'], ltp)
            c['low'] = min(c['low'], ltp)
            c['close'] = ltp
            if stock['last_vol'] > 0:
                c['volume'] += max(0, vol - stock['last_vol'])
        
        stock['last_vol'] = vol

    @staticmethod
    async def analyze_momentum_logic(token: int, candle: dict, state: dict):
        """
        Checks for price velocity and volume surges.
        """
        stock = state["stocks"][token]
        body_size = abs(candle['close'] - candle['open'])
        body_pct = (body_size / candle['open']) * 100 if candle['open'] > 0 else 0

        # --- BULLISH MOMENTUM ---
        if state["engine_live"].get("mom_bull") and candle['close'] > candle['open']:
            # Requirement: Strong green candle (e.g. > 0.3% body)
            if body_pct > 0.25:
                is_qualified, detail = await MomentumEngine.check_vol_matrix(stock, candle, 'mom_bull', state)
                if is_qualified:
                    logger.info(f"⚡ [MOM-BULL] {stock['symbol']} Surge | Body: {body_pct:.2f}% | {detail}")
                    stock['status'] = 'MOM_TRIGGER_WATCH'
                    stock['side_latch'] = 'MOM_BULL'
                    stock['trigger_px'] = round(candle['high'] + (body_size * 0.1), 2)

        # --- BEARISH MOMENTUM ---
        elif state["engine_live"].get("mom_bear") and candle['close'] < candle['open']:
            if body_pct > 0.25:
                is_qualified, detail = await MomentumEngine.check_vol_matrix(stock, candle, 'mom_bear', state)
                if is_qualified:
                    logger.info(f"🔻 [MOM-BEAR] {stock['symbol']} Crash | Body: {body_pct:.2f}% | {detail}")
                    stock['status'] = 'MOM_TRIGGER_WATCH'
                    stock['side_latch'] = 'MOM_BEAR'
                    stock['trigger_px'] = round(candle['low'] - (body_size * 0.1), 2)

    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
        """Asynchronous Volume validation using SMA tiers."""
        matrix = state["config"][side].get('volume_criteria', [])
        c_vol = candle['volume']
        s_sma = stock.get('sma', 0)
        c_val_cr = (c_vol * candle['close']) / 10000000.0

        if not matrix: return True, "No Vol Matrix"

        qualified_tier = None
        for i, level in enumerate(matrix):
            min_sma = float(level.get('min_sma_avg', 0))
            if s_sma >= min_sma: qualified_tier = (i, level)
            else: break

        if qualified_tier:
            idx, level = qualified_tier
            mult, min_cr = float(level.get('sma_multiplier', 1.0)), float(level.get('min_vol_price_cr', 0))
            target_vol = s_sma * mult
            if c_vol >= target_vol and c_val_cr >= min_cr:
                return True, f"L{idx+1} OK: Vol {c_vol:,.0f} > {target_vol:,.0f}"
        
        return False, "Vol matrix fail"

    @staticmethod
    async def open_trade(token: int, stock: dict, ltp: float, state: dict, side_key: str):
        """Executes trade entry with Redis atomic limit checks."""
        cfg = state["config"][side_key]
        
        if not await TradeControl.can_trade(side_key, int(cfg.get('total_trades', 5))):
            logger.warning(f"🚫 [LIMIT] {side_key.upper()} Limit Reached for {stock['symbol']}")
            stock['status'] = 'WAITING'
            return

        # Risk Management
        candle = stock['candle']
        risk = abs(ltp - candle['low']) if 'bull' in side_key else abs(ltp - candle['high'])
        if risk < (ltp * 0.002): risk = ltp * 0.005 # Minimum risk floor

        # Position Sizing
        total_risk = float(cfg.get('risk_trade_1', 2000))
        qty = floor(total_risk / risk)
        
        if qty <= 0:
            stock['status'] = 'WAITING'
            return

        # Target (R:R)
        rr_val = float(cfg.get('risk_reward', "1:2").split(':')[-1])
        target_px = round(ltp + (risk * rr_val), 2) if 'bull' in side_key else round(ltp - (risk * rr_val), 2)
        sl_px = round(ltp - risk, 2) if 'bull' in side_key else round(ltp + risk, 2)

        trade = {
            "symbol": stock['symbol'],
            "qty": qty, "entry_price": ltp, "sl_price": sl_px,
            "target_price": target_px, "pnl": 0.0,
            "entry_time": datetime.now(IST).strftime("%H:%M:%S")
        }
        
        state["trades"][side_key].append(trade)
        stock['status'] = 'MOM_OPEN'
        stock['active_trade'] = trade
        
        logger.info(f"🔥 [MOM-ENTRY] {stock['symbol']} {side_key.upper()} @ {ltp} | Qty: {qty} | Tgt: {target_px}")

    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        """Real-time trade management (Target, SL, TSL)."""
        trade = stock.get('active_trade')
        if not trade: return
        
        side_key = stock['side_latch'].lower()
        cfg = state["config"][side_key]
        is_bull = 'bull' in side_key

        # Live PnL
        trade['pnl'] = (ltp - trade['entry_price']) * trade['qty'] if is_bull else (trade['entry_price'] - ltp) * trade['qty']

        # 1. CHECK TARGET
        if (is_bull and ltp >= trade['target_price']) or (not is_bull and ltp <= trade['target_price']):
            logger.info(f"🎯 [MOM-TARGET] {stock['symbol']} closed @ {ltp}")
            await MomentumEngine.close_position(stock, state, "TARGET")

        # 2. CHECK SL
        elif (is_bull and ltp <= trade['sl_price']) or (not is_bull and ltp >= trade['sl_price']):
            logger.info(f"🛑 [MOM-SL] {stock['symbol']} stopped @ {ltp}")
            await MomentumEngine.close_position(stock, state, "SL")

        # 3. TRAILING STOP LOSS
        else:
            tsl_ratio = float(cfg.get('trailing_sl', "1:1.5").split(':')[-1])
            new_sl = await MomentumEngine.calculate_tsl(trade, ltp, tsl_ratio, is_bull)
            if is_bull and new_sl > trade['sl_price']: trade['sl_price'] = new_sl
            elif not is_bull and new_sl < trade['sl_price']: trade['sl_price'] = new_sl

        # 4. MANUAL EXIT
        if stock['symbol'] in state['manual_exits']:
            await MomentumEngine.close_position(stock, state, "MANUAL")
            state['manual_exits'].remove(stock['symbol'])

    @staticmethod
    async def calculate_tsl(trade: dict, ltp: float, ratio: float, is_bull: bool):
        entry, sl = trade['entry_price'], trade['sl_price']
        risk = abs(entry - sl)
        profit = (ltp - entry) if is_bull else (entry - ltp)
        if profit > (risk * ratio):
            return round(ltp - risk, 2) if is_bull else round(ltp + risk, 2)
        return sl

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        stock['status'] = 'WAITING'
        stock['active_trade'] = None
        logger.info(f"🏁 [MOM-CLOSED] {stock['symbol']} | Reason: {reason}")