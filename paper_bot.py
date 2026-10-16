import asyncio
import json
import os
from datetime import datetime
import aiohttp
from aiohttp import web
import ccxt.async_support as ccxt

# ==========================================
# 1. CONFIGURATION & STRICT RISK RULES
# ==========================================
CONFIG = {
    "SYMBOL": "BTC/USDT",
    "TIMEFRAME": "1m",
    "WALLET_BALANCE": 20.00,
    "MARGIN_PER_TRADE": 4.00,
    "FIXED_RISK_AMOUNT": 0.50,
    "DAILY_LOSS_LIMIT": 4.00,
    "TREE_NEWS_WS_URL": "wss://news.treeofalpha.com/ws",
    "NEWS_KEYWORDS": ["CPI", "INFLATION", "FED", "FOMC", "RATE", "POWELL", "SEC", "ETF", "APPROV", "TRUMP", "TARIFF"]
}

state = {
    "balance": CONFIG["WALLET_BALANCE"],
    "daily_pnl": 0.0,
    "trades_taken": 0,
    "wins": 0,
    "losses": 0,
    "active_trade": None,
    "frozen": False,
    "is_news_event_active": False,
    "last_btc_price": 0.0,
    "range_low": 0.0,
    "range_high": 0.0,
    "last_error": "None",
    "trade_history": []
}

# ==========================================
# 2. EXACT $0.50 SL & DYNAMIC LEVERAGE ENGINE
# ==========================================
def calculate_trade_metrics(side: str, entry: float, raw_sl: float, raw_tp: float):
    # Minimum 0.15% SL (~$120 move) taaki exact $0.50 loss aaye
    min_sl_dist = entry * 0.0015
    actual_sl_dist = max(abs(entry - raw_sl), min_sl_dist)

    sl_price = entry - actual_sl_dist if side == "LONG" else entry + actual_sl_dist
    sl_pct = actual_sl_dist / entry

    min_tp_dist = actual_sl_dist * 2.2  # Minimum 1:2.2 R:R (+$1.10 USDT Profit)
    actual_tp_dist = max(abs(raw_tp - entry), min_tp_dist)
    tp_price = entry + actual_tp_dist if side == "LONG" else entry - actual_tp_dist
    tp_pct = actual_tp_dist / entry

    notional_size = CONFIG["FIXED_RISK_AMOUNT"] / sl_pct
    leverage = round(notional_size / CONFIG["MARGIN_PER_TRADE"], 1)

    expected_loss = CONFIG["FIXED_RISK_AMOUNT"]
    expected_profit = round(notional_size * tp_pct, 2)
    rr_ratio = round(expected_profit / expected_loss, 2)

    return {
        "sl_price": round(sl_price, 2),
        "tp_price": round(tp_price, 2),
        "notional": round(notional_size, 2),
        "leverage": leverage,
        "loss": expected_loss,
        "profit": expected_profit,
        "rr": rr_ratio
    }

# ==========================================
# 3. TRADE MONITOR & TRAILING ENGINE
# ==========================================
async def manage_trade(exchange, side, entry, raw_sl, raw_tp, mode):
    state["trades_taken"] += 1
    m = calculate_trade_metrics(side, entry, raw_sl, raw_tp)
    current_sl = m["sl_price"]
    tp = m["tp_price"]
    breakeven_activated = False
    entry_time = datetime.now().strftime('%H:%M:%S')

    state["active_trade"] = {
        "mode": mode, "side": side, "entry": entry,
        "sl": current_sl, "tp": tp, "leverage": m["leverage"],
        "live_pnl": 0.0, "time": entry_time
    }

    print(f"\n🚀 [OPENED {side}] Entry: ${entry:,.2f} | SL: ${current_sl:,.2f} (-$0.50) | TP: ${tp:,.2f} (+${m['profit']}) | Lev: {m['leverage']}x")

    while True:
        await asyncio.sleep(2)
        try:
            ticker = await exchange.fetch_ticker(CONFIG["SYMBOL"])
            curr_price = float(ticker["last"])
            state["last_btc_price"] = curr_price
        except Exception:
            continue

        price_change_pct = (curr_price - entry) / entry if side == "LONG" else (entry - curr_price) / entry
        live_pnl = round(m["notional"] * price_change_pct, 2)
        state["active_trade"]["live_pnl"] = live_pnl
        state["active_trade"]["sl"] = current_sl

        # +$0.55 par Breakeven Lock
        if live_pnl >= 0.55 and not breakeven_activated:
            current_sl = entry
            breakeven_activated = True

        # Check Stop-Loss Hit
        if (side == "LONG" and curr_price <= current_sl) or (side == "SHORT" and curr_price >= current_sl):
            pnl = 0.0 if breakeven_activated else -m["loss"]
            reason = "Breakeven ($0)" if breakeven_activated else "Stop-Loss (-$0.50)"
            if pnl < 0:
                state["losses"] += 1
            break

        # Check Take-Profit Hit
        elif (side == "LONG" and curr_price >= tp) or (side == "SHORT" and curr_price <= tp):
            pnl = m["profit"]
            reason = "Take-Profit Hit"
            state["wins"] += 1
            break

    state["daily_pnl"] = round(state["daily_pnl"] + pnl, 2)
    state["balance"] = round(state["balance"] + pnl, 2)

    state["trade_history"].insert(0, {
        "time": f"{entry_time} -> {datetime.now().strftime('%H:%M:%S')}",
        "mode": mode,
        "side": side,
        "entry": f"${entry:,.1f}",
        "exit": f"${curr_price:,.1f}",
        "lev": f"{m['leverage']}x",
        "reason": reason,
        "pnl": pnl
    })

    print(f"🏁 [CLOSED {side}] Exit: ${curr_price:,.2f} | Reason: {reason} | PnL: ${pnl:.2f} | Wallet: ${state['balance']:.2f}")
    state["active_trade"] = None
    await asyncio.sleep(20)

    if state["daily_pnl"] <= -CONFIG["DAILY_LOSS_LIMIT"]:
        state["frozen"] = True

# ==========================================
# 4. 1-MINUTE SMC SCANNER & NEWS LISTENER
# ==========================================
async def scan_1m_smc(exchange):
    try:
        ohlcv = await exchange.fetch_ohlcv(CONFIG["SYMBOL"], timeframe=CONFIG["TIMEFRAME"], limit=22)
        if not ohlcv or len(ohlcv) < 22:
            return None, None, None, None

        ref_candles = ohlcv[:-2]
        range_high = max(c[2] for c in ref_candles)
        range_low = min(c[3] for c in ref_candles)

        closed_c, live_c = ohlcv[-2], ohlcv[-1]
        recent_high = max(closed_c[2], live_c[2])
        recent_low = min(closed_c[3], live_c[3])
        curr_close = float(live_c[4])

        state["last_btc_price"] = curr_close
        state["range_low"] = range_low
        state["range_high"] = range_high
        state["last_error"] = "Connected & Scanning OK"

        # Bearish Sweep -> SHORT
        if recent_high > range_high and curr_close < range_high:
            return "SHORT", curr_close, recent_high + 15.0, range_low
        # Bullish Sweep -> LONG
        elif recent_low < range_low and curr_close > range_low:
            return "LONG", curr_close, recent_low - 15.0, range_high

    except Exception as err:
        state["last_error"] = str(err)[:60]
        print(f"[Scan Error] {err}")
    return None, None, None, None

async def tree_news_listener(exchange):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.ws_connect(CONFIG["TREE_NEWS_WS_URL"], heartbeat=30) as ws:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            title = data.get("title", "").upper()
                            if any(k in title for k in CONFIG["NEWS_KEYWORDS"]):
                                if state["frozen"] or state["active_trade"]:
                                    continue
                                state["is_news_event_active"] = True
                                p0 = float((await exchange.fetch_ticker(CONFIG["SYMBOL"]))["last"])
                                await asyncio.sleep(2)
                                p1 = float((await exchange.fetch_ticker(CONFIG["SYMBOL"]))["last"])
                                move_pct = (p1 - p0) / p0
                                if move_pct >= 0.0020:
                                    await manage_trade(exchange, "LONG", p1, p1 * 0.9975, p1 * 1.0060, "NEWS SNIPER")
                                elif move_pct <= -0.0020:
                                    await manage_trade(exchange, "SHORT", p1, p1 * 1.0025, p1 * 0.9940, "NEWS SNIPER")
                                state["is_news_event_active"] = False
            except Exception:
                await asyncio.sleep(5)

async def normal_day_scanner(exchange):
    while True:
        await asyncio.sleep(5)
        if state["frozen"] or state["active_trade"] or state["is_news_event_active"]:
            continue
        side, entry, raw_sl, raw_tp = await scan_1m_smc(exchange)
        if side:
            await manage_trade(exchange, side, entry, raw_sl, raw_tp, "1M SMC SWEEP")

# ==========================================
# 5. MOBILE LIVE WEB DASHBOARD (FOR RENDER)
# ==========================================
async def handle_dashboard(request):
    active_html = f"<p style='color:#9ca3af;'>No active trade right now. Feed Status: <b>{state['last_error']}</b></p>"
    if state["active_trade"]:
        t = state["active_trade"]
        color = "#10b981" if t["live_pnl"] >= 0 else "#ef4444"
        active_html = f"""
        <div style='background:#1f2937; padding:12px; border-radius:8px; border-left:4px solid {color}; margin-bottom:15px;'>
            <b>🔥 ACTIVE {t['side']} ({t['mode']})</b><br>
            Entry: ${t['entry']:,.1f} | SL: ${t['sl']:,.1f} (-$0.50) | TP: ${t['tp']:,.1f} | Lev: {t['leverage']}x<br>
            <b>Live PnL: <span style='color:{color};'>${t['live_pnl']:.2f} USDT</span></b>
        </div>
        """

    rows = ""
    for h in state["trade_history"]:
        c = "#10b981" if h["pnl"] > 0 else ("#9ca3af" if h["pnl"] == 0 else "#ef4444")
        rows += f"<tr><td>{h['time']}</td><td>{h['side']}</td><td>{h['entry']}</td><td>{h['exit']}</td><td>{h['lev']}</td><td>{h['reason']}</td><td style='color:{c};font-weight:bold;'>${h['pnl']:.2f}</td></tr>"

    html = f"""
    <html>
    <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta http-equiv="refresh" content="5">
        <title>BTC Paper Bot Live</title>
        <style>
            body {{ font-family: Arial, sans-serif; background: #111827; color: #f3f4f6; padding: 15px; }}
            .card {{ background: #1f2937; padding: 15px; border-radius: 10px; margin-bottom: 15px; }}
            table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
            th, td {{ padding: 8px; border-bottom: 1px solid #374151; text-align: left; }}
        </style>
    </head>
    <body>
        <h2>🚀 BTC 1-Min Quant Paper Bot (Futures)</h2>
        <div class="card">
            <h3>💰 Wallet: ${state['balance']:.2f} USDT | Today PnL: <span style="color:{'#10b981' if state['daily_pnl']>=0 else '#ef4444'}">${state['daily_pnl']:.2f} USDT</span></h3>
            <p>📊 Live BTC: <b>${state['last_btc_price']:,.1f}</b> | Range L: ${state['range_low']:,.1f} | Range H: ${state['range_high']:,.1f}</p>
            <p>🎯 Trades: {state['trades_taken']} (Wins: {state['wins']} | Losses: {state['losses']}) | Margin: $4.00 | Fixed SL: -$0.50</p>
        </div>
        {active_html}
        <h3>📜 Trade History</h3>
        <table>
            <tr><th>Time</th><th>Side</th><th>Entry</th><th>Exit</th><th>Lev</th><th>Reason</th><th>PnL</th></tr>
            {rows if rows else "<tr><td colspan='7'>No trades closed yet.</td></tr>"}
        </table>
    </body>
    </html>
    """
    return web.Response(text=html, content_type="text/html")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_dashboard)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"🌐 Live Dashboard started on port {port}")

# ==========================================
# 6. MAIN RUNNER (US-CLOUD COMPATIBLE)
# ==========================================
async def main():
    # Kraken works 100% on US Render servers without IP blocks!
    exchange = ccxt.kraken({"enableRateLimit": True})
    await start_web_server()
    await asyncio.gather(
        tree_news_listener(exchange),
        normal_day_scanner(exchange)
    )

if __name__ == "__main__":
    asyncio.run(main())
