import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
import yfinance as yf

from .signals import run_signals
from .chart import render_chart_png_bytes, render_chart_png_bytes_with_ichimoku

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "60"))
CHUNK_PAUSE = float(os.getenv("CHUNK_PAUSE", "1.0"))

def fetch_chunk(tickers, start, interval="1d"):
    for attempt in range(3):
        try:
            data = yf.download(
                tickers=tickers,
                start=start,
                interval=interval,
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
            if data is not None and not data.empty:
                return data
        except Exception as e:
            print(f"[fetch_chunk] retry {attempt+1}/3: {e}")
            time.sleep(1.5)
    return pd.DataFrame()

def extract_single_ticker_df(data, ticker):
    try:
        if isinstance(data.columns, pd.MultiIndex):
            if ticker not in data.columns.get_level_values(0):
                return pd.DataFrame()
            sub = data[ticker]
            cols = ["Open","High","Low","Close","Volume"]
            for c in cols:
                if c not in sub.columns:
                    sub[c] = np.nan
            return sub[cols].dropna()
        else:
            cols = ["Open","High","Low","Close","Volume"]
            for c in cols:
                if c not in data.columns:
                    data[c] = np.nan
            return data[cols].dropna()
    except Exception:
        return pd.DataFrame()

def market_cap_fetcher_krx(ticker):
    try:
        tk = yf.Ticker(ticker)
        fi = getattr(tk, "fast_info", None)
        if fi and getattr(fi, "market_cap", None):
            return float(fi.market_cap)
        info = tk.info or {}
        return float(info.get("marketCap")) if "marketCap" in info else None
    except Exception:
        return None

def scan_and_collect(tickers, cfg):
    scan_cfg = (cfg.get("scan") or {}) if isinstance(cfg, dict) else {}
    interval = str(scan_cfg.get("interval", "1d"))
    lookback_days = int(scan_cfg.get("lookback_days", 2200))
    start = datetime.utcnow() - timedelta(days=lookback_days)

    stats = {"total": len(tickers), "ok": 0, "empty": 0, "matched": 0, "errors": 0}
    per_signal = {}

    results = []
    for i in range(0, len(tickers), CHUNK_SIZE):
        batch = tickers[i:i+CHUNK_SIZE]
        data = fetch_chunk(batch, start=start, interval=interval)
        if data is None or data.empty:
            time.sleep(CHUNK_PAUSE)
            continue
        for t in batch:
            df = extract_single_ticker_df(data, t)
            if df is None or df.empty or len(df) < 260:
                stats["empty"] += 1
                continue
            stats["ok"] += 1
            try:
                findings = run_signals(df, cfg, ticker=t, market_cap_fetcher=market_cap_fetcher_krx)
                if findings:
                    stats["matched"] += 1
                    for f in findings:
                        per_signal[f["name"]] = per_signal.get(f["name"], 0) + 1
                    results.append((t, df, findings))
            except Exception as e:
                stats["errors"] += 1
                print(f"[{t}] signal error: {e}")
        time.sleep(CHUNK_PAUSE)
    stats["per_signal"] = per_signal
    return results, stats

def _kst_now_str():
    kst = datetime.now(ZoneInfo("Asia/Seoul"))
    return kst.strftime("%Y-%m-%d %H:%M KST")

def _top_signal_name(stats):
    ps = stats.get("per_signal") or {}
    if not ps:
        return "없음"
    items = sorted(ps.items(), key=lambda x: (-x[1], x[0]))
    return items[0][0]

def format_header_kst(stats, shard_info=""):
    sig_name = _top_signal_name(stats)
    color_emoji = "🔴" if stats.get("matched", 0) > 0 else "⚫️"
    lines = [
        f"🔔 스캔: {_kst_now_str()}",
        f"신호 : {color_emoji} {sig_name}",
    ]
    return "\n".join(lines)

def resolve_display_name(ticker, name_map=None):
    if name_map:
        if ticker in name_map:
            return name_map[ticker]
        code = ticker.split(".")[0]
        if code in name_map:
            return name_map[code]
    try:
        info = yf.Ticker(ticker).info or {}
        nm = info.get("shortName") or info.get("longName")
        if nm:
            return str(nm)
    except Exception:
        pass
    return ticker

def build_caption(ticker, df, findings, name_map=None):
    name = resolve_display_name(ticker, name_map)
    code = ticker.split(".")[0]
    last_open = df["Open"].iloc[-1]
    last_close = df["Close"].iloc[-1]
    lines = [
        f"<b>{name} ({code})</b> | O: {last_open:.2f} C: {last_close:.2f}",
        "발견된 시점의 차트",
    ]
    return "\n".join(lines)

def notify_results(results, stats, cfg, tg_send_msg, tg_send_photo, shard_info="", name_map=None):
    tele_cfg = (cfg.get("telegram") or {}) if isinstance(cfg, dict) else {}
    scan_cfg = (cfg.get("scan") or {}) if isinstance(cfg, dict) else {}
    token = os.getenv("TELEGRAM_BOT_TOKEN", tele_cfg.get("bot_token",""))
    chat_id = os.getenv("TELEGRAM_CHAT_ID", tele_cfg.get("chat_id",""))
    send_chart = bool(scan_cfg.get("send_chart", True))
    max_alerts = int(scan_cfg.get("max_alerts_per_run", 200))

    if not token or not chat_id:
        raise RuntimeError("텔레그램 토큰/chat_id가 설정되어 있지 않습니다.")

    header = format_header_kst(stats, shard_info)
    tg_send_msg(token, chat_id, header)

    sent = 0
    for (ticker, df, findings) in results:
        if sent >= max_alerts: break
        cap = build_caption(ticker, df, findings, name_map=name_map)
        if send_chart:
            s1 = next((f for f in findings if "구름돌파-조정-재돌파" in f["name"]), None)
            if s1 and s1.get("extras",{}).get("resistance") is not None:
                title = f"{resolve_display_name(ticker, name_map)} ({ticker.split('.')[0]}) (1D)"
                img = render_chart_png_bytes_with_ichimoku(df.tail(260), title=title, resistance=s1["extras"]["resistance"])
            else:
                title = f"{resolve_display_name(ticker, name_map)} ({ticker.split('.')[0]}) (1D)"
                img = render_chart_png_bytes(df.tail(220), title=title)
            tg_send_photo(token, chat_id, img, caption=cap)
        else:
            tg_send_msg(token, chat_id, cap)
        sent += 1
        time.sleep(0.5)

    tg_send_msg(token, chat_id, f"완료: 알림 {sent}건")
