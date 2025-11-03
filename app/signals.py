import numpy as np
from .indicators import ichimoku, bbands, dmi_dx, to_weekly

# 1) 구름돌파-조정-재돌파(저항+BB55+물량: 검색당일 ≥ 조정평균×배수)
def signal_cloud_pullback_rebreak_full(
    df,
    tenkan=9, kijun=26, senkou_b=52, displacement=26,
    retrace="into", min_gap_bars=3, min_retrace_bars=1, require_open_cross=True,
    resistance_mode="swing_high", use_open_for_now=True,
    bb_window=55, bb_k=2.0, retrace_vol_mult=2.0
):
    out = {"name": "구름돌파-조정-재돌파(저항+BB55+물량)", "trigger": False, "detail": ""}

    need = max(senkou_b, kijun) + 80
    if len(df) < need: return out

    ich = ichimoku(df, tenkan, kijun, senkou_b, displacement)
    cloud_top = np.maximum(ich["span_a_now"], ich["span_b_now"])
    cloud_bot = np.minimum(ich["span_a_now"], ich["span_b_now"])
    prev_top = cloud_top.shift(1)

    if require_open_cross:
        breakout_up = (df["Open"] > cloud_top) & (df["Close"].shift(1) <= prev_top)
    else:
        breakout_up = (df["Close"] > cloud_top) & (df["Close"].shift(1) <= prev_top)

    retrace_mask = (df["Close"] < cloud_bot) if retrace == "below" else (df["Close"] <= cloud_top)

    events = np.where(breakout_up.fillna(False).values)[0]
    if len(events) < 2 or events[-1] != len(df) - 1:
        return out

    e2 = events[-1]
    idx1 = retrace_start = None
    resist = np.nan

    for e1 in reversed(events[:-1]):
        if e2 - e1 <= max(1, int(min_gap_bars)): continue
        seg = retrace_mask.iloc[e1+1:e2]
        if seg.sum() >= int(min_retrace_bars):
            first_rel = np.where(seg.values)[0][0]
            retrace_start = e1 + 1 + first_rel
            if resistance_mode == "bar_high":
                resist = float(df["High"].iloc[e1])
            else:
                resist = float(df["High"].iloc[e1:retrace_start].max())
            idx1 = e1
            break
    if idx1 is None: return out

    now_price = float(df["Open"].iloc[-1]) if use_open_for_now else float(df["Close"].iloc[-1])
    prev_price = float(df["Open"].iloc[-2]) if use_open_for_now else float(df["Close"].iloc[-2])

    cond_res_break = now_price > resist and prev_price <= resist

    _, _, bb_up = bbands(df["Close"], window=int(bb_window), k=float(bb_k))
    prev_bb_up = bb_up.iloc[-2]
    cond_bb = (now_price > bb_up.iloc[-1]) and (prev_price <= (prev_bb_up if not np.isnan(prev_bb_up) else np.inf))

    now_vol = float(df["Volume"].iloc[-1])
    retrace_avg = float(df["Volume"].iloc[retrace_start:e2].mean()) if (e2 - retrace_start) >= 1 else 0.0
    cond_vol = (retrace_avg > 0) and (now_vol >= float(retrace_vol_mult) * retrace_avg)

    if cond_res_break and cond_bb and cond_vol:
        out["trigger"] = True
        d1 = df.index[idx1].strftime("%Y-%m-%d"); d2 = df.index[e2].strftime("%Y-%m-%d")
        out["detail"] = (f"{d2} 2차 구름 상향돌파. 저항 {resist:.2f} 돌파 + BB55 상단(시가) 돌파. "
                         f"당일거래량 {now_vol:,.0f} ≥ {retrace_vol_mult:.1f}× 조정평균 {retrace_avg:,.0f}.")
        out["extras"] = {"resistance": resist}
    return out

# 2) 전화선 골든크로스 콤보
def signal_ichimoku_tenkan_golden_combo(
    df,
    tenkan=9, kijun=26, senkou_b=52, displacement=26,
    lookback_cross_bars=1, dx_period=14, ma_fast=5, ma_mid=10
):
    out = {"name": "전화선 골크 콤보", "trigger": False, "detail": ""}

    ich = ichimoku(df, tenkan, kijun, senkou_b, displacement)
    if ich["tenkan"].isna().iloc[-1] or ich["kijun"].isna().iloc[-1]:
        return out

    tenkan_above = ich["tenkan"].iloc[-1] > ich["kijun"].iloc[-1]
    tenkan_xup_now = (ich["tenkan"].iloc[-1] > ich["kijun"].iloc[-1]) and (ich["tenkan"].iloc[-2] <= ich["kijun"].iloc[-2])
    tenkan_xup_prev = (ich["tenkan"].iloc[-2] > ich["kijun"].iloc[-2]) and (ich["tenkan"].iloc[-3] <= ich["kijun"].iloc[-3]) if len(df) >= 3 else False
    cond_A = tenkan_above and (tenkan_xup_now or (lookback_cross_bars >= 1 and tenkan_xup_prev))

    i0 = len(df) - int(displacement) - 1
    cond_B = False
    if i0 >= 2 and not np.isnan(ich["chikou"].iloc[i0]) and not np.isnan(ich["tenkan"].iloc[i0]):
        above_now = ich["chikou"].iloc[i0] > ich["tenkan"].iloc[i0]
        xup_now = (ich["chikou"].iloc[i0] > ich["tenkan"].iloc[i0]) and (ich["chikou"].iloc[i0-1] <= ich["tenkan"].iloc[i0-1])
        xup_prev = (ich["chikou"].iloc[i0-1] > ich["tenkan"].iloc[i0-1]) and (ich["chikou"].iloc[i0-2] <= ich["tenkan"].iloc[i0-2])
        cond_B = above_now and (xup_now or (lookback_cross_bars >= 1 and xup_prev))

    _, _, dx = dmi_dx(df, period=int(dx_period))
    cond_C = (len(dx) >= 2) and (dx.iloc[-1] > dx.iloc[-2])

    ma5 = df["Close"].rolling(int(ma_fast)).mean()
    ma10 = df["Close"].rolling(int(ma_mid)).mean()
    cond_D = df["Close"].iloc[-1] > ma5.iloc[-1]
    cond_E = ma5.iloc[-1] > ma10.iloc[-1]

    cond_F = ich["kijun"].iloc[-1] > ich["kijun"].iloc[-2]
    cond_G = df["Close"].iloc[-1] > ich["kijun"].iloc[-1]

    if all([cond_A, cond_B, cond_C, cond_D, cond_E, cond_F, cond_G]):
        out["trigger"] = True
        out["detail"] = "전환선>기준선 골크(1봉내) 지속, 후행스팬>전환선 골크(1봉내) 지속, DX↑, 종>MA5>MA10, 기준선↑ & 종>기준선"
    return out

# 3) 대세상승 후 하락구간 반등
def signal_major_uptrend_pullback_bounce(
    df,
    min_market_cap_krw=100_000_000_000,
    min_close=1000, max_close=99_999_999,
    min_daily_volume=100_000, max_daily_volume=999_999_999,
    weekly_lookback=135, nhigh_weeks=299, week_ending="FRI",
    market_cap_fetcher=None, ticker=None
):
    out = {"name": "대세상승 후 하락구간 반등", "trigger": False, "detail": ""}

    mc = market_cap_fetcher(ticker) if market_cap_fetcher and ticker else None
    if mc is None or mc < float(min_market_cap_krw): return out

    close = df["Close"].iloc[-1]; vol = df["Volume"].iloc[-1]
    if not (float(min_close) <= close <= float(max_close)): return out
    if not (int(min_daily_volume) <= vol <= int(max_daily_volume)): return out
    if not (df["Close"].iloc[-1] > df["Open"].iloc[-1]): return out

    w = to_weekly(df, week_ending=str(week_ending))
    if len(w) < max(int(weekly_lookback), int(nhigh_weeks)) + 5: return out
    hi_299 = w["High"].rolling(int(nhigh_weeks), min_periods=int(nhigh_weeks)).max().iloc[-1]
    if not (w["High"].iloc[-1] >= hi_299 - 1e-8): return out

    lo_ll = w["Low"].rolling(int(weekly_lookback)).min()
    hi_ll = w["High"].rolling(int(weekly_lookback)).max()
    rng = (hi_ll - lo_ll).replace(0, np.nan)
    pos0 = (w["Close"].iloc[-1] - lo_ll.iloc[-1]) / rng.iloc[-1]
    pos1 = (w["Close"].iloc[-2] - lo_ll.iloc[-2]) / rng.iloc[-2]
    cond_F = pos0 <= 0.25
    cond_I = pos1 <= 0.25
    cond_J = pos0 >= 0.25

    if (cond_F or cond_I) and cond_J:
        out["trigger"] = True
        out["detail"] = (f"주봉 299 신고가 + (전주≤25% 또는 금주≤25%) → 금주≥25% 반등, 양봉. 시총 {mc:,.0f} KRW")
    return out

# 4) 폭락후 이평선반등
def signal_crash_ma_rebound(
    df,
    min_dod_close_change=0.01,
    max_open_to_low_drawdown=-0.03,
    min_low_to_close_rebound=0.02,
    near_ma_tolerance=0.005,
    ma_set=(5,20,60)
):
    out = {"name": "폭락후 이평선반등", "trigger": False, "detail": ""}

    if len(df) < max(ma_set) + 5: return out

    close = df["Close"]; open_ = df["Open"]; low = df["Low"]
    cond_A = (close.iloc[-1] / close.iloc[-2] - 1.0) >= float(min_dod_close_change)
    cond_B = ((low.iloc[-1] / open_.iloc[-1]) - 1.0) <= float(max_open_to_low_drawdown)
    cond_C = ((close.iloc[-1] / low.iloc[-1]) - 1.0) >= float(min_low_to_close_rebound)

    tol = float(near_ma_tolerance)
    ma_vals = {p: close.rolling(int(p)).mean().iloc[-1] for p in ma_set}
    near_any = any(abs(close.iloc[-1] - ma_vals[p]) / ma_vals[p] <= tol for p in ma_set if not np.isnan(ma_vals[p]))

    if cond_A and cond_B and cond_C and near_any:
        out["trigger"] = True
        out["detail"] = (f"종가 +{(close.iloc[-1]/close.iloc[-2]-1)*100:.2f}% | 장중 -{(1 - low.iloc[-1]/open_.iloc[-1])*100:.2f}% → "
                         f"+{(close.iloc[-1]/low.iloc[-1]-1)*100:.2f}% | MA 근접±{tol*100:.1f}% ({', '.join(map(str,ma_set))})")
    return out

# 0) 점검용 간단 신호(원하면 켜서 테스트)
def signal_sanity_ma5_gt_ma10(df):
    out = {"name": "SANITY MA5>MA10", "trigger": False, "detail": ""}
    if len(df) < 10: return out
    ma5 = df["Close"].rolling(5).mean().iloc[-1]
    ma10 = df["Close"].rolling(10).mean().iloc[-1]
    if not np.isnan(ma5) and not np.isnan(ma10) and ma5 > ma10:
        out["trigger"] = True
        out["detail"] = "파이프라인 점검용"
    return out

def run_signals(df, cfg, ticker=None, market_cap_fetcher=None):
    findings = []
    s_cfg = cfg.get("signals", {})

    if s_cfg.get("cloud_pullback_rebreak_full", {}).get("enabled"):
        c = s_cfg["cloud_pullback_rebreak_full"]
        res = signal_cloud_pullback_rebreak_full(
            df,
            tenkan=int(c.get("tenkan",9)), kijun=int(c.get("kijun",26)),
            senkou_b=int(c.get("senkou_b",52)), displacement=int(c.get("displacement",26)),
            retrace=str(c.get("retrace","into")).lower(),
            min_gap_bars=int(c.get("min_gap_bars",3)),
            min_retrace_bars=int(c.get("min_retrace_bars",1)),
            require_open_cross=bool(c.get("require_open_cross",True)),
            resistance_mode=str(c.get("resistance_mode","swing_high")).lower(),
            use_open_for_now=bool(c.get("use_open_for_now",True)),
            bb_window=int(c.get("bb_window",55)),
            bb_k=float(c.get("bb_k",2.0)),
            retrace_vol_mult=float(c.get("retrace_vol_mult",2.0))
        )
        if res["trigger"]: findings.append(res)

    if s_cfg.get("ichimoku_tenkan_golden_combo", {}).get("enabled"):
        c = s_cfg["ichimoku_tenkan_golden_combo"]
        res = signal_ichimoku_tenkan_golden_combo(
            df,
            tenkan=int(c.get("tenkan",9)), kijun=int(c.get("kijun",26)),
            senkou_b=int(c.get("senkou_b",52)), displacement=int(c.get("displacement",26)),
            lookback_cross_bars=int(c.get("lookback_cross_bars",1)),
            dx_period=int(c.get("dx_period",14)),
            ma_fast=int(c.get("ma_fast",5)),
            ma_mid=int(c.get("ma_mid",10))
        )
        if res["trigger"]: findings.append(res)

    if s_cfg.get("major_uptrend_pullback_bounce", {}).get("enabled"):
        c = s_cfg["major_uptrend_pullback_bounce"]
        res = signal_major_uptrend_pullback_bounce(
            df,
            min_market_cap_krw=float(c.get("min_market_cap_krw", 100_000_000_000)),
            min_close=float(c.get("min_close",1000)),
            max_close=float(c.get("max_close",99_999_999)),
            min_daily_volume=int(c.get("min_daily_volume",100_000)),
            max_daily_volume=int(c.get("max_daily_volume",999_999_999)),
            weekly_lookback=int(c.get("weekly_lookback",135)),
            nhigh_weeks=int(c.get("nhigh_weeks",299)),
            week_ending=str(c.get("week_ending","FRI")),
            market_cap_fetcher=market_cap_fetcher,
            ticker=ticker
        )
        if res["trigger"]: findings.append(res)

    if s_cfg.get("crash_ma_rebound", {}).get("enabled"):
        c = s_cfg["crash_ma_rebound"]
        res = signal_crash_ma_rebound(
            df,
            min_dod_close_change=float(c.get("min_dod_close_change",0.01)),
            max_open_to_low_drawdown=float(c.get("max_open_to_low_drawdown",-0.03)),
            min_low_to_close_rebound=float(c.get("min_low_to_close_rebound",0.02)),
            near_ma_tolerance=float(c.get("near_ma_tolerance",0.005)),
            ma_set=tuple(c.get("ma_set",[5,20,60])),
        )
        if res["trigger"]: findings.append(res)

    if s_cfg.get("sanity_ma5_gt_ma10", {}).get("enabled"):
        s = signal_sanity_ma5_gt_ma10(df)
        if s["trigger"]:
            findings.append(s)

    return findings
