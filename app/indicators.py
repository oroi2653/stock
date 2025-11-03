import numpy as np
import pandas as pd

def sma(series, window):
    return series.rolling(window, min_periods=window).mean()

def rma(series, period):
    return series.ewm(alpha=1/period, adjust=False).mean()

def bbands(series, window=20, k=2.0):
    mid = series.rolling(window, min_periods=window).mean()
    std = series.rolling(window, min_periods=window).std(ddof=0)
    upper = mid + k * std
    lower = mid - k * std
    return lower, mid, upper

def ichimoku(df, tenkan=9, kijun=26, senkou_b=52, displacement=26):
    high = df["High"]; low = df["Low"]; close = df["Close"]
    conv = (high.rolling(tenkan, min_periods=tenkan).max() + low.rolling(tenkan, min_periods=tenkan).min()) / 2.0
    base = (high.rolling(kijun, min_periods=kijun).max() + low.rolling(kijun, min_periods=kijun).min()) / 2.0
    span_a_now = (conv + base) / 2.0
    span_b_now = (high.rolling(senkou_b, min_periods=senkou_b).max() + low.rolling(senkou_b, min_periods=senkou_b).min()) / 2.0
    span_a_fwd = span_a_now.shift(displacement)
    span_b_fwd = span_b_now.shift(displacement)
    chikou = close.shift(-displacement)
    return pd.DataFrame({
        "tenkan": conv, "kijun": base,
        "span_a_now": span_a_now, "span_b_now": span_b_now,
        "span_a_fwd": span_a_fwd, "span_b_fwd": span_b_fwd,
        "chikou": chikou
    }, index=df.index)

def dmi_dx(df, period=14):
    high, low, close = df["High"], df["Low"], df["Close"]
    up = high.diff(); down = -low.diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    tr_r = rma(tr, period)
    p_r = rma(plus_dm, period)
    m_r = rma(minus_dm, period)
    plus_di = 100 * (p_r / tr_r.replace(0, np.nan))
    minus_di = 100 * (m_r / tr_r.replace(0, np.nan))
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return plus_di, minus_di, dx

def to_weekly(df, week_ending="FRI"):
    rule = f"W-{week_ending.upper()}"
    w = {
        "Open": df["Open"].resample(rule).first(),
        "High": df["High"].resample(rule).max(),
        "Low": df["Low"].resample(rule).min(),
        "Close": df["Close"].resample(rule).last(),
        "Volume": df["Volume"].resample(rule).sum(),
    }
    return pd.DataFrame(w).dropna()
