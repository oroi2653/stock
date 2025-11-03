import io
import pandas as pd
import mplfinance as mpf
from .indicators import ichimoku

def render_chart_png_bytes(df, title):
    fig_bytes = io.BytesIO()
    style = mpf.make_mpf_style(base_mpf_style="yahoo", gridstyle=":")
    mpf.plot(df, type="candle", mav=(20,50,200), volume=True, style=style, title=title,
             savefig=dict(fname=fig_bytes, dpi=160, bbox_inches="tight"))
    fig_bytes.seek(0)
    return fig_bytes

def render_chart_png_bytes_with_ichimoku(df, title, tenkan=9, kijun=26, senkou_b=52, displacement=26, resistance=None):
    ich = ichimoku(df, tenkan, kijun, senkou_b, displacement)
    apds = [
        mpf.make_addplot(ich["tenkan"], color="#ff8c00"),
        mpf.make_addplot(ich["kijun"], color="#1e90ff"),
    ]
    if resistance is not None:
        res_line = pd.Series(resistance, index=df.index)
        apds.append(mpf.make_addplot(res_line, color="#777", linestyle="--", width=1.2))
    fig_bytes = io.BytesIO()
    style = mpf.make_mpf_style(base_mpf_style="yahoo", gridstyle=":")
    mpf.plot(df, type="candle", mav=(20,50,200), volume=True, style=style, title=title,
             addplot=apds,
             fill_between=dict(y1=ich["span_a_fwd"], y2=ich["span_b_fwd"], alpha=0.12, color="#7cb342"),
             savefig=dict(fname=fig_bytes, dpi=160, bbox_inches="tight"))
    fig_bytes.seek(0)
    return fig_bytes
