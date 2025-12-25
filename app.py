import time, random
import pandas as pd
import yfinance as yf
import streamlit as st

# 1) 全域節流：跨 rerun 生效
def throttle(min_interval=2.0):
    if "last_yahoo_ts" not in st.session_state:
        st.session_state.last_yahoo_ts = 0.0
    now = time.monotonic()
    wait = st.session_state.last_yahoo_ts + min_interval - now
    if wait > 0:
        time.sleep(wait)
    st.session_state.last_yahoo_ts = time.monotonic()

# 2) 快取：同一檔短時間不要重抓
@st.cache_data(ttl=3600, show_spinner=False)  # 1 小時
def fetch_yahoo(symbol: str) -> pd.DataFrame:
    t = yf.Ticker(symbol)

    for i in range(6):  # 退避重試
        throttle(2.0)
        df = t.history(period="3mo", interval="1d", auto_adjust=False)

        if not df.empty:
            if getattr(df.index, "tz", None):
                df.index = df.index.tz_localize(None)
            return df

        time.sleep(min(60, (2 ** i) + random.random()))

    return pd.DataFrame()
