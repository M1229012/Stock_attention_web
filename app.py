# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import requests
import yfinance as yf
from curl_cffi import requests as curl_requests
import urllib3
import time
import socket
from datetime import datetime

# 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="終極連線診斷", layout="wide")
st.title("💉 網路連線終極診斷 (Yahoo / TWSE / TPEx)")

st.write("此程式將測試 Zeabur 主機對各個金融數據源的連線能力。")

target_stock = "2330"

# ==========================================
# 0. 基礎網路環境測試
# ==========================================
st.header("0. 基礎環境")
col1, col2 = st.columns(2)
with col1:
    if st.button("測試對外網路 (Google)"):
        try:
            ip = requests.get("https://api.ipify.org", timeout=5).text
            st.success(f"✅ 對外連線正常 | 本機 IP: {ip}")
        except Exception as e:
            st.error(f"❌ 對外連線失敗 (可能 DNS 或防火牆問題): {e}")

# ==========================================
# 1. Yahoo Finance 測試 (三種手段)
# ==========================================
st.header("1. Yahoo Finance 測試")
c1, c2, c3 = st.columns(3)

with c1:
    if st.button("方法 A: 標準 yfinance"):
        st.info("測試官方 yf.download()...")
        try:
            df = yf.download(f"{target_stock}.TW", period="5d", progress=False)
            if not df.empty:
                st.success(f"✅ 成功! (取得 {len(df)} 筆)")
                st.dataframe(df.head(2))
            else:
                st.error("❌ 失敗: 回傳空資料 (Empty)")
        except Exception as e:
            st.error(f"❌ 報錯: {e}")

with c2:
    if st.button("方法 B: Requests + UserAgent"):
        st.info("測試偽裝 Header 直接請求 API...")
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{target_stock}.TW?range=5d&interval=1d"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        try:
            r = requests.get(url, headers=headers, timeout=5)
            if r.status_code == 200:
                st.success("✅ 成功! (HTTP 200)")
                st.json(r.json()['chart']['result'][0]['meta'])
            else:
                st.error(f"❌ 失敗: HTTP {r.status_code} (可能被擋 IP)")
        except Exception as e:
            st.error(f"❌ 報錯: {e}")

with c3:
    if st.button("方法 C: curl_cffi (最強偽裝)"):
        st.info("測試 curl_cffi 模擬 Chrome 瀏覽器...")
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{target_stock}.TW?range=5d&interval=1d"
        try:
            r = curl_requests.get(url, impersonate="chrome110", timeout=10)
            if r.status_code == 200:
                st.success("✅ 成功! (HTTP 200)")
                st.write("資料長度:", len(r.text))
            else:
                st.error(f"❌ 失敗: HTTP {r.status_code}")
        except Exception as e:
            st.error(f"❌ 報錯: {e}")

# ==========================================
# 2. 證交所 TWSE 測試 (上市)
# ==========================================
st.header("2. 證交所 (TWSE) 測試")
c4, c5 = st.columns(2)

with c4:
    if st.button("TWSE: 標準連線 (Verify SSL)"):
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={target_stock}"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if data['stat'] == 'OK':
                    st.success(f"✅ 成功! 抓到 {len(data['data'])} 筆")
                else:
                    st.warning(f"⚠️ 連線成功但無資料: {data['stat']}")
            else:
                st.error(f"❌ 失敗: HTTP {r.status_code}")
        except Exception as e:
            st.error(f"❌ SSL/連線錯誤: {e}")

with c5:
    if st.button("TWSE: 忽略憑證 (Verify=False)"):
        st.info("強制忽略 SSL 憑證驗證...")
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={target_stock}"
        try:
            r = requests.get(url, timeout=10, verify=False) # 關鍵
            if r.status_code == 200:
                data = r.json()
                if data['stat'] == 'OK':
                    st.success(f"✅ 成功! 抓到 {len(data['data'])} 筆")
                    st.write(data['data'][0])
                else:
                    st.warning(f"⚠️ 無資料: {data['stat']}")
            else:
                st.error(f"❌ 失敗: HTTP {r.status_code}")
        except Exception as e:
            st.error(f"❌ 依然失敗: {e}")

# ==========================================
# 3. 櫃買中心 TPEx 測試 (上櫃)
# ==========================================
st.header("3. 櫃買中心 (TPEx) 測試")
c6, c7 = st.columns(2)
otc_stock = "8069" # 元太

with c6:
    if st.button("TPEx: 標準連線"):
        # 需計算民國年
        roc_year = datetime.now().year - 1911
        roc_month = datetime.now().month
        date_str = f"{roc_year}/{roc_month:02d}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={date_str}&stkno={otc_stock}"
        
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if data.get('aaData'):
                    st.success(f"✅ 成功! 抓到資料")
                else:
                    st.warning("⚠️ 無資料")
            else:
                st.error(f"❌ 失敗: HTTP {r.status_code}")
        except Exception as e:
            st.error(f"❌ 錯誤: {e}")

with c7:
    if st.button("TPEx: 忽略憑證 (Verify=False)"):
        roc_year = datetime.now().year - 1911
        roc_month = datetime.now().month
        date_str = f"{roc_year}/{roc_month:02d}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={date_str}&stkno={otc_stock}"
        
        try:
            r = requests.get(url, timeout=10, verify=False)
            if r.status_code == 200:
                data = r.json()
                if data.get('aaData'):
                    st.success(f"✅ 成功! 抓到資料")
                    st.write(data['aaData'][0])
                else:
                    st.warning("⚠️ 無資料")
            else:
                st.error(f"❌ 失敗: HTTP {r.status_code}")
        except Exception as e:
            st.error(f"❌ 錯誤: {e}")

# ==========================================
# 4. FinMind 測試 (最後備案)
# ==========================================
st.header("4. FinMind 測試")
if st.button("測試 FinMind API"):
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": target_stock,
        "start_date": "2024-12-01"
    }
    try:
        r = requests.get(url, params=params, timeout=10, verify=False)
        if r.status_code == 200:
            data = r.json()
            if data['data']:
                st.success(f"✅ 成功! 抓到 {len(data['data'])} 筆")
                st.dataframe(pd.DataFrame(data['data']).head())
            else:
                st.warning("⚠️ 無資料")
        else:
            st.error(f"❌ 失敗: HTTP {r.status_code}")
    except Exception as e:
        st.error(f"❌ 錯誤: {e}")
