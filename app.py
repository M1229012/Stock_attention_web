# -*- coding: utf-8 -*-
import streamlit as st
import yfinance as yf
import pandas as pd
import requests
import twstock
from curl_cffi import requests as curl_requests
import time

st.set_page_config(page_title="連線診斷工具", layout="wide")
st.title("🔧 Yahoo Finance 連線診斷工具")
st.markdown("此工具用於測試雲端環境 (Zeabur) 是否能成功連線至 Yahoo Finance，請依序測試。")

stock_id = st.text_input("輸入測試代號", "2330.TW")

# ==========================================
# 測試 1: 標準 yfinance (最容易失敗)
# ==========================================
if st.button("測試 1: 標準 yfinance (官方原版)"):
    st.info(f"正在嘗試使用 yfinance 下載 {stock_id}...")
    try:
        # 完全不加任何參數，測試最原始的連線
        df = yf.download(stock_id, period="5d", progress=False)
        
        if df.empty:
            st.error("❌ 回傳空資料 (Empty DataFrame)")
        else:
            st.success(f"✅ 成功抓取！(筆數: {len(df)})")
            st.dataframe(df)
            
    except Exception as e:
        st.error(f"❌ 發生錯誤: {type(e).__name__}: {e}")

# ==========================================
# 測試 2: curl_cffi 偽裝瀏覽器 (繞過封鎖)
# ==========================================
if st.button("測試 2: curl_cffi 直接請求 (繞過 yfinance)"):
    st.info("正在嘗試偽裝成 Chrome 瀏覽器直接請求 Yahoo API...")
    
    # 這是 Yahoo Finance 畫圖用的原始 API，不透過 yfinance 套件
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}?range=5d&interval=1d"
    
    try:
        # 使用 curl_cffi 模擬真實瀏覽器指紋
        r = curl_requests.get(
            url, 
            impersonate="chrome110",  # 模擬 Chrome 110
            timeout=10
        )
        
        if r.status_code == 200:
            data = r.json()
            if "chart" in data and "result" in data["chart"] and data["chart"]["result"]:
                result = data["chart"]["result"][0]
                timestamps = result["timestamp"]
                quotes = result["indicators"]["quote"][0]
                
                df = pd.DataFrame({
                    "Date": pd.to_datetime(timestamps, unit="s"),
                    "Close": quotes["close"],
                    "Volume": quotes["volume"]
                })
                # 修正時區
                df["Date"] = df["Date"].dt.tz_localize("UTC").dt.tz_convert("Asia/Taipei").dt.tz_localize(None)
                
                st.success(f"✅ 成功！curl_cffi 成功騙過 Yahoo。")
                st.dataframe(df)
            else:
                st.warning("⚠️ 連線成功但沒有數據 (可能是代號錯誤或無交易)")
                st.json(data)
        elif r.status_code == 429:
            st.error("❌ 失敗：429 Too Many Requests (IP 被封鎖)")
        elif r.status_code == 403:
            st.error("❌ 失敗：403 Forbidden (Yahoo 拒絕存取)")
        else:
            st.error(f"❌ 失敗：Status Code {r.status_code}")
            st.text(r.text[:500])
            
    except Exception as e:
        st.error(f"❌ 程式錯誤: {e}")

# ==========================================
# 測試 3: 證交所 TWSE (最後防線)
# ==========================================
if st.button("測試 3: 證交所 TWSE 官網 (不靠 Yahoo)"):
    clean_id = stock_id.replace(".TW", "").replace(".TWO", "")
    st.info(f"正在嘗試從證交所抓取 {clean_id}...")
    
    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={clean_id}"
        r = requests.get(url, timeout=5)
        data = r.json()
        
        if data.get('stat') == 'OK':
            raw = data['data']
            df = pd.DataFrame(raw, columns=['Date', 'Volume', 'Turnover', 'Open', 'High', 'Low', 'Close', 'Change', 'Trans'])
            st.success(f"✅ 成功從證交所抓到資料！")
            st.dataframe(df)
        else:
            st.error(f"❌ 證交所回傳錯誤: {data.get('stat')} (可能是上櫃股或是休市)")
            
    except Exception as e:
        st.error(f"❌ 連線錯誤: {e}")

# ==========================================
# 環境資訊
# ==========================================
with st.expander("查看環境資訊"):
    try:
        ip = requests.get("https://api.ipify.org", timeout=3).text
        st.write(f"目前主機 IP: {ip}")
    except:
        st.write("無法取得 IP")
    
    st.write(f"yfinance 版本: {yf.__version__}")
