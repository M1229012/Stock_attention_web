# -*- coding: utf-8 -*-
import streamlit as st
import yfinance as yf
import pandas as pd
import requests
import twstock
# 確保 curl_cffi 有被引用
try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

# [Fix] 補上 os，雖然這支測試程式為了避免錯誤，我已經把 os 相關操作移除了，但預防萬一還是加上
import os 

st.set_page_config(page_title="連線診斷工具", layout="wide")
st.title("🔧 Yahoo Finance 連線診斷工具 (修復版)")

# 顯示套件狀態
st.write("### 環境檢查")
col1, col2 = st.columns(2)
with col1:
    st.write(f"Pandas version: {pd.__version__}")
    st.write(f"Yfinance version: {yf.__version__}")
with col2:
    if curl_requests:
        st.success("✅ curl_cffi 套件已安裝")
    else:
        st.error("❌ curl_cffi 套件未安裝 (請檢查 requirements.txt)")

stock_id = st.text_input("輸入測試代號 (例如 2330.TW)", "2330.TW")

# ==========================================
# 測試 1: 標準 yfinance (最基本測試)
# ==========================================
if st.button("測試 1: 標準 yfinance (官方原版)"):
    st.info(f"正在嘗試使用 yfinance 下載 {stock_id}...")
    try:
        # 完全不加任何參數，測試最原始的連線
        df = yf.download(stock_id, period="5d", progress=False)
        
        if df.empty:
            st.error("❌ 回傳空資料 (Empty DataFrame) - 可能被 Yahoo 擋 IP")
        else:
            st.success(f"✅ 成功抓取！(筆數: {len(df)})")
            # 簡單處理顯示
            st.dataframe(df.head())
            
    except Exception as e:
        st.error(f"❌ 發生錯誤: {type(e).__name__}: {e}")

# ==========================================
# 測試 2: curl_cffi 偽裝瀏覽器 (繞過封鎖)
# ==========================================
if st.button("測試 2: curl_cffi 直接請求 (繞過 yfinance)"):
    if not curl_requests:
        st.error("無法測試：curl_cffi 未安裝")
    else:
        st.info("正在嘗試偽裝成 Chrome 瀏覽器直接請求 Yahoo API...")
        
        # 這是 Yahoo Finance 畫圖用的原始 API
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}?range=5d&interval=1d"
        
        try:
            # 使用 curl_cffi 模擬真實瀏覽器指紋
            r = curl_requests.get(
                url, 
                impersonate="chrome110",  # 模擬 Chrome
                timeout=10
            )
            
            if r.status_code == 200:
                data = r.json()
                if "chart" in data and "result" in data["chart"] and data["chart"]["result"]:
                    st.success(f"✅ 成功！curl_cffi 成功騙過 Yahoo。")
                    st.json(data["chart"]["result"][0]["meta"]) # 顯示部分資料證明成功
                else:
                    st.warning("⚠️ 連線成功但沒有數據 (可能是代號錯誤或無交易)")
            elif r.status_code == 429:
                st.error("❌ 失敗：429 Too Many Requests (IP 被封鎖)")
            elif r.status_code == 403:
                st.error("❌ 失敗：403 Forbidden (Yahoo 拒絕存取)")
            else:
                st.error(f"❌ 失敗：Status Code {r.status_code}")
                st.text(r.text[:200])
                
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
        # 加上基本的 User-Agent
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=5)
        data = r.json()
        
        if data.get('stat') == 'OK':
            st.success(f"✅ 成功從證交所抓到資料！")
            st.write(data['data'][:3]) # 顯示前三筆
        else:
            st.error(f"❌ 證交所回傳錯誤: {data.get('stat')} (如果是上櫃股，請忽略此錯誤)")
            
    except Exception as e:
        st.error(f"❌ 連線錯誤: {e}")
