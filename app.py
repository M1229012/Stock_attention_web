# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import requests
import urllib3
import time
from datetime import datetime, timedelta

# 忽略討厭的 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="最終連線診斷", layout="wide")
st.title("🔧 最終連線診斷 (TWSE 修復 + FinMind)")

stock_id = st.text_input("輸入測試代號", "2330")

# ==========================================
# 選項 A: 證交所 TWSE (加上 verify=False 修復 SSL)
# ==========================================
if st.button("測試 A: 證交所 (TWSE) - 已修復 SSL"):
    st.info(f"嘗試從證交所抓取 {stock_id} (強制忽略憑證)...")
    
    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
        
        # [關鍵修正] verify=False 解決 SSLError
        r = requests.get(url, timeout=10, verify=False)
        
        if r.status_code == 200:
            data = r.json()
            if data.get('stat') == 'OK':
                raw = data['data']
                df = pd.DataFrame(raw, columns=['Date', 'Volume', 'Turnover', 'Open', 'High', 'Low', 'Close', 'Change', 'Trans'])
                st.success(f"✅ 成功！證交所連線正常。")
                st.dataframe(df.head())
            else:
                st.warning(f"⚠️ 連線成功但無資料: {data.get('stat')} (可能是上櫃股，請測 FinMind)")
        else:
            st.error(f"❌ HTTP 錯誤: {r.status_code}")
            
    except Exception as e:
        st.error(f"❌ 依然失敗: {e}")

# ==========================================
# 選項 B: FinMind (最強備案)
# ==========================================
if st.button("測試 B: FinMind (開源台股 API)"):
    st.info(f"嘗試從 FinMind 抓取 {stock_id}...")
    
    try:
        # FinMind 不需要 token 也能抓少量資料
        url = "https://api.finmindtrade.com/api/v4/data"
        parameter = {
            "dataset": "TaiwanStockPrice",
            "data_id": stock_id,
            "start_date": (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d"),
            "end_date": datetime.now().strftime("%Y-%m-%d")
        }
        
        r = requests.get(url, params=parameter, timeout=10, verify=False)
        data = r.json()
        
        if data.get("msg") == "success" and data.get("data"):
            df = pd.DataFrame(data["data"])
            st.success(f"✅ 成功！FinMind 資料抓取正常。")
            st.dataframe(df)
        else:
            st.warning("⚠️ FinMind 回傳空資料 (請確認代號是否正確)")
            st.json(data)
            
    except Exception as e:
        st.error(f"❌ FinMind 失敗: {e}")

# ==========================================
# 選項 C: 櫃買中心 (TPEx) - 上櫃股專用
# ==========================================
if st.button("測試 C: 櫃買中心 (TPEx) - 上櫃股"):
    st.info(f"嘗試從櫃買中心抓取 {stock_id} (強制忽略憑證)...")
    
    try:
        # 櫃買需要民國年月份
        roc_year = datetime.now().year - 1911
        roc_month = datetime.now().month
        date_str = f"{roc_year}/{roc_month:02d}"
        
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={date_str}&stkno={stock_id}"
        
        # [關鍵修正] verify=False
        r = requests.get(url, timeout=10, verify=False)
        data = r.json()
        
        if data.get("aaData"):
            st.success(f"✅ 成功！櫃買中心連線正常。")
            st.write(data["aaData"][:3])
        else:
            st.warning("⚠️ 無資料 (可能不是上櫃股)")
            
    except Exception as e:
        st.error(f"❌ 櫃買失敗: {e}")
