# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from fugle_marketdata import RestClient
from datetime import datetime, timedelta

st.set_page_config(page_title="Fugle 連線測試", layout="wide")
st.title("⚡ Fugle API 連線測試診斷")

st.markdown("""
### 測試說明
此工具用於測試 Zeabur 主機是否能透過 Fugle API 抓取股價資料。
請先去 [Fugle Developer](https://developer.fugle.tw/) 申請 API Key。
""")

# 1. 輸入 API Key
api_key_input = st.text_input("請輸入您的 Fugle API Key:", type="password")

if st.button("🚀 開始測試連線"):
    if not api_key_input:
        st.error("❌ 請先輸入 API Key")
    else:
        st.info("正在嘗試連線 Fugle 伺服器...")
        
        try:
            # 初始化 Client
            client = RestClient(api_key=api_key_input)
            stock = client.stock
            
            # 設定測試參數 (抓取台積電 2330 過去 5 天的日 K)
            # Fugle 不需要 ".TW"，直接給 "2330" 即可
            target_id = "2330"
            today = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
            
            st.write(f"測試目標: {target_id} | 時間範圍: {start_date} ~ {today}")
            
            # 發送請求
            data = stock.historical.candles(
                symbol=target_id,
                from_=start_date,
                to=today,
                fields=["open", "high", "low", "close", "volume", "date"]
            )
            
            # 檢查結果
            if 'data' in data and len(data['data']) > 0:
                st.success(f"✅ 連線成功！成功取得 {len(data['data'])} 筆 K 線資料。")
                
                # 轉成 DataFrame 展示
                df = pd.DataFrame(data['data'])
                df['date'] = pd.to_datetime(df['date'])
                st.dataframe(df)
                
                st.markdown("### ✅ 診斷結果：")
                st.markdown("- Fugle API 在此環境 **可正常運作**。")
                st.markdown("- 您可以放心地將主程式改為 Fugle 版本。")
            else:
                st.warning("⚠️ 連線成功，但回傳無資料 (可能是休市或日期範圍問題)。")
                st.json(data)
                
        except Exception as e:
            st.error(f"❌ 連線失敗 (Crash): {e}")
            st.write("常見原因：API Key 錯誤、額度用盡、或套件版本不相容。")
