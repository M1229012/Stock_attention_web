# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import json
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf
import twstock
import gspread
import requests
import re
import urllib3
import time
from datetime import datetime
from google.oauth2.service_account import Credentials

# ==========================================
# 忽略 SSL 警告
# ==========================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =================設定區=================
DATA_CACHE_DIR = "stock_cache_warning_v2"
GSHEET_URL = "https://docs.google.com/spreadsheets/d/1VNgYMxxHoJQPqtntcnPxENOQ2Mbn-wv1kkPoG91l1G8/edit?usp=drive_link"
GSHEET_NAME = "台股注意股資料庫_V33"
GSHEET_WORKSHEET = "近30日熱門統計"
# ========================================

st.set_page_config(page_title="處置股監控中心 Pro", layout="wide", page_icon="🚨")

if not os.path.exists(DATA_CACHE_DIR): os.makedirs(DATA_CACHE_DIR)

# ==========================================
# 1. 樣式設定
# ==========================================
st.markdown("""
<style>
    [data-testid="stMetricValue"] { font-family: 'Courier New', monospace; color: #ff4b4b; }
    .stExpander { border: 1px solid #444; border-radius: 5px; }
    .stButton button { width: 100%; text-align: left; justify-content: flex-start; border: 1px solid #444; }
    .risk-badge { padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 0.9em; display: inline-block; }
    .risk-high { background-color: #521818; color: #ffaaaa; border: 1px solid #ff4b4b; }
    .risk-mid { background-color: #524400; color: #ffeb3b; border: 1px solid #ffeb3b; }
    .risk-low { background-color: #183d20; color: #aaffaa; border: 1px solid #4caf50; }
    .strategy-box { background-color: #262730; padding: 10px; border-radius: 5px; border-left: 5px solid #ff4b4b; margin-top: 10px; line-height: 1.6; }
    [data-testid="stDataFrame"] { font-size: 0.95rem; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. 資料讀取
# ==========================================
@st.cache_data(ttl=30) 
def fetch_data_from_sheet():
    try:
        gc = None
        if os.path.exists("/service_key.json"):
            gc = gspread.service_account(filename="/service_key.json")
        elif os.path.exists("service_key.json"):
            gc = gspread.service_account(filename="service_key.json")
        else:
            try:
                if "gcp_service_account" in st.secrets:
                    creds_dict = st.secrets["gcp_service_account"]
                    creds = Credentials.from_service_account_info(
                        creds_dict,
                        scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
                    )
                    gc = gspread.authorize(creds)
            except: pass
        
        if gc is None:
            st.error("⚠️ 找不到憑證 (請確認 Zeabur Config File 或 service_key.json 是否存在)")
            return pd.DataFrame()

        sh = gc.open_by_url(GSHEET_URL)
        ws = sh.worksheet(GSHEET_WORKSHEET) 
        data = ws.get_all_values()
        
        if len(data) < 2: return pd.DataFrame()
        
        df = pd.DataFrame(data[1:], columns=data[0])
        df = df[df['代號'].astype(str).str.strip() != '']
        return df

    except Exception as e:
        st.error(f"❌ 連接 Google Sheet 錯誤: {e}")
        return pd.DataFrame()

# ==========================================
# 3. 畫圖功能 (Yahoo Debug 強力修復版)
# ==========================================
def get_yahoo_ticker_code(stock_id):
    clean_id = str(stock_id).strip()
    suffix = ".TW" 
    if clean_id in twstock.codes:
        if twstock.codes[clean_id].market == '上櫃': suffix = '.TWO'
    return f"{clean_id}{suffix}"

def fetch_chart_data(stock_id):
    ticker_code = get_yahoo_ticker_code(stock_id)
    
    # 建立偽裝 Session
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })

    df = pd.DataFrame()
    last_error = None

    # [Fix] 定義下載重試邏輯 (包含 session/no-session 雙路徑)
    def attempt_download(target_code):
        inner_err = None
        for i in range(3): # Retry 3 times
            try:
                # Path A: 帶 Session (防擋)
                try:
                    data = yf.download(target_code, period="3mo", auto_adjust=False, session=session, progress=False)
                except TypeError:
                    # Path B: 不帶 Session (舊版兼容)
                    data = yf.download(target_code, period="3mo", auto_adjust=False, progress=False)
                
                if not data.empty:
                    return data, None
            except Exception as e:
                inner_err = e
            
            # Backoff: 降頻避免 429
            time.sleep(1.5 * (i + 1))
        
        return pd.DataFrame(), inner_err

    # 1. 嘗試主要代號 (如 2330.TW)
    df, last_error = attempt_download(ticker_code)

    # 2. 如果失敗，嘗試切換市場 (如 2330.TWO)
    if df.empty and ".TW" in ticker_code:
        alt_ticker = ticker_code.replace(".TW", ".TWO")
        df, last_error = attempt_download(alt_ticker)

    # 資料處理
    if not df.empty:
        try:
            try: df.index = df.index.tz_localize(None)
            except: pass

            if isinstance(df.columns, pd.MultiIndex):
                try: df.columns = df.columns.get_level_values(0)
                except: pass

            df = df.reset_index()
            
            col_map = {}
            for c in df.columns:
                c_str = str(c).lower()
                if 'date' in c_str: col_map[c] = 'Date'
                elif 'open' in c_str: col_map[c] = 'Open'
                elif 'high' in c_str: col_map[c] = 'High'
                elif 'low' in c_str: col_map[c] = 'Low'
                elif 'close' in c_str: col_map[c] = 'Close'
                elif 'volume' in c_str: col_map[c] = 'Volume'
            
            df = df.rename(columns=col_map)

            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
                df.set_index('Date', inplace=True)
                for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                    if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
                for m in [5, 10, 20, 60]: df[f'MA{m}'] = df['Close'].rolling(m).mean()
                return df
        except Exception as e:
            last_error = e # 捕捉處理過程的錯誤

    # --- 3. 救援模式：Twstock (如果 Yahoo 全滅) ---
    if df.empty:
        try:
            ts = twstock.Stock(stock_id)
            raw_data = ts.fetch_31()
            if raw_data:
                df = pd.DataFrame(raw_data)
                df.rename(columns={'date': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'capacity': 'Volume'}, inplace=True)
                df['Date'] = pd.to_datetime(df['Date'])
                df.set_index('Date', inplace=True)
                for m in [5, 10, 20, 60]: df[f'MA{m}'] = df['Close'].rolling(m).mean()
                return df
        except Exception as e:
            # 如果連 Twstock 都掛了，保留 Yahoo 的錯誤訊息
            pass

    # [Fix] 如果全部失敗，印出具體錯誤 (不要 Pass)
    if df.empty and last_error:
        st.error(f"❌ K 線圖抓取失敗 (Yahoo/Twstock): {type(last_error).__name__}: {last_error}")
    
    return pd.DataFrame()

def plot_stock_analysis(stock_id, stock_name):
    df = fetch_chart_data(stock_id)
    if df.empty: 
        # 這裡不顯示 Warning，因為上方 fetch_chart_data 已經會顯示具體 Error
        return

    df.index = df.index.strftime('%Y-%m-%d')
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05, 
                        row_heights=[0.7, 0.3], subplot_titles=(f'{stock_id} {stock_name}', '成交量'))
    
    fig.add_trace(go.Candlestick(x=df.index, open=df['Open'], high=df['High'], 
                                 low=df['Low'], close=df['Close'], name='K線',
                                 increasing_line_color='#ff4b4b', decreasing_line_color='#00da3c'), row=1, col=1)
    
    colors = {'MA5':'#00FFFF', 'MA10':'#FFFF00', 'MA20':'#FF00FF', 'MA60':'#00FF00'}
    for ma, color in colors.items():
        if ma in df.columns:
            fig.add_trace(go.Scatter(x=df.index, y=df[ma], line=dict(color=color, width=1), name=ma), row=1, col=1)
            
    colors_vol = ['#ff4b4b' if c >= o else '#00da3c' for c, o in zip(df['Close'], df['Open'])]
    fig.add_trace(go.Bar(x=df.index, y=df['Volume'], marker_color=colors_vol, name='成交量'), row=2, col=1)
    
    fig.update_layout(height=500, template='plotly_dark', xaxis_rangeslider_visible=False, 
                      showlegend=False, margin=dict(l=10, r=10, t=30, b=10))
    
    fig.update_xaxes(type='category', tickmode='auto', nticks=10) 
    
    st.plotly_chart(fig, use_container_width=True)

# ==========================================
# 4. UI 呈現
# ==========================================
def render_risk_item(row):
    stock_id = row['代號']
    stock_name = row['名稱']
    risk_level = row.get('風險等級', '低')
    trigger_msg = row.get('觸發條件', '')
    reason_msg = row.get('處置觸發原因', '')
    
    try: est_days = int(row.get('最快處置天數', 99))
    except: est_days = 99
    
    try: curr_price = float(row.get('目前價', 0))
    except: curr_price = 0
    try: limit_price = float(row.get('警戒價', 0))
    except: limit_price = 0
    try: gap_pct = float(row.get('差幅(%)', 999))
    except: gap_pct = 999.9
    try: curr_vol = int(float(row.get('目前量', 0))) 
    except: curr_vol = 0
    try: limit_vol = int(float(row.get('警戒量', 0)))
    except: limit_vol = 0
    
    try: turnover_val = float(row.get('成交值(億)', 0))
    except: turnover_val = 0
    try: turnover_rate = float(row.get('週轉率(%)', 0))
    except: turnover_rate = 0
    
    try: pe = float(row.get('PE', 0))
    except: pe = 0
    try: pb = float(row.get('PB', 0))
    except: pb = 0
    try: day_trade_pct = float(row.get('當沖佔比(%)', 0))
    except: day_trade_pct = 0

    try: cnt_10 = int(float(row.get('近10日注意次數', 0)))
    except: cnt_10 = 0
    try: cnt_30 = int(float(row.get('近30日注意次數', 0)))
    except: cnt_30 = 0
    try: streak = int(float(row.get('連續天數', 0)))
    except: streak = 0

    if risk_level == '高':
        icon = "🔴"
        label_html = f'<span class="risk-badge risk-high">極高風險</span>'
    elif risk_level == '中':
        icon = "🟡"
        label_html = f'<span class="risk-badge risk-mid">中風險</span>'
    else:
        icon = "🟢"
        label_html = f'<span class="risk-badge risk-low">低風險</span>'

    if est_days < 90:
        days_str = f"最快 {est_days} 營業日進處置"
    else:
        days_str = "觀察中"

    is_accumulated = (
        "10日" in reason_msg or "30日" in reason_msg or "次" in reason_msg or
        (est_days <= 1 and (cnt_10 >= 5 or cnt_30 >= 11 or streak >= 2))
    )

    key_conditions = []
    
    if est_days == 1:
        if is_accumulated:
            key_conditions.append(f"🔥關鍵: 明日只要 漲/量增 即進處置")
        else:
            conds = []
            if limit_price > 0: 
                if curr_price >= limit_price: 
                    conds.append(f"⚠️現價{curr_price}>警戒{limit_price}")
                else: 
                    conds.append(f"💰安{curr_price}<警{limit_price}")
            
            if limit_vol > 0:
                if curr_vol >= limit_vol: 
                    conds.append(f"⚠️現量{curr_vol:,}>警戒{limit_vol:,}")
                else: 
                    conds.append(f"量<警戒{limit_vol:,}")
            
            if conds:
                cond_str = " | ".join(conds)
                key_conditions.append(f"🔥 {cond_str}")
            else:
                key_conditions.append(f"🔥關鍵: 明日 再觸發任一條款 即進處置")

    elif est_days == 2:
        key_conditions.append(f"🔥關鍵: 未來三日 任兩日漲/達標 即進處置")

    elif est_days == 3:
        key_conditions.append(f"⚠️關鍵: 累積頻繁 留意連續觸發")

    title_parts = [f"{icon} {stock_id} {stock_name} (現價 {curr_price})", days_str]
    
    if key_conditions:
        title_parts.extend(key_conditions)
        
    title_text = " | ".join(title_parts)
    
    with st.expander(title_text):
        c1, c2, c3, c4 = st.columns([0.25, 0.25, 0.25, 0.25])
        
        with c1:
            st.markdown(f"#### 風險：{label_html}", unsafe_allow_html=True)
            st.markdown(f"#### 預測：{days_str}", unsafe_allow_html=True)
            if reason_msg:
                st.markdown(f"<div style='color:#ffaaaa; font-size:0.9em;'>⚠️ {reason_msg}</div>", unsafe_allow_html=True)
            
        with c2:
            strategy_text = ""
            if est_days == 1:
                strategy_text += f"<b>🔥 明日關鍵一戰</b> (最快1日=今日)<br><br>"
                if is_accumulated:
                    strategy_text += f"🚨 <b>次數累計滿水位</b>：近10日已 {cnt_10} 次 (門檻6次)。<br>"
                    strategy_text += f"- ⚠️ <b>操作建議</b>：因次數已滿，今日只要觸發<b>任一款</b>注意條款 (最常見為第6款: 收盤漲、週轉率高)，明日即進處置。<br>"
                    strategy_text += f"- ⛔ <b>請勿追高</b>：這類股票只要收紅盤或量能維持，極高機率被關。<br>"
                else:
                    strategy_text += f"📊 <b>價量防守線</b>：<br>"
                    if limit_price > 0:
                        if curr_price >= limit_price:
                            strategy_text += f"- ⚠️ <b>價格危險</b>：現價 <b>{curr_price}</b> 已高於警戒 <b>{limit_price}</b>。若收盤不壓回，明日處置。<br>"
                        else:
                            strategy_text += f"- ✅ <b>價格安全</b>：現價 <b>{curr_price}</b> 低於警戒 <b>{limit_price}</b>。<br>"
                    
                    if limit_vol > 0:
                        if curr_vol >= limit_vol:
                            strategy_text += f"- ⚠️ <b>量能危險</b>：現量 <b>{curr_vol:,}</b> 已高於警戒 <b>{limit_vol:,}</b>。若收盤不縮量，明日處置。<br>"
                        else:
                            strategy_text += f"- ✅ <b>量能安全</b>：現量 <b>{curr_vol:,}</b> 低於警戒 <b>{limit_vol:,}</b>。<br>"
            
            elif est_days <= 3:
                strategy_text += f"<b>⚠️ 高度警戒區</b><br>"
                strategy_text += f"- 未來 <b>{est_days}</b> 天內，若持續上漲或量能失控，極高機率進入處置。<br>"
            else:
                strategy_text += "✅ <b>目前相對安全</b>，但仍需留意漲跌幅過大被列入注意股。"

            st.markdown(f"<div class='strategy-box'>{strategy_text}</div>", unsafe_allow_html=True)

        with c3:
            st.metric("近30日累積", f"{cnt_30} 次", help="門檻: 12次")
            st.metric("近10日累積", f"{cnt_10} 次", help="門檻: 6次")
            st.metric("連續天數", f"{streak} 天", help="門檻: 3天或5天")
            
        with c4:
            st.metric("成交值", f"{turnover_val} 億")
            st.metric("週轉率", f"{turnover_rate} %")
            day_trade_color = "normal"
            if day_trade_pct > 60: day_trade_color = "off"
            st.metric("當沖佔比", f"{day_trade_pct} %", delta="過熱" if day_trade_pct > 60 else None, delta_color=day_trade_color)
            st.write(f"**PE**: {pe} | **PB**: {pb}")
        
        st.markdown("---")
        plot_stock_analysis(stock_id, stock_name)

# ==========================================
# 5. 輔助函數 (處置中股票用)
# ==========================================
def get_today_date():
    return datetime.now().date()

def parse_roc_date(roc_date_str):
    try:
        roc_date_str = str(roc_date_str).strip()
        parts = re.split(r'[/-]', roc_date_str)
        if len(parts) == 3:
            year = int(parts[0]) + 1911
            month = int(parts[1])
            day = int(parts[2])
            return datetime(year, month, day).date()
    except: return None
    return None

def is_active(period_str):
    if not period_str: return False
    dates = []
    if '～' in period_str: dates = period_str.split('～')
    elif '~' in period_str: dates = period_str.split('~')
    elif '-' in period_str and '/' in period_str:
        if period_str.count('-') == 1: dates = period_str.split('-')
        else: return True 
            
    if len(dates) >= 2:
        end_date_str = dates[1].strip()
        end_date = parse_roc_date(end_date_str)
        if end_date:
            today = get_today_date()
            if end_date >= today: return True
            else: return False
    return True

def clean_tpex_name(raw_name):
    if '(' in raw_name: return raw_name.split('(')[0]
    return raw_name

def clean_tpex_measure(content):
    if "第二次" in content or "再次" in content or "每20分鐘" in content or "每25分鐘" in content or "每60分鐘" in content:
        return "20分鐘盤"
    return "5分鐘盤"

@st.cache_data(ttl=3600)
def fetch_all_disposition_stocks():
    headers = {'User-Agent': 'Mozilla/5.0'}
    all_stock_list = []

    try:
        url_twse = "https://openapi.twse.com.tw/v1/announcement/punish"
        res = requests.get(url_twse, headers=headers, timeout=10, verify=False)
        if res.status_code == 200:
            data = res.json()
            for item in data:
                code = item.get('Code', '').strip()
                name = item.get('Name', '').strip()
                period = item.get('DispositionPeriod', '').strip()
                raw_measure = item.get('DispositionMeasures', '').strip()
                measure = "5分鐘盤"
                if "第二次" in raw_measure or "再次" in raw_measure: measure = "20分鐘盤"
                elif "第一次" in raw_measure: measure = "5分鐘盤"
                if is_active(period):
                    all_stock_list.append({'市場': '上市', '代號': code, '名稱': name, '處置期間': period, '處置措施': measure})
    except: pass

    try:
        url_tpex = "https://www.tpex.org.tw/web/bulletin/disposal_information/disposal_information_result.php?l=zh-tw&o=json"
        res = requests.get(url_tpex, headers=headers, timeout=10, verify=False)
        data = res.json()
        tpex_data = []
        is_tables = False
        if 'tables' in data and len(data['tables']) > 0:
            tpex_data = data['tables'][0]['data']
            is_tables = True
        elif 'aaData' in data:
            tpex_data = data['aaData']
            is_tables = False
            
        if tpex_data:
            for row in tpex_data:
                try:
                    if is_tables:
                        code = str(row[2]).strip(); raw_name = str(row[3]).strip(); period = str(row[5]).strip(); raw_content = str(row[7]).strip()
                    else:
                        code = str(row[1]).strip(); raw_name = str(row[2]).strip(); period = str(row[4]).strip(); raw_content = str(row[6]).strip() if len(row) > 6 else ""

                    if is_active(period):
                        name = clean_tpex_name(raw_name)
                        measure = clean_tpex_measure(raw_content)
                        all_stock_list.append({'市場': '上櫃', '代號': code, '名稱': name, '處置期間': period, '處置措施': measure})
                except: continue
    except: pass

    df = pd.DataFrame(all_stock_list)
    if not df.empty:
        df['sort_key'] = df['市場'].map({'上市': 0, '上櫃': 1})
        df = df.sort_values(by=['sort_key', '代號'], ascending=[True, True])
        df = df[['市場', '代號', '名稱', '處置期間', '處置措施']]
    return df

# ==========================================
# 6. 主頁面：處置股預警
# ==========================================
def run_warning_page():
    st.title("⚠️ 處置股預警機")
    col_btn, col_info = st.columns([0.2, 0.8])
    if col_btn.button("🔄 重新讀取資料"):
        st.cache_data.clear() 
        st.rerun()
        
    df = fetch_data_from_sheet()
    df_jail = fetch_all_disposition_stocks()
    jail_codes = []
    if not df_jail.empty: jail_codes = df_jail['代號'].astype(str).tolist()

    if not df.empty:
        last_date = df.iloc[0]['最近一次日期'] if '最近一次日期' in df.columns else "未知"
        col_info.info(f"資料來源：Google Sheet | 資料日期：{last_date}")
        initial_count = len(df)
        df = df[~df['代號'].isin(jail_codes)]
        filtered_count = initial_count - len(df)
        if filtered_count > 0: st.caption(f"已自動隱藏 {filtered_count} 檔正在處置中的股票。")

        def sort_key(row):
            try: days = int(row.get('最快處置天數', 99))
            except: days = 99
            risk_map = {'高': 3, '中': 2, '低': 1}
            risk_score = risk_map.get(row.get('風險等級', '低'), 0)
            try: streak = int(row.get('連續天數', 0))
            except: streak = 0
            return (risk_score * 10000) + ((100 - days) * 100) + streak

        data_list = df.to_dict('records')
        data_list.sort(key=sort_key, reverse=True)
        
        st.subheader(f"📋 潛在風險名單 (共 {len(data_list)} 檔)")
        for row in data_list: render_risk_item(row)
    else:
        st.warning("無法讀取資料，請檢查 Google Sheet 連線或確認後端程式是否已執行。")

# ==========================================
# 7. 主頁面：處置中股票
# ==========================================
def run_jail_page():
    st.title("🔒 處置中股票")
    if st.button("🔄 抓取最新名單"):
        with st.spinner("連線中..."):
            df_dispo = fetch_all_disposition_stocks()
            if not df_dispo.empty:
                st.success(f"目前共有 {len(df_dispo)} 檔處置股。")
                def highlight_status(val):
                    color = ''
                    s_val = str(val)
                    if '20分鐘' in s_val: color = '#521818'
                    elif '5分鐘' in s_val: color = '#3d3300'
                    if color: return f'background-color: {color}; font-weight: bold; border-radius: 5px;'
                    return ''
                try:
                    styled_df = df_dispo.style.applymap(highlight_status, subset=['處置措施'])
                    st.dataframe(styled_df, hide_index=True, use_container_width=True)
                except: st.dataframe(df_dispo, hide_index=True, use_container_width=True)
            else: st.success("目前沒有處置股。")

# ==========================================
# 主程式入口
# ==========================================
with st.sidebar:
    st.title("⚡ 監控中心")
    page = st.radio("功能", ["⚠️ 處置預警", "🔒 處置中股票"])

if page == "⚠️ 處置預警": run_warning_page()
elif page == "🔒 處置中股票": run_jail_page()
