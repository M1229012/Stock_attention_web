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
# ✅ 修正 1: 補上 date import，避免 is_active() 噴錯
from datetime import datetime, date
from google.oauth2.service_account import Credentials
from zoneinfo import ZoneInfo
from requests.exceptions import SSLError

# 忽略 SSL 警告
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
# 2. 資料讀取 (雲端適配版)
# ==========================================
@st.cache_data(ttl=30) 
def fetch_data_from_sheet():
    try:
        gc = None
        # 優先檢查 Streamlit Cloud 的 Secrets
        if "gcp_service_account" in st.secrets:
            creds_dict = st.secrets["gcp_service_account"]
            creds = Credentials.from_service_account_info(
                creds_dict,
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
        else:
            # 本地端檔案讀取模式
            json_key_path = "service_key.json"
            if not os.path.exists(json_key_path):
                current_dir = os.path.dirname(os.path.abspath(__file__))
                json_key_path = os.path.join(current_dir, "service_key.json")
            
            if os.path.exists(json_key_path):
                gc = gspread.service_account(filename=json_key_path)

        if not gc:
            st.error("⚠️ 找不到憑證 (請在 Streamlit Cloud 設定 Secrets 或檢查 service_key.json)")
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
# 3. 畫圖功能 (Yahoo 原版)
# ==========================================
def get_yahoo_ticker_code(stock_id):
    clean_id = str(stock_id).strip()
    suffix = ".TW" 
    if clean_id in twstock.codes:
        if twstock.codes[clean_id].market == '上櫃': suffix = '.TWO'
    return f"{clean_id}{suffix}"

def fetch_chart_data(stock_id):
    ticker_code = get_yahoo_ticker_code(stock_id)
    try:
        ticker = yf.Ticker(ticker_code)
        df = ticker.history(period="3mo")
        
        if df.empty and ".TW" in ticker_code: 
             ticker = yf.Ticker(ticker_code.replace(".TW", ".TWO"))
             df = ticker.history(period="3mo")
        
        if not df.empty:
            df = df.reset_index()
            if df['Date'].dt.tz is not None:
                df['Date'] = df['Date'].dt.tz_localize(None)
            df.set_index('Date', inplace=True)
            for m in [5, 10, 20, 60]: df[f'MA{m}'] = df['Close'].rolling(m).mean()
            return df
    except: pass
    return pd.DataFrame()

def plot_stock_analysis(stock_id, stock_name):
    df = fetch_chart_data(stock_id)
    if df.empty: 
        st.warning("⚠️ 無法載入 K 線圖數據 (Yahoo 可能暫時限流)")
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
    
    # ✅ [前端修正]：強制把剩 2 天以內的股票升級為「高風險(紅燈)」
    if est_days <= 2:
        risk_level = '高'
    
    def safe_float(v):
        try: return float(str(v).replace(',', ''))
        except: return 0
    def safe_int(v):
        try: return int(float(str(v).replace(',', '')))
        except: return 0

    curr_price = safe_float(row.get('目前價'))
    limit_price = safe_float(row.get('警戒價'))
    curr_vol = safe_int(row.get('目前量'))
    limit_vol = safe_int(row.get('警戒量'))
    
    turnover_val = safe_float(row.get('成交值(億)'))
    turnover_rate = safe_float(row.get('週轉率(%)'))
    pe = safe_float(row.get('PE'))
    pb = safe_float(row.get('PB'))
    day_trade_pct = safe_float(row.get('當沖佔比(%)'))

    cnt_10 = safe_int(row.get('近10日注意次數'))
    cnt_30 = safe_int(row.get('近30日注意次數'))
    streak = safe_int(row.get('連續天數'))

    if risk_level == '高':
        icon = "🔴"
        label_html = f'<span class="risk-badge risk-high">極高風險</span>'
    elif risk_level == '中':
        icon = "🟡"
        label_html = f'<span class="risk-badge risk-mid">中風險</span>'
    else:
        icon = "🟢"
        label_html = f'<span class="risk-badge risk-low">低風險</span>'

    days_str = f"最快 {est_days} 營業日進處置" if est_days < 90 else "觀察中"

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
                if curr_price >= limit_price: conds.append(f"⚠️現價{curr_price}>警戒{limit_price}")
                else: conds.append(f"💰安{curr_price}<警{limit_price}")
            
            if limit_vol > 0:
                if curr_vol >= limit_vol: conds.append(f"⚠️現量{curr_vol:,}>警戒{limit_vol:,}")
                else: conds.append(f"量<警戒{limit_vol:,}")
            
            if conds: key_conditions.append(f"🔥 {' | '.join(conds)}")
            else: key_conditions.append(f"🔥關鍵: 明日 再觸發任一條款 即進處置")

    elif est_days == 2:
        key_conditions.append(f"🔥關鍵: 未來三日 任兩日漲/達標 即進處置")

    elif est_days == 3:
        key_conditions.append(f"⚠️關鍵: 累積頻繁 留意連續觸發")

    title_parts = [f"{icon} {stock_id} {stock_name} (現價 {curr_price})", days_str]
    if key_conditions: title_parts.extend(key_conditions)
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
                        strategy_text += f"- {'⚠️ 價格危險' if curr_price >= limit_price else '✅ 價格安全'}：現價 {curr_price} vs 警戒 {limit_price}<br>"
                    if limit_vol > 0:
                        strategy_text += f"- {'⚠️ 量能危險' if curr_vol >= limit_vol else '✅ 量能安全'}：現量 {curr_vol:,} vs 警戒 {limit_vol:,}<br>"
            elif est_days <= 3:
                strategy_text += f"<b>⚠️ 高度警戒區</b><br>- 未來 {est_days} 天內，若持續上漲或量能失控，極高機率進入處置。<br>"
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
            day_trade_color = "off" if day_trade_pct > 60 else "normal"
            st.metric("當沖佔比", f"{day_trade_pct} %", delta="過熱" if day_trade_pct > 60 else None, delta_color=day_trade_color)
            st.write(f"**PE**: {pe} | **PB**: {pb}")
        
        st.markdown("---")
        plot_stock_analysis(stock_id, stock_name)

# ==========================================
# 5. 輔助函數 (處置中股票用) - 本地一致版
# ==========================================
def get_today_date():
    # 強制使用台灣時間，確保換日邏輯一致
    return datetime.now(ZoneInfo("Asia/Taipei")).date()

def extract_dates_any(s: str):
    s = str(s or "").strip()

    # 1) 114/12/25、114-12-25、2025.12.25
    p1 = re.findall(r'(\d{2,4})[./-](\d{1,2})[./-](\d{1,2})', s)

    # 2) 114年12月25日（可無日）
    p2 = re.findall(r'(\d{2,4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日?', s)
    
    # 3) 1141223 (7碼純數字，TPEx 常見格式)
    p3 = re.findall(r'(\d{3})(\d{2})(\d{2})', s)

    hits = p1 + p2 + p3
    dates = []
    for y, m, d in hits:
        try:
            y = int(y); m = int(m); d = int(d)
            # 年份判斷：小於 1911 視為民國年；否則視為西元年
            if y < 1911:
                y += 1911
            dates.append(date(y, m, d))
        except:
            pass
    return dates

def format_roc_period(period_str):
    """將解析到的日期格式化為 114/MM/DD～114/MM/DD"""
    dates = extract_dates_any(period_str)
    if len(dates) >= 2:
        start, end = dates[0], dates[1]
        # 確保順序
        if start > end: start, end = end, start
        
        s_str = f"{start.year - 1911}/{start.month:02d}/{start.day:02d}"
        e_str = f"{end.year - 1911}/{end.month:02d}/{end.day:02d}"
        return f"{s_str}～{e_str}"
    return period_str

def is_active(period_str):
    """
    回傳：
    - True  : 今日在處置區間內
    - False : 今日不在處置區間內
    - None  : 解析不到區間（避免把整批 TPEx 全濾掉）
    """
    ds = extract_dates_any(period_str)
    if len(ds) < 2:
        return None

    start, end = ds[0], ds[1]
    if start > end:
        start, end = end, start

    today = get_today_date()
    return start <= today <= end

def clean_tpex_name(raw_name):
    return raw_name.split('(')[0] if '(' in raw_name else raw_name

def clean_tpex_measure(content):
    if any(k in content for k in ["第二次", "再次", "每20分鐘", "每25分鐘", "每60分鐘"]): return "20分鐘盤"
    return "5分鐘盤"

def safe_get(url, headers=None, timeout=10):
    """
    Streamlit Cloud 上 openapi.twse / tpex 常見 OpenSSL 驗證問題：
    直接強制 verify=False，避免每次先 verify=True 一定炸。
    """
    try:
        res = requests.get(url, headers=headers, timeout=timeout, verify=False)
        return res
    except Exception as e:
        st.error(f"❌ 請求失敗: {url}\n原因: {e}")
        raise

def safe_json(res):
    """避免 res.json() 因為 BOM/非 JSON 直接炸掉"""
    try:
        return res.json()
    except Exception:
        return json.loads(res.text.lstrip("\ufeff").strip())

def pick_4digit_code_from_values(obj):
    # obj 可以是 dict 或 list
    vals = obj.values() if isinstance(obj, dict) else obj
    for v in vals:
        t = re.sub(r'<[^>]+>', '', str(v)).strip()  # 去 HTML
        if re.fullmatch(r'\d{4}', t):
            return t
    return ""

def clean_text(x):
    return re.sub(r'<[^>]+>', '', str(x)).replace("&nbsp;", " ").strip()

@st.cache_data(ttl=300)
def fetch_all_disposition_stocks():
    headers = {'User-Agent': 'Mozilla/5.0'}
    all_stock_list = []

    # 1. 上市 (TWSE) - 移除 verify=False，使用標準流程
    try:
        url_twse = "https://openapi.twse.com.tw/v1/announcement/punish"
        # 使用 safe_get
        res = safe_get(url_twse, headers=headers, timeout=10)
        
        if res.status_code == 200:
            payload = safe_json(res)
            for item in payload:
                code = item.get('Code', '').strip()
                # ✅ 修正 2: 增加四碼檢查
                if not (code.isdigit() and len(code) == 4): continue

                name = item.get('Name', '').strip()
                period = item.get('DispositionPeriod', '').strip()
                raw_measure = item.get('DispositionMeasures', '').strip()
                
                measure = "20分鐘盤" if any(k in raw_measure for k in ["第二次","再次"]) else "5分鐘盤"
                
                # is_active 回傳 True/False/None，只要不是 False 都當作有效 (None保留)
                active = is_active(period)
                if active is not False:
                    # 上市通常已經格式好了，但也可以套用一下統一格式
                    all_stock_list.append({'市場': '上市', '代號': code, '名稱': name, '處置期間': format_roc_period(period), '處置措施': measure})
        else:
            st.error(f"TWSE 回傳非 200: {res.status_code}\n{res.text[:200]}")
    except Exception as e:
        st.error(f"TWSE 處置股抓取失敗: {e}")

    # 2. 上櫃 (TPEx) - 改用 TPEx OpenAPI v1（本地/雲端都更穩），舊 aaData 當 fallback
    try:
        # ✅ 官方 OpenAPI：上櫃處置有價證券資訊
        url_tpex_api = "https://www.tpex.org.tw/openapi/v1/tpex_disposal_information"
        res = safe_get(url_tpex_api, headers=headers, timeout=10)
        
        if res.status_code != 200:
            st.error(f"TPEx OpenAPI 非 200: {res.status_code}\n{res.text[:200]}")

        payload = safe_json(res)

        # 這個 API 實務上通常回傳「list[dict]」
        if isinstance(payload, dict) and "data" in payload:
            payload = payload["data"]
        if not isinstance(payload, list):
            payload = []

        for item in payload:
            # ✅ key 名可能不同：先吃常見 key，再不行就從 values 撿 4 碼代號
            code = clean_text(
                item.get("SecuritiesCompanyCode")
                or item.get("證券代號")
                or item.get("代號")
                or ""
            )
            if not code:
                code = pick_4digit_code_from_values(item)

            if not (code.isdigit() and len(code) == 4):
                continue

            name = clean_text(
                item.get("CompanyName")
                or item.get("證券名稱")
                or item.get("名稱")
                or ""
            )

            # 抓取原始字串 (可能是 1141223 這種格式)
            period_raw = clean_text(
                item.get("DispositionPeriod")
                or item.get("處置期間")
                or item.get("處置起迄")
                or ""
            )
            # 統一格式化
            period = format_roc_period(period_raw)

            raw_content = clean_text(
                item.get("DisposalCondition")
                or item.get("DispositionReasons")
                or item.get("處置措施")
                or item.get("處置內容")
                or ""
            )

            active = is_active(period_raw)

            # ✅ 關鍵：解析不到日期(None)不要直接丟掉，否則 TPEx 很容易全空
            if active is False:
                continue

            all_stock_list.append({
                "市場": "上櫃",
                "代號": code,
                "名稱": clean_tpex_name(name) if name else "",
                "處置期間": period,
                "處置措施": clean_tpex_measure(raw_content),
            })

        # ✅ 若 OpenAPI 端點回來是空（被擋/格式變），再用你本來 aaData 的舊端點備援
        if len([x for x in all_stock_list if x.get("市場") == "上櫃"]) == 0:
            url_tpex_old = "https://www.tpex.org.tw/web/bulletin/disposal_information/disposal_information_result.php?l=zh-tw&o=json"
            res2 = safe_get(url_tpex_old, headers=headers, timeout=10)
            
            if res2.status_code != 200:
                st.error(f"TPEx 舊端點非 200: {res2.status_code}\n{res2.text[:200]}")

            data2 = safe_json(res2)
            tpex_data = data2.get("aaData", [])

            for row in tpex_data:
                if not isinstance(row, list) or len(row) == 0:
                    continue

                cells = [clean_text(x) for x in row]

                code = next((c for c in cells if re.fullmatch(r"\d{4}", c)), "")
                if not code:
                    continue

                # 名稱：找第一個「不是代號、不是日期區間」的字串
                name = ""
                for c in cells:
                    if not c or c == code:
                        continue
                    if len(extract_dates_any(c)) >= 2:
                        continue
                    name = c
                    break

                # 期間：找包含至少 2 個日期的 cell
                period_raw = next((c for c in cells if len(extract_dates_any(c)) >= 2), "")
                period = format_roc_period(period_raw)

                # 措施：找含「分鐘」或「分盤」字樣
                raw_content = next((c for c in cells if ("分鐘" in c) or ("分盤" in c)), "")
                if not raw_content:
                    raw_content = " ".join(cells)

                active = is_active(period_raw)
                if active is False:
                    continue

                all_stock_list.append({
                    "市場": "上櫃",
                    "代號": code,
                    "名稱": clean_tpex_name(name) if name else "",
                    "處置期間": period,
                    "處置措施": clean_tpex_measure(raw_content),
                })

    except Exception as e:
        st.error(f"TPEx 處置股抓取失敗: {e}")

    df = pd.DataFrame(all_stock_list)
    if not df.empty:
        df['sort_key'] = df['市場'].map({'上市': 0, '上櫃': 1})
        df = df.sort_values(by=['sort_key', '代號'], ascending=[True, True])
        df = df[['市場', '代號', '名稱', '處置期間', '處置措施']]
    return df

# ==========================================
# 6. 主頁面
# ==========================================
def run_warning_page():
    st.title("⚠️ 處置股預警機")
    
    col_btn, col_chk, col_info = st.columns([0.2, 0.2, 0.6])
    
    if col_btn.button("🔄 重新讀取"):
        st.cache_data.clear() 
        st.rerun()
    
    # 讓使用者決定要不要看已經被關的股票
    show_jail_stocks = col_chk.checkbox("顯示已處置股", value=False)
    
    # ✅ 新增：搜尋欄
    search_term = st.text_input("🔍 搜尋股票 (輸入代號或名稱)", "").strip()
        
    df = fetch_data_from_sheet()
    df_jail = fetch_all_disposition_stocks()
    # ✅ 修正 3: 增加 str.strip()，確保比對精確
    jail_codes = df_jail['代號'].astype(str).str.strip().tolist() if not df_jail.empty else []

    if not df.empty:
        last_date = df.iloc[0].get('最近一次日期', '未知')
        col_info.info(f"資料來源：Google Sheet | 資料日期：{last_date}")
        
        # 修改邏輯：只有在「不勾選」顯示處置股時，才進行過濾
        if not show_jail_stocks:
            df = df[~df['代號'].isin(jail_codes)]
        
        # ✅ 新增：搜尋過濾邏輯
        if search_term:
            df = df[df['代號'].astype(str).str.contains(search_term) | df['名稱'].astype(str).str.contains(search_term)]

        # ✅ 優化排序：天數越少越前面 (權重最大)，其次是風險等級
        def sort_key(row):
            try: days = int(row.get('最快處置天數', 99))
            except: days = 99
            
            # 前端強制修正風險等級 (讓排序正確)
            risk_level = row.get('風險等級', '低')
            if days <= 2: risk_level = '高'
            
            risk_map = {'高': 3, '中': 2, '低': 1}
            risk_score = risk_map.get(risk_level, 0)
            
            # 排序公式：
            # 1. 天數 (越小分越高): (100 - days) * 100000 -> 權重最大，確保剩1天的排在剩2天的前面
            # 2. 風險 (越高分越高): risk_score * 1000
            return ((100 - days) * 100000) + (risk_score * 1000)

        data_list = df.to_dict('records')
        data_list.sort(key=sort_key, reverse=True)
        
        st.subheader(f"📋 潛在風險名單 (共 {len(data_list)} 檔)")
        
        if len(data_list) == 0:
            st.info("目前沒有符合條件的股票。")
        
        for row in data_list: 
            # 額外標註一下是否在處置中
            is_in_jail = str(row['代號']) in jail_codes
            if is_in_jail:
                row['名稱'] = f"(🔒處置中) {row['名稱']}"
            render_risk_item(row)
    else:
        st.warning("無法讀取資料，請檢查 Google Sheet 連線或確認後端程式是否已執行。")

def run_jail_page():
    st.title("🔒 處置中股票")
    if st.button("🔄 抓取最新名單"):
        st.cache_data.clear()
        with st.spinner("連線中..."):
            df_dispo = fetch_all_disposition_stocks()
            if not df_dispo.empty:
                st.success(f"目前共有 {len(df_dispo)} 檔處置股。")
                def highlight_status(val):
                    color = '#521818' if '20分鐘' in str(val) else '#3d3300' if '5分鐘' in str(val) else ''
                    return f'background-color: {color}; font-weight: bold; border-radius: 5px;' if color else ''
                try:
                    st.dataframe(df_dispo.style.applymap(highlight_status, subset=['處置措施']), hide_index=True, use_container_width=True)
                except:
                    st.dataframe(df_dispo, hide_index=True, use_container_width=True)
            else: st.success("目前沒有處置股。")

with st.sidebar:
    st.title("⚡ 監控中心")
    page = st.radio("功能", ["⚠️ 處置預警", "🔒 處置中股票"])

if page == "⚠️ 處置預警": run_warning_page()
elif page == "🔒 處置中股票": run_jail_page()
