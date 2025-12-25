from fugle_marketdata import RestClient
import pandas as pd

# 初始化 (填入您的 API Key)
client = RestClient(api_key="NzY3NTRkMjktMDAwYS00OWYzLWE1ZGQtNTRkM2M2ODA1NTA5IGVlNDAxODIyLWE2YmQtNDlhOC1hZTQwLTZmMjRkMTBmNGM3Mw==")

def fetch_kline_fugle(stock_id):
    stock = client.stock  # Stock API
    
    # 抓取歷史 K 線 (日線)
    data = stock.historical.candles(
        symbol=stock_id, 
        from_="2023-09-01", 
        to_="2023-12-25", 
        fields=["open", "high", "low", "close", "volume"]
    )
    
    # 轉成 DataFrame
    df = pd.DataFrame(data['data'])
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    
    return df
