from RawDataReader.RawDataReader import RawDataReader
from datetime import datetime
import pandas as pd
import re


# 用於讀取google trending上的csv data
class CSVFileRawDataReader(RawDataReader):
    def __init__(self, path):
        super().__init__()
        self.path = path
    
    def readData(self) -> list:
        df = pd.read_csv(self.path)
        result = []

        for _, row in df.iterrows():
            # keyword
            keyword = row['Trends']
            
            # frequency: 轉換 "5M+" 或 "120K" 成整數
            freq_str = str(row['Search volume'])
            freq = 0
            if pd.notna(freq_str):
                if 'M' in freq_str:
                    freq = int(float(freq_str.replace('M', '').replace('+','')) * 1_000_000)
                elif 'K' in freq_str:
                    freq = int(float(freq_str.replace('K', '').replace('+','')) * 1_000)
                else:
                    # 沒有單位直接轉 int
                    freq = int(re.sub(r'\D', '', freq_str))
            
            # Started: "November 9, 2025 at 2:10:00 PM UTC+8" -> timestamp
            started_str = row['Started']
            started_ts = None
            if pd.notna(started_str):
                try:
                    # 先去掉非標準空格
                    started_str = started_str.replace("\u202f"," ")
                    dt = datetime.strptime(started_str, "%B %d, %Y at %I:%M:%S %p UTC%z")
                    started_ts = int(dt.timestamp())
                except Exception as e:
                    started_ts = None  # 無法解析就設 None
            
            # alias: 將 Trend breakdown 逗號分割成 list
            alias = []
            if pd.notna(row['Trend breakdown']):
                alias = [x.strip() for x in str(row['Trend breakdown']).split(',') if x.strip()]
            
            result.append({
                "keyword": keyword,
                "frequency": freq,
                "started": started_ts,
                "alias": alias
            })
        return result