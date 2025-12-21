import os
from datetime import datetime, timedelta
import pandas as pd
import re
from RawDataReader.RawDataReader import RawDataReader
from serpapi import GoogleSearch

class AutoUpdateCSVRawDataReader(RawDataReader):
    def __init__(self, update_day=14, dirpath='.'):
        super().__init__()
        self.update_day = update_day
        self.dirpath = dirpath
        # 定義要抓取的國家列表
        self.countries = ["US", "IN", "JP", "GB", "CA", "DE", "AU", "BR", "FR", "TW"]
        # self.countries = ["TW"]

    def readData(self) -> list:
        csv_file = os.path.join(self.dirpath, "last_trending.csv")
        need_update = True

        # 1. 判斷是否需要更新 (檢查檔案是否存在 & 是否過期)
        if os.path.exists(csv_file):
            last_modified = datetime.fromtimestamp(os.path.getmtime(csv_file))
            if (datetime.now() - last_modified).days < self.update_day:
                need_update = False

        # 2. 執行更新或讀取舊檔
        if need_update:
            print("Need Fetch Trending Query")
            
            # 如果檔案存在，先進行備份
            if os.path.exists(csv_file):
                backup_name = os.path.join(self.dirpath, f"last_trending_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
                os.rename(csv_file, backup_name)
            
            # 遍歷國家，抓取數據
            all_data = []
            for geo in self.countries:
                trending = self._fetch_trending_now(geo)
                all_data.extend(trending)
            
            # 轉為 DataFrame 進行處理
            df = pd.DataFrame(all_data)

            if not df.empty:
                # --- 資料清洗與聚合 (Dedup) ---
                
                # A. 確保 frequency 是整數，若有空值補 0
                df['frequency'] = pd.to_numeric(df['frequency'], errors='coerce').fillna(0).astype(int)

                # B. 定義 alias 合併邏輯 (去重並用逗號串接)
                def merge_aliases(series):
                    all_tags = []
                    for val in series:
                        if pd.notna(val) and str(val).strip():
                            # 假設 raw data 已經是用逗號分隔的字串，或是單純字串
                            all_tags.extend([x.strip() for x in str(val).split(',') if x.strip()])
                    # 使用 set 去重後再組合成字串
                    return ','.join(list(set(all_tags)))

                # C. GroupBy 聚合：
                # Keyword: 分組依據
                # Frequency: Sum (累加)
                # Started: Min (取最早時間)
                # Alias: Apply merge_aliases (合併)
                df = df.groupby('keyword', as_index=False).agg({
                    'frequency': 'sum',
                    'started': 'min',
                    'alias': merge_aliases
                })

                # D. 依照總熱度排序 (由高到低)
                df = df.sort_values(by='frequency', ascending=False)
            
            # 寫入 CSV
            df.to_csv(csv_file, index=False)
            
        else:
            print(f'Use {csv_file}')
            # 不需要更新，直接讀取
            df = pd.read_csv(csv_file)

        # 3. 將 DataFrame 轉回 List of Dict 格式 (符合 RawDataReader 介面)
        result = []
        for _, row in df.iterrows():
            keyword = row['keyword']

            # 處理 frequency
            freq = int(row['frequency']) if pd.notna(row['frequency']) else 0

            # 處理 started timestamp
            started_ts = int(row['started']) if pd.notna(row['started']) else None

            # 處理 alias (轉回 list)
            alias = []
            if pd.notna(row.get('alias')):
                alias = [x.strip() for x in str(row['alias']).split(',') if x.strip()]

            result.append({
                "keyword": keyword,
                "frequency": freq,
                "started": started_ts,
                "alias": alias
            })

        return result

    def _fetch_trending_now(self, geo: str) -> list:
        """
        從 SerpApi 獲取指定國家的 Google Trends Trending Now 數據
        """
        try:
            params = {
                "engine": "google_trends_trending_now",
                "geo": geo,
                "api_key": os.environ.get('SERPAPI_KEY'),
                "hours": 168  # 過去 7 天
            }

            search = GoogleSearch(params)
            results = search.get_dict()

            # 檢查 API 是否回傳錯誤訊息 (例如 Quota 不足)
            if "error" in results:
                raise Exception(results['error'])
            
            # 獲取主要數據列表
            trending_searches = results.get("trending_searches", [])

            trending_list = []
            for row in trending_searches:
                keyword = row.get("query")
                freq = row.get("search_volume")
                started_ts = row.get("start_timestamp")
                
                # 處理 alias (trend_breakdown)
                alias_data = row.get("trend_breakdown")
                if alias_data is None:
                    alias_str = ""
                else:
                    # 確保轉為字串並用逗號連接
                    alias_str = ",".join([str(x).strip() for x in alias_data if str(x).strip()])
                
                data = {
                    "keyword": keyword,
                    "frequency": freq,
                    "started": started_ts,
                    "alias": alias_str
                }
                trending_list.append(data)
                
            print(f'Fetch {geo} Trending Query Success')
            return trending_list

        except Exception as e:
            # 捕捉所有錯誤 (包含 Quota 不足)，印出 Log 但不中斷整個程式
            # 這樣單一國家失敗不會影響其他國家的數據收集
            error_msg = str(e)
            print(f"Error fetching {geo}: {error_msg}")

            return []