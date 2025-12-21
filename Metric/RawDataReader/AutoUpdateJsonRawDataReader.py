from RawDataReader.RawDataReader import RawDataReader

from Dataset.DatasetFactory import DatasetFactory

from utils.getLastest import get_latest_dataset_file

from serpapi import GoogleSearch
from datetime import datetime
import time
import os


# 用於讀取google trending上的 json data
class AutoUpdateJsonRawDataReader(RawDataReader):
    def __init__(self, path, update_day=14):
        super().__init__()
        self.path = path
        self.update_day = update_day
        self.countries = [
            {"geo": "US", "Country": "United States"},
            {"geo": "IN", "Country": "India"},
            {"geo": "JP", "Country": "Japan"},
            {"geo": "GB", "Country": "United Kingdom"},
            {"geo": "CA", "Country": "Canada"},
            {"geo": "DE", "Country": "Germany"},
            {"geo": "AU", "Country": "Australia"},
            {"geo": "BR", "Country": "Brazil"},
            {"geo": "FR", "Country": "France"},
            {"geo": "TW", "Country": "Taiwan"}
        ]
    
    def readData(self) -> list:
        latestFile = get_latest_dataset_file(self.path, 'trending', '.json')

        delta = None
        if latestFile != None:
            date_str = latestFile.split('_')[-1].split('.')[0]

            # 2. 將字串轉為 datetime 物件
            file_date = datetime.strptime(date_str, "%Y%m%d")

            # 3. 取得現在時間 (並只取日期部分，忽略時分秒，避免誤判)
            current_date = datetime.now()

            # 4. 計算時間差
            delta = current_date - file_date

        dataset = None
        if delta != None and delta.days < self.update_day:
            print('Use old trending')
            dataset = DatasetFactory().getDataset(latestFile)

        else:

            print("Fetch Trending Query")

            # get trending query
            all_data = []
            for country in self.countries:
                trending = self._fetch_trending_now(country["geo"])
                all_data.extend(trending)
                country["query nums"] = len(trending)
            
            # dedup
            merged_dict = {}
            for item in all_data:
                kw = item['keyword']
                
                if kw not in merged_dict:
                    # 如果是第一次出現，複製一份存入字典 (使用 copy 避免改到原始資料)
                    merged_dict[kw] = item.copy()
                else:
                    merged_dict[kw]['geo'] += f', {item["geo"]}'
                    # 如果已經存在，進行合併邏輯
                    merged_dict[kw]['frequency'] += item['frequency']
                    # 取兩者中較早的時間 (最小值)
                    merged_dict[kw]['started'] = min(merged_dict[kw]['started'], item['started'])

            # 最後將字典的 values 轉回 list
            dedup_data = list(merged_dict.values())

            # sort by frequency
            sorted_data = sorted(dedup_data, key=lambda x: x['frequency'], reverse=True)
            dataset = DatasetFactory().getDataset(f'{self.path}/trending.json', True)
            dataset.store("metadata", sorted(self.countries, key=lambda x: x['query nums'], reverse=True))
            dataset.store("data", {
                "nums": len(sorted_data),
                "data": sorted_data
            })
            dataset.dump()
        
        result = dataset.get("data")["data"]
        
        return result
    
    def _fetch_trending_now(self, geo: str) -> list:
        """
        從 SerpApi 獲取指定國家的 Google Trends Trending Now 數據
        包含重試機制：失敗重試 3 次，每次間隔 30 秒
        """
        max_retries = 3
        retry_delay = 30  # 秒

        # 嘗試次數 = 1 (原本) + 3 (重試) = 4 次
        for attempt in range(max_retries + 1):
            try:
                params = {
                    "engine": "google_trends_trending_now",
                    "geo": geo,
                    "api_key": os.environ.get('SERPAPI_KEY'),
                    "hours": 168  # 過去 7 天
                }

                search = GoogleSearch(params)
                results = search.get_dict()

                # 檢查 API 是否回傳錯誤訊息
                if "error" in results:
                    raise Exception(results['error'])
                
                # 獲取主要數據列表
                trending_searches = results.get("trending_searches", [])

                trending_list = []
                for row in trending_searches:
                    keyword = row.get("query")
                    # 防禦性寫法：若無 search_volume 則補 0
                    freq = row.get("search_volume") or 0
                    started_ts = row.get("start_timestamp")
                    
                    data = {
                        "keyword": keyword,
                        "frequency": freq,
                        "started": started_ts,
                        "geo": geo
                    }
                    trending_list.append(data)
                    
                print(f'Fetch {geo} Trending Query Success')
                return trending_list

            except Exception as e:
                error_msg = str(e)
                print(f"Error fetching {geo} (Attempt {attempt + 1}/{max_retries + 1}): {error_msg}")

                # 如果還不是最後一次嘗試，就等待並重試
                if attempt < max_retries:
                    print(f"Waiting {retry_delay} seconds before retrying...")
                    time.sleep(retry_delay)
                else:
                    # 如果是最後一次嘗試依然失敗
                    print(f"All {max_retries + 1} attempts failed for {geo}. Giving up.")
                    return []