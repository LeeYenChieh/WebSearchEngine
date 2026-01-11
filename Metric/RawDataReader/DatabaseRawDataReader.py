from RawDataReader.RawDataReader import RawDataReader
from serpapi import GoogleSearch
from datetime import datetime
import time
import os
from collections import defaultdict
from sqlalchemy import desc
from Database.ModelFactory.AppModelFactory import AppModelFactory
from Database.Database import Database

class DatabaseRawDataReader(RawDataReader):
    def __init__(self, db, model_factory, update_day=14):
        super().__init__()
        self.db: Database = db
        self.model_factory: AppModelFactory = model_factory
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
        """
        主邏輯：
        1. 檢查資料庫是否有 update_day 內的 batch。
        2. 如果有 -> 從 DB 讀取並回傳。
        3. 如果沒有 -> 呼叫 API -> 存入 DB -> 回傳。
        """
        MetricBatch = self.model_factory.create_metric_batches()
        
        latest_batch = None
        
        # 1. 檢查資料庫最新紀錄
        with self.db.session() as session:
            latest_batch = session.query(MetricBatch)\
                .order_by(desc(MetricBatch.created_at))\
                .first()
            
            # 如果有找到資料，且資料夠新，就直接用
            if latest_batch:
                delta = datetime.now() - latest_batch.created_at
                if delta.days < self.update_day:
                    print(f'Use cached trending data from DB (Batch ID: {latest_batch.id}, Date: {latest_batch.created_at})')
                    return self._fetch_from_db(latest_batch.id)

        # 2. 資料過期或不存在，重新抓取
        print("Fetch Trending Query from SerpApi...")
        return self._fetch_from_api_and_store()

    def _fetch_from_db(self, batch_id: int) -> list:
        """
        從資料庫讀取指定 Batch 的資料，並轉換回 list of dict 格式
        """
        MetricQuery = self.model_factory.create_metric_queries()
        result_list = []
        
        with self.db.session() as session:
            queries = session.query(MetricQuery).filter(MetricQuery.batch_id == batch_id).all()
            
            for q in queries:
                result_list.append({
                    "keyword": q.keyword,
                    "frequency": q.frequency,
                    "geo": q.geo,  # 這裡是 JSONB (list)
                    # DB 沒存 started timestamp，如果需要可補欄位，這裡暫略
                })
        
        # 依照 frequency 排序 (大到小)
        return sorted(result_list, key=lambda x: x['frequency'], reverse=True)

    def _fetch_from_api_and_store(self) -> list:
        """
        呼叫 API，處理 deduplication，存入 DB，並回傳
        """
        # 1. 獲取所有國家資料
        all_data = []
        for country in self.countries:
            trending = self._fetch_trending_now(country["geo"])
            all_data.extend(trending)
        
        # 2. Dedup & Merge (合併多國家的 geo)
        merged_dict = {}
        for item in all_data:
            kw = item['keyword']
            
            if kw not in merged_dict:
                # 第一次出現，將 string 轉為 list 以便後續 append
                # 例如: 'US' -> ['US']
                item['geo'] = [item['geo']]
                merged_dict[kw] = item
            else:
                # 已經存在，合併邏輯
                if item['geo'] not in merged_dict[kw]['geo']:
                    merged_dict[kw]['geo'].append(item['geo'])
                
                merged_dict[kw]['frequency'] += item['frequency']
                # 取較早的時間
                merged_dict[kw]['started'] = min(merged_dict[kw]['started'], item['started'])

        dedup_data = list(merged_dict.values())
        
        # 3. 排序
        sorted_data = sorted(dedup_data, key=lambda x: x['frequency'], reverse=True)

        # 4. 存入資料庫
        self._save_to_database(sorted_data)
        
        return sorted_data

    def _save_to_database(self, data_list: list):
        """
        將清洗後的資料寫入 MetricBatch 與 MetricQuery
        """
        MetricBatch = self.model_factory.create_metric_batches()
        MetricQuery = self.model_factory.create_metric_queries()

        # 準備 Metadata 統計
        total_keywords = len(data_list)
        geo_counts = defaultdict(int)

        for item in data_list:
            for g in item['geo']: # item['geo'] is a list ["US", "TW"]
                geo_counts[g] += 1

        with self.db.session() as session:
            # A. 建立 Batch
            new_batch = MetricBatch(
                created_at=datetime.now(),
                meta_total_keywords=total_keywords,
                meta_total_urls=0, # 這個階段還沒爬 URL，先填 0
                meta_geo_counts=dict(geo_counts)
            )
            session.add(new_batch)
            session.flush() # 取得 new_batch.id
            
            print(f"Saving new batch to DB. ID: {new_batch.id}, Keywords: {total_keywords}")

            # B. 建立 Queries
            # 這裡可以用 bulk_save_objects 優化效能，但為了安全起見先用 add
            for item in data_list:
                query = MetricQuery(
                    batch_id=new_batch.id,
                    keyword=item['keyword'],
                    geo=item['geo'], # JSONB 欄位直接存 list
                    frequency=item['frequency']
                )
                session.add(query)
            
            session.commit()
            print("Save completed.")

    def _fetch_trending_now(self, geo: str) -> list:
        """
        從 SerpApi 獲取指定國家的 Google Trends Trending Now 數據
        (邏輯保持不變，僅微調回傳格式)
        """
        max_retries = 3
        retry_delay = 30 

        for attempt in range(max_retries + 1):
            try:
                params = {
                    "engine": "google_trends_trending_now",
                    "geo": geo,
                    "api_key": os.environ.get('SERPAPI_KEY'),
                    "hours": 168
                }

                search = GoogleSearch(params)
                results = search.get_dict()

                if "error" in results:
                    raise Exception(results['error'])
                
                trending_searches = results.get("trending_searches", [])
                trending_list = []
                
                for row in trending_searches:
                    keyword = row.get("query")
                    freq = row.get("search_volume") or 0
                    started_ts = row.get("start_timestamp")
                    
                    data = {
                        "keyword": keyword,
                        "frequency": freq,
                        "started": started_ts,
                        "geo": geo # 這裡先維持單一字串，merge 時會轉成 list
                    }
                    trending_list.append(data)
                    
                print(f'Fetch {geo} Trending Query Success ({len(trending_list)} items)')
                return trending_list

            except Exception as e:
                print(f"Error fetching {geo} (Attempt {attempt + 1}): {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    return []
        return []