from Metric.Measure.Measure import Measure
from Database.Database import Database
from Database.ModelFactory.AppModelFactory import AppModelFactory
from datetime import datetime
from sqlalchemy import func, case, select
from sqlalchemy.dialects.postgresql import insert  # 關鍵：引入 PostgreSQL 的 Insert
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

class CrawlerStatusMeasure(Measure):
    def __init__(self, modelFactory: AppModelFactory, crawlerDB: Database, metricDB: Database):
        super().__init__()
        self.crawlerDB: Database = crawlerDB
        self.metricDB: Database = metricDB
        self.modelFactory: AppModelFactory = modelFactory
    
    def _scan_shard(self, shard_id):
        # 這裡使用 crawlerDB 的 session
        with self.crawlerDB.session() as session:
            try:
                # 假設 factory 也能產生 Shard Model
                UrlState = self.modelFactory.create_url_state_model(shard_id)
                
                stmt = select(
                    func.count(),
                    func.count(case((UrlState.fetch_ok > 0, 1))),
                    func.count(case((UrlState.indexed > 0, 1)))
                )
                
                result = session.execute(stmt).one()
                return shard_id, result[0], result[1], result[2]
                
            except Exception as e:
                # 錯誤處理：回傳 0 避免影響整體統計
                return shard_id, 0, 0, 0
    
    def test(self):
        now = datetime.now()
        date_str = now.strftime('%Y-%m-%d')
        
        # 1. 初始化統計容器
        stats = {
            "total":   {"discovered": 0, "crawled": 0, "indexed": 0},
            "team_a":  {"discovered": 0, "crawled": 0, "indexed": 0},
            "team_b":  {"discovered": 0, "crawled": 0, "indexed": 0}
        }

        print(f'🚀 Start Measuring Status (Parallel) - {date_str}')
        
        # 2. 平行掃描 Shards
        MAX_WORKERS = 16
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self._scan_shard, i) for i in range(256)]
            
            for future in tqdm(as_completed(futures), total=256, desc="Scanning"):
                shard_id, disc, crawl, idx = future.result()
                
                # 分類
                if 0 <= shard_id <= 127:
                    team_key = "team_a"
                else:
                    team_key = "team_b"
                
                # 累加
                for key, val in [("discovered", disc), ("crawled", crawl), ("indexed", idx)]:
                    stats[team_key][key] += val
                    stats["total"][key] += val

        # 3. 寫入 MetricDB (關鍵修改部分)
        print(f"💾 Saving to MetricDB...")
        
        # 定義映射關係：stats key -> Factory suffix
        mapping = [
            ("total", "Total"),
            ("team_a", "A"),
            ("team_b", "B")
        ]

        with self.metricDB.session() as session:
            for stats_key, suffix in mapping:
                # A. 透過 Factory 取得對應的 Class (例如 CrawlerStatA)
                # 注意：這裡依賴先前定義的 AppModelFactory.create_crawler_stat
                ModelClass = self.modelFactory.create_crawler_stat_model(suffix)
                
                # B. 準備要寫入的資料
                row_data = {
                    "stat_date": date_str,
                    "discovered": stats[stats_key]["discovered"],
                    "crawled":    stats[stats_key]["crawled"],
                    "indexed":    stats[stats_key]["crawled"] # temp
                }
                
                # C. 執行 Upsert (Insert on Conflict Update)
                # 這樣做的好處是：如果該日期的 fetch_ok 已經被其他程式寫入了，
                # 這段程式碼只會「更新」 discovered/crawled/indexed，不會蓋掉 fetch_ok。
                stmt = insert(ModelClass).values(row_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['stat_date'], # 依據主鍵判斷衝突
                    set_=row_data                 # 衝突時更新這些欄位
                )
                
                session.execute(stmt)
            
            # 一次提交所有變更
            session.commit()

        # 4. 輸出報告
        self._print_report(date_str, stats)

    def _print_report(self, date_str, stats):
        print("\n" + "="*35)
        print(f"📊 Crawler Status Report: {date_str}")
        print("="*35)
        # 簡單的對齊格式化
        headers = f"{'Group':<8} | {'Discovered':>12} | {'Crawled':>12} | {'Indexed':>12}"
        print(headers)
        print("-" * len(headers))
        
        for group in ["team_a", "team_b", "total"]:
            d = stats[group]
            print(f"{group.upper():<8} | {d['discovered']:>12,} | {d['crawled']:>12,} | {d['indexed']:>12,}")
        print("="*35)