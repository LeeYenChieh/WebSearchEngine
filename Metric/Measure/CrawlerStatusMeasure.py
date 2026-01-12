from Metric.Measure.Measure import Measure
from Database.Database import Database
from Database.ModelFactory.AppModelFactory import AppModelFactory
from datetime import datetime, timedelta, date
from sqlalchemy import func, case, select, and_
from sqlalchemy.dialects.postgresql import insert
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from collections import defaultdict

class CrawlerStatusMeasure(Measure):
    def __init__(self, modelFactory: AppModelFactory, crawlerDB: Database, metricDB: Database):
        super().__init__()
        self.crawlerDB: Database = crawlerDB
        self.metricDB: Database = metricDB
        self.modelFactory: AppModelFactory = modelFactory
    
    def _scan_shard(self, shard_id):
        """
        掃描單一分片，取得目前的 Snapshot 狀態
        """
        with self.crawlerDB.session() as session:
            try:
                UrlState = self.modelFactory.create_url_state_model(shard_id)
                
                stmt = select(
                    func.count(), # discovered
                    func.count(case((UrlState.fetch_ok > 0, 1))), # crawled
                    func.count(case((UrlState.indexed > 0, 1)))   # indexed
                )
                
                result = session.execute(stmt).one()
                return shard_id, result[0], result[1], result[1]
                
            except Exception as e:
                return shard_id, 0, 0, 0

    def _get_daily_summary_stats(self, target_date: date):
        """
        從 SummaryDaily 計算 1天, 7天, 30天 的統計數據
        回傳 dict: 包含所有 CrawlerStatMixin 需要的 fetch 與 error 欄位
        """
        stats_data = defaultdict(int)
        
        # 定義時間範圍
        start_date_30 = target_date - timedelta(days=29) # 含今天共30天
        
        SummaryDaily = self.modelFactory.create_summary_model()
        
        with self.crawlerDB.session() as session:
            # 一次撈取過去 30 天的資料
            rows = session.query(SummaryDaily).filter(
                and_(SummaryDaily.stat_date >= start_date_30, SummaryDaily.stat_date <= target_date)
            ).all()
            
            # 在記憶體中進行聚合計算 (Python loop)
            # 這樣處理 JSONB 的 key 累加比寫複雜 SQL 容易維護
            for row in rows:
                # 計算該列與目標日期的差距
                delta_days = (target_date - row.stat_date).days
                
                # 基礎數值
                f_ok = row.fetch_ok or 0
                f_fail = row.fetch_fail or 0
                f_total = f_ok + f_fail
                
                # 解析 fail_reasons (JSONB)
                reasons = row.fail_reasons if row.fail_reasons else {}
                err_404 = reasons.get("HttpError 404", 0)
                err_500 = reasons.get("HttpError 500", 0) # 依據你的需求 HttpError 505
                
                # 1. 當日數據 (delta_days == 0)
                if delta_days == 0:
                    stats_data["fetch_ok"] = f_ok
                    stats_data["fetch_fail"] = f_fail
                    stats_data["fetch_total"] = f_total
                    stats_data["http_error_404"] = err_404
                    stats_data["http_error_500"] = err_500

                # 2. 7天滾動數據 (0 <= delta_days < 7)
                if delta_days < 7:
                    stats_data["fetch_ok_7"] += f_ok
                    stats_data["fetch_fail_7"] += f_fail
                    stats_data["fetch_total_7"] += f_total
                    stats_data["http_error_404_7"] += err_404
                    stats_data["http_error_500_7"] += err_500

                # 3. 30天滾動數據 (0 <= delta_days < 30)
                if delta_days < 30:
                    stats_data["fetch_ok_30"] += f_ok
                    stats_data["fetch_fail_30"] += f_fail
                    stats_data["fetch_total_30"] += f_total
                    stats_data["http_error_404_30"] += err_404
                    stats_data["http_error_500_30"] += err_500
                    
        return stats_data

    def test(self):
        # 設定日期 (使用 date 物件)
        now = datetime.now()
        today_date = now.date()
        date_str = now.strftime('%Y-%m-%d')
        
        # 1. 初始化 Snapshot 統計容器
        # 這些是從 url_state 算出來的累計值 (Total Snapshot)
        snapshot_stats = {
            "Total":   {"discovered": 0, "crawled": 0, "indexed": 0},
            "A":       {"discovered": 0, "crawled": 0, "indexed": 0}, # 000-127
            "B":       {"discovered": 0, "crawled": 0, "indexed": 0}  # 128-255
        }

        print(f'🚀 Start Measuring Status - {date_str}')
        
        # 2. [Snapshot] 平行掃描 Shards
        print("   [1/3] Scanning UrlState Shards...")
        MAX_WORKERS = 16
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self._scan_shard, i) for i in range(256)]
            
            for future in tqdm(as_completed(futures), total=256, desc="Scanning Shards"):
                shard_id, disc, crawl, idx = future.result()
                
                # 分類 Team A / Team B
                if 0 <= shard_id <= 127:
                    team_key = "A"
                else:
                    team_key = "B"
                
                # 累加 (Team & Total)
                for key, val in [("discovered", disc), ("crawled", crawl), ("indexed", idx)]:
                    snapshot_stats[team_key][key] += val
                    snapshot_stats["Total"][key] += val

        # 3. [Flow] 計算 SummaryDaily 統計 (Fetch & Errors & Rolling)
        print("   [2/3] Calculating Daily & Rolling Stats...")
        daily_flow_stats = self._get_daily_summary_stats(today_date)
        
        # 4. 寫入 MetricDB
        print(f"   [3/3] Saving to MetricDB...")
        
        suffixes = ["Total", "A", "B"]

        with self.metricDB.session() as session:
            for suffix in suffixes:
                ModelClass = self.modelFactory.create_crawler_stat_model(suffix)
                
                # 準備基礎資料 (來自 Snapshot 掃描)
                row_data = {
                    "stat_date": today_date,
                    "discovered": snapshot_stats[suffix]["discovered"],
                    "crawled":    snapshot_stats[suffix]["crawled"],
                    "indexed":    snapshot_stats[suffix]["indexed"],
                }
                
                # 如果是 Total 表，我們要填入完整的 SummaryDaily 統計數據
                # (因為 SummaryDaily 是全域的，無法拆分 A/B，除非有額外邏輯)
                if suffix == "Total":
                    row_data.update(daily_flow_stats)
                else:
                    # 對於 A 和 B，若無數據來源，則補 0 以防資料庫 Null constraint (如果欄位沒設 default)
                    # 或是維持 None 讓 DB default 運作。
                    # 這裡假設我們要顯式填 0 或是只填 snapshot。
                    # 根據需求：A, B 的 fetch_ok 等欄位若無法計算，通常填 0。
                    keys_to_zero = [
                        "fetch_ok", "fetch_fail", "fetch_total",
                        "fetch_ok_7", "fetch_fail_7", "fetch_total_7",
                        "fetch_ok_30", "fetch_fail_30", "fetch_total_30",
                        "http_error_404", "http_error_404_7", "http_error_404_30",
                        "http_error_500", "http_error_500_7", "http_error_500_30"
                    ]
                    for k in keys_to_zero:
                        row_data[k] = 0

                # 執行 Upsert
                stmt = insert(ModelClass).values(row_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['stat_date'],
                    set_=row_data
                )
                
                session.execute(stmt)
            
            session.commit()

        # 5. 輸出報告
        self._print_report(date_str, snapshot_stats, daily_flow_stats)

    def _print_report(self, date_str, snapshot_stats, flow_stats):
        print("\n" + "="*50)
        print(f"📊 Crawler Status Report: {date_str}")
        print("="*50)
        
        # Snapshot Report
        headers = f"{'Group':<8} | {'Discovered':>12} | {'Crawled':>12} | {'Indexed':>12}"
        print(headers)
        print("-" * len(headers))
        for group in ["A", "B", "Total"]:
            d = snapshot_stats[group]
            print(f"{group:<8} | {d['discovered']:>12,} | {d['crawled']:>12,} | {d['indexed']:>12,}")
            
        print("-" * 50)
        # Flow Report (Total Only)
        print(f"Daily Flow (Total):")
        print(f"  Fetch OK (Today):      {flow_stats['fetch_ok']:,}")
        print(f"  Fetch OK (7 Days):     {flow_stats['fetch_ok_7']:,}")
        print(f"  Fetch OK (30 Days):    {flow_stats['fetch_ok_30']:,}")
        print(f"  404 Errors (7 Days):   {flow_stats['http_error_404_7']:,}")
        print("="*50)