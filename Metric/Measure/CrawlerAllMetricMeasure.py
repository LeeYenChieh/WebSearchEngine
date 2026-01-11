from Metric.Measure.Measure import Measure
from Metric.Dataset.Dataset import Dataset
from Database.Database import Database
from Database.ModelFactory.AppModelFactory import AppModelFactory
from sqlalchemy import text, select
from tqdm import tqdm
import tldextract
from concurrent.futures import ThreadPoolExecutor, as_completed

class CrawlerAllMetricMeasure(Measure):
    def __init__(self, modelFactory: AppModelFactory, dataset: Dataset, crawlerDB: Database, metricDB: Database, resultDataset: Dataset):
        super().__init__()
        self.dataset = dataset
        self.modelFactory = modelFactory
        self.crawlerDB: Database = crawlerDB
        self.metricDB: Database = metricDB
        self.resultDataset = resultDataset
        self.resultDataset.clear()
        self.extractor = tldextract.TLDExtract(cache_dir=False)
    
    def get_domain(self, url: str) -> str:
        try:
            extracted = self.extractor(url)
            if extracted.domain and extracted.suffix:
                return f"{extracted.domain}.{extracted.suffix}"
            return ""
        except:
            return ""

    def _scan_domain_shard(self, shard_ids, domain_tuple):
        found_map = {}
        with self.crawlerDB.session() as session:
            for i in shard_ids:
                try:
                    # 1. 動態取得該分表的 ORM Model Class
                    DomainStats = self.modelFactory.create_domain_stats_model(i)

                    # 2. 建構 ORM 查詢
                    # 對應 SQL: SELECT domain FROM {table_name} WHERE domain IN :domains
                    stmt = select(DomainStats.domain).where(DomainStats.domain.in_(domain_tuple))

                    # 3. 執行並取得結果
                    # session.execute(stmt) 回傳的是 Result Proxy
                    # .scalars() 會自動把每一列的第一個欄位取出來，變成一個 iterator
                    result = session.execute(stmt).scalars()

                    for domain in result:
                        found_map[domain] = i
                        
                except Exception:
                    # 忽略表不存在或其他錯誤
                    pass
                    
        return found_map

    def _scan_url_shard(self, shard_ids, url_tuple):
        found_data = [] # List of (url, fetch_ok, indexed, table_id)
        
        with self.crawlerDB.session() as session:
            for i in shard_ids:
                try:
                    # 1. 動態建立 ORM Model
                    UrlState = self.modelFactory.create_url_state_model(i)

                    # 2. 建構查詢語句
                    # 對應 SQL: SELECT url, fetch_ok, indexed FROM table WHERE url IN (...)
                    stmt = select(
                        UrlState.url, 
                        UrlState.fetch_ok, 
                        UrlState.indexed
                    ).where(UrlState.url.in_(url_tuple))

                    # 3. 執行並取得結果
                    # result 是一組 Row 物件
                    result = session.execute(stmt)

                    for row in result:
                        # row 屬性會對應 select 中的欄位順序或名稱
                        # 格式: (url, fetch_ok, indexed, shard_id)
                        found_data.append((row.url, row.fetch_ok, row.indexed, i))
                        
                except Exception:
                    # 忽略表不存在或其他錯誤
                    pass
                    
        return found_data

    def test(self):
        print(f'Start Measuring Crawler (Direct DB - Parallel), data: {self.dataset.path}')

        # 1. 收集資料
        all_golden_urls = set()
        all_golden_domains = set()
        url_to_domain = {}

        for keyword in self.dataset.getKeys():
            urls = self.dataset.get(keyword).get('url', [])
            for u in urls:
                all_golden_urls.add(u)
                domain = self.get_domain(u)
                url_to_domain[u] = domain
                if domain:
                    all_golden_domains.add(domain)
        
        url_list = list(all_golden_urls)
        domain_list = list(all_golden_domains)
        domain_tuple = tuple(domain_list) # 轉成 tuple 供 SQL 使用
        url_tuple = tuple(url_list)

        # 定義並行參數
        MAX_WORKERS = 16  # 設定執行緒數量，建議 8~16 之間，視 DB 連線池大小而定
        SHARDS = range(256)
        
        # 將 256 個表平均分配給 Worker，減少 Context Switch
        chunk_size = 256 // MAX_WORKERS + 1
        shard_chunks = [range(i, min(i + chunk_size, 256)) for i in range(0, 256, chunk_size)]

        # 2. 並行掃描 Domain Tables
        domain_shard_map = {}
        print(f"Mapping {len(domain_list)} Domains using {MAX_WORKERS} threads...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交任務
            futures = [executor.submit(self._scan_domain_shard, chunk, domain_tuple) for chunk in shard_chunks]
            
            # 收集結果
            for future in tqdm(as_completed(futures), total=len(futures), desc="Scanning Domains"):
                result_map = future.result()
                domain_shard_map.update(result_map)

        # 3. 初始化 URL 狀態
        url_status_map = {
            u: {'discovered': False, 'crawled': False, 'indexed': False, 'table_id': -1} 
            for u in url_list
        }

        # 4. 並行掃描 URL Tables
        print(f"Scanning 256 URL Tables using {MAX_WORKERS} threads...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self._scan_url_shard, chunk, url_tuple) for chunk in shard_chunks]
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Scanning URLs"):
                results = future.result()
                for (url, fetch_ok, indexed, shard_id) in results:
                    if url in url_status_map:
                        url_status_map[url]['discovered'] = True
                        url_status_map[url]['crawled'] = (fetch_ok > 0)
                        url_status_map[url]['indexed'] = (indexed == 1)
                        url_status_map[url]['table_id'] = shard_id

        # 5. 統計與聚合 (邏輯保持不變)
        stats = {
            'total':   {'count': 0, 'discover': 0, 'fetch': 0, 'upload': 0},
            'team_a':  {'count': 0, 'discover': 0, 'fetch': 0, 'upload': 0}, 
            'team_b':  {'count': 0, 'discover': 0, 'fetch': 0, 'upload': 0},
            'unknown': {'count': 0, 'discover': 0, 'fetch': 0, 'upload': 0}
        }

        example_unknown = None 
        example_team_a = None

        for keyword in tqdm(self.dataset.getKeys(), desc="Aggregating Results"):
            keyword_results = []
            
            for goldenurl in self.dataset.get(keyword)['url']:
                status = url_status_map.get(goldenurl, {})
                domain = url_to_domain.get(goldenurl, "")
                
                table_id = status.get('table_id', -1)
                # 如果 URL 沒找到表，嘗試用 Domain 找表
                if table_id == -1:
                    table_id = domain_shard_map.get(domain, -1)

                team_key = 'unknown'
                if 0 <= table_id <= 127:
                    team_key = 'team_a'
                elif 128 <= table_id <= 255:
                    team_key = 'team_b'
                
                if team_key == 'unknown' and example_unknown is None:
                    example_unknown = (goldenurl, domain)
                
                if team_key == 'team_a' and example_team_a is None:
                    example_team_a = (goldenurl, domain)

                is_discovered = status.get('discovered', False)
                is_crawled = status.get('crawled', False)
                is_indexed = status.get('indexed', False)
                
                stats['total']['count'] += 1
                stats[team_key]['count'] += 1

                if is_discovered:
                    stats['total']['discover'] += 1
                    stats[team_key]['discover'] += 1
                    if is_crawled: 
                        stats['total']['fetch'] += 1
                        stats[team_key]['fetch'] += 1
                    if is_indexed: 
                        stats['total']['upload'] += 1
                        stats[team_key]['upload'] += 1

                keyword_results.append({
                    'url': goldenurl,
                    'team': team_key,
                    'discover_find': is_discovered,
                    'fetch_find': is_crawled,
                    'upload_find': is_indexed
                })

            if self.resultDataset.get(keyword) is None:
                self.resultDataset.store(keyword, keyword_results)
            else:
                self.resultDataset.get(keyword).extend(keyword_results)

        # 6. 輸出報告 (保持原樣)
        self._print_report(stats, example_team_a, example_unknown)
        self.resultDataset.store("__total__", stats)
        self.resultDataset.dump()

    def _print_report(self, stats, example_team_a, example_unknown):
        def print_stat(name, data):
            total = data['count']
            if total == 0:
                print(f"[{name}] No Golden URLs found in this group.")
                return
            print(f"[{name}] (Total: {total})")
            print(f"  - Discovered: {data['discover']} ({data['discover']/total:.2%})")
            print(f"  - Crawled:    {data['fetch']} ({data['fetch']/total:.2%})")
            print(f"  - Indexed:    {data['upload']} ({data['upload']/total:.2%})")
            print("-" * 30)

        print("\n" + "="*40)
        print(f"📊 Evaluation Report")
        print("="*40)
        print_stat("Team A (Shards 000-127)", stats['team_a'])
        if example_team_a:
            print(f"  🟢 Example Team A: {example_team_a[0]} ({example_team_a[1]})")
            print("-" * 30)

        print_stat("Team B (Shards 128-255)", stats['team_b'])
        
        if stats['unknown']['count'] > 0:
            print_stat("Unknown Team (Domain not in DB)", stats['unknown'])
            if example_unknown:
                print(f"  🔴 Example Unknown: {example_unknown[0]} ({example_unknown[1]})")
                print("-" * 30)

        print(f"[Total Performance] (Total: {stats['total']['count']})")
        # 避免除以零錯誤
        total = stats['total']['count'] if stats['total']['count'] > 0 else 1
        print(f"  - Discovered: {stats['total']['discover']} ({stats['total']['discover']/total:.2%})")
        print(f"  - Fetch:      {stats['total']['fetch']} ({stats['total']['fetch']/total:.2%})")
        print(f"  - Upload:     {stats['total']['upload']} ({stats['total']['upload']/total:.2%})")
        print("="*40)