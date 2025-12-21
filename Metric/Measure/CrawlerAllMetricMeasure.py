from Metric.Measure.Measure import Measure
from Metric.Dataset.Dataset import Dataset
from Database.Database import Database
from sqlalchemy import text
from tqdm import tqdm
from collections import defaultdict

class CrawlerAllMetricMeasure(Measure):
    def __init__(self, dataset: Dataset, db: Database, resultDataset: Dataset):
        super().__init__()
        self.dataset = dataset
        self.db: Database = db
        self.resultDataset = resultDataset
        self.resultDataset.clear()
    
    def test(self):
        print(f'Start Measuring Crawler (Direct DB), data: {self.dataset.path}')

        # 1. 收集所有的 Golden URL (去重，加速查詢)
        all_golden_urls = set()
        for keyword in self.dataset.getKeys():
            urls = self.dataset.get(keyword).get('url', [])
            for u in urls:
                all_golden_urls.add(u)
        
        # 轉換成 List 以便 SQL 使用
        url_list = list(all_golden_urls)
        total_urls_count = len(url_list)
        
        # 2. 初始化狀態字典
        # key: url, value: {discovered: bool, crawled: bool, indexed: bool, table_id: int}
        url_status_map = {
            u: {'discovered': False, 'crawled': False, 'indexed': False, 'table_id': -1} 
            for u in url_list
        }

        # 3. 遍歷資料庫 (url_state_000 ~ url_state_255)
        # 為了效率，我們不一筆一筆查，而是每張表查一次 "這張表裡有沒有我們的 Golden URL"
        print("Scanning 256 Database Tables...")
        
        # 如果 URL 很多 (例如 > 5000)，建議分批切分 url_list 放入 IN 子句，這裡假設測試集不大直接塞
        
        with self.db.session() as session:
            # 使用 tqdm 顯示掃描進度
            for i in tqdm(range(256), desc="Checking Tables"):
                table_name = f'url_state_{i:03}'
                
                # 建構 SQL: 只撈取是 Golden URL 的資料
                # 注意：這裡使用 text() 執行 Raw SQL 以獲得最高效能，避免 ORM overhead
                sql = text(f"""
                    SELECT url, fetch_ok, indexed 
                    FROM {table_name} 
                    WHERE url IN :urls
                """)
                
                # 執行查詢
                try:
                    result = session.execute(sql, {'urls': tuple(url_list)}).fetchall()
                    
                    # 更新狀態
                    for row in result:
                        url = row[0]
                        fetch_ok = row[1]
                        indexed = row[2]
                        
                        if url in url_status_map:
                            url_status_map[url]['discovered'] = True
                            url_status_map[url]['crawled'] = (fetch_ok > 0)
                            url_status_map[url]['indexed'] = (indexed == 1)
                            url_status_map[url]['table_id'] = i
                            
                except Exception as e:
                    # 有些表可能還沒建立，可以忽略或 print
                    # print(f"Skipping {table_name}: {e}")
                    pass

        # 4. 統計分數 (Global, Group A, Group B)
        stats = {
            'total': {'discover': 0, 'fetch': 0, 'upload': 0},
            'group_a': {'discover': 0, 'fetch': 0, 'upload': 0}, # 000-127
            'group_b': {'discover': 0, 'fetch': 0, 'upload': 0}  # 128-255
        }

        # 用來計算 resultDataset 的邏輯
        for keyword in tqdm(self.dataset.getKeys(), desc="Aggregating Results"):
            keyword_results = []
            
            for goldenurl in self.dataset.get(keyword)['url']:
                status = url_status_map.get(goldenurl, {})
                
                is_discovered = status.get('discovered', False)
                is_crawled = status.get('crawled', False)
                is_indexed = status.get('indexed', False)
                table_id = status.get('table_id', -1)
                
                # 存入 Result Dataset
                keyword_results.append({
                    'url': goldenurl,
                    'discover_find': is_discovered,
                    'fetch_find': is_crawled,
                    'upload_find': is_indexed
                })

                # 更新統計數據
                if is_discovered:
                    # Global
                    stats['total']['discover'] += 1
                    if is_crawled: stats['total']['fetch'] += 1
                    if is_indexed: stats['total']['upload'] += 1
                    
                    # Grouping
                    if 0 <= table_id <= 127:
                        stats['group_a']['discover'] += 1
                        if is_crawled: stats['group_a']['fetch'] += 1
                        if is_indexed: stats['group_a']['upload'] += 1
                    elif 128 <= table_id <= 255:
                        stats['group_b']['discover'] += 1
                        if is_crawled: stats['group_b']['fetch'] += 1
                        if is_indexed: stats['group_b']['upload'] += 1

            # 儲存個別 Keyword 的結果
            if self.resultDataset.get(keyword) is None:
                self.resultDataset.store(keyword, keyword_results)
            else:
                self.resultDataset.get(keyword).extend(keyword_results)

        # 5. 輸出結果
        total_golden_count = sum(len(self.dataset.get(k)['url']) for k in self.dataset.getKeys())
        
        print("\n" + "="*30)
        print(f"📊 Evaluation Report (Total Golden URLs: {total_golden_count})")
        print("="*30)

        # Print Group A (000-127)
        print(f"[Group A (Tables 000-127)] Found:")
        print(f"  - Discovered: {stats['group_a']['discover']}")
        print(f"  - Crawled:    {stats['group_a']['fetch']}")
        print(f"  - Indexed:    {stats['group_a']['upload']}")
        print("-" * 30)

        # Print Group B (128-255)
        print(f"[Group B (Tables 128-255)] Found:")
        print(f"  - Discovered: {stats['group_b']['discover']}")
        print(f"  - Crawled:    {stats['group_b']['fetch']}")
        print(f"  - Indexed:    {stats['group_b']['upload']}")
        print("-" * 30)

        # Print Total
        print(f"[Total Performance]")
        print(f"  - Discovered: {stats['total']['discover']} / {total_golden_count} ({stats['total']['discover']/total_golden_count:.2%})")
        print(f"  - Fetch:      {stats['total']['fetch']} / {total_golden_count} ({stats['total']['fetch']/total_golden_count:.2%})")
        print(f"  - Upload:     {stats['total']['upload']} / {total_golden_count} ({stats['total']['upload']/total_golden_count:.2%})")
        print("="*30)

        # 儲存總結到 JSON
        self.resultDataset.store("__total__", {
            "discover_find": stats['total']['discover'],
            "fetch_find": stats['total']['fetch'],
            "upload_find": stats['total']['upload'],
            "total": total_golden_count,
            "group_a_stats": stats['group_a'],
            "group_b_stats": stats['group_b']
        })
        self.resultDataset.dump()