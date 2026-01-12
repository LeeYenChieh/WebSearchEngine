from Metric.Query.QueryStrategy import QueryStrategy
from tqdm import tqdm
from sqlalchemy import func

class HeadQueryStrategy(QueryStrategy):
    def __init__(self, db, modelFactory, batch_id, rawData, keywordNums):
        """
        :param db: Database instance
        :param modelFactory: AppModelFactory instance
        :param batch_id: 當前執行的 MetricBatch ID
        :param rawData: 原始資料列表 (通常來自 RawDataReader.readData())
        :param keywordNums: 要選取前幾名
        """
        super().__init__(rawData, keywordNums)
        self.db = db
        self.modelFactory = modelFactory
        self.batch_id = batch_id
    
    def getGoldenSet(self):
        # 1. 根據 Frequency 排序並取出前 N 名
        sorted_data = sorted(self.rawData, key=lambda x: x["frequency"], reverse=True)
        target_data = sorted_data[:self.keywordNums]

        # 準備 Models
        MetricQuery = self.modelFactory.create_metric_queries()
        MetricURL = self.modelFactory.create_metric_url()
        MetricBatch = self.modelFactory.create_metric_batches()

        print(f"Processing Head Strategy for Batch {self.batch_id}...")
        pbar = tqdm(total=len(target_data))

        with self.db.session() as session:
            for s in target_data:
                key = s['keyword']
                
                # A. 呼叫 SerpApi 取得 URL (使用父類別的方法)
                # 注意：這裡會消耗 API 額度與時間
                url_list = self.getQuery(key)
                
                # B. 處理 MetricQuery (Upsert Logic)
                # 先檢查這個關鍵字在這個 Batch 是否已經存在 (例如由 Trending 匯入過)
                query_obj = session.query(MetricQuery).filter_by(
                    batch_id=self.batch_id, 
                    keyword=key
                ).first()

                if query_obj:
                    # 情境 1: 關鍵字已存在 (例如它是 Trending 關鍵字)
                    # 更新 tags：如果 "head" 不在裡面，就加進去
                    current_tags = list(query_obj.tags) # 複製 list
                    if "head" not in current_tags:
                        current_tags.append("head")
                        query_obj.tags = current_tags # 觸發 SQLAlchemy 更新
                else:
                    # 情境 2: 關鍵字不存在，建立新的
                    query_obj = MetricQuery(
                        batch_id=self.batch_id,
                        keyword=key,
                        geo=s.get('geo', []), # 繼承原始資料的 geo
                        frequency=s['frequency'],
                        tags=["head"] # 標記為 head
                    )
                    session.add(query_obj)
                    session.flush() # 取得 ID
                
                # C. 處理 MetricURL
                # 為了避免重跑時重複插入，先刪除該 Query 舊的 URL (如果有)
                session.query(MetricURL).filter_by(query_id=query_obj.id).delete()

                # 寫入新的 URL
                for idx, u in enumerate(url_list):
                    url_obj = MetricURL(
                        query_id=query_obj.id,
                        url=u,
                        rank=idx + 1
                    )
                    session.add(url_obj)

                # 每一筆 Commit 一次，避免長時間佔用 Transaction 或 API 中斷導致全部回滾
                session.commit()
                pbar.update(1)

            # D. 最後更新 Batch Metadata (統計數據)
            self._update_batch_stats(session, MetricBatch, MetricQuery, MetricURL)
            
        pbar.close()

    def _update_batch_stats(self, session, MetricBatch, MetricQuery, MetricURL):
        """
        輔助函式：重新計算並更新 Batch 的 Metadata
        """
        batch = session.get(MetricBatch, self.batch_id)
        if not batch:
            return

        # 1. 計算 Head Set 的統計數據
        # Head Queries 數量
        head_q_count = session.query(MetricQuery).filter(
            MetricQuery.batch_id == self.batch_id,
            MetricQuery.tags.contains(["head"])
        ).count()

        # Head URLs 數量 (透過 Join 計算)
        head_u_count = session.query(MetricURL).join(MetricQuery).filter(
            MetricQuery.batch_id == self.batch_id,
            MetricQuery.tags.contains(["head"])
        ).count()

        # 2. 更新 meta_tag_stats
        # 先取出舊的 dict (如果有的話)
        current_stats = dict(batch.meta_tag_stats) if batch.meta_tag_stats else {}
        current_stats['head'] = {
            "queries": head_q_count,
            "urls": head_u_count
        }
        batch.meta_tag_stats = current_stats

        # 3. 更新全域總量 (Total Stats)
        # 這裡直接重算整個 Batch 的總量，確保數據一致性
        total_q = session.query(MetricQuery).filter_by(batch_id=self.batch_id).count()
        total_u = session.query(MetricURL).join(MetricQuery).filter(
            MetricQuery.batch_id == self.batch_id
        ).count()

        batch.meta_total_queries = total_q
        batch.meta_total_urls = total_u

        session.commit()
        print(f"Batch {self.batch_id} Stats Updated: Head Queries={head_q_count}, Head URLs={head_u_count}")