from Metric.Query.QueryStrategy import QueryStrategy
from tqdm import tqdm
import random
from sqlalchemy import func

class RandomQueryStrategy(QueryStrategy):
    def __init__(self, db, modelFactory, batch_id, rawData, keywordNums):
        """
        :param db: Database instance
        :param modelFactory: AppModelFactory instance
        :param batch_id: 當前執行的 MetricBatch ID
        :param rawData: 原始資料列表
        :param keywordNums: 要隨機選取的數量
        """
        # dataset 傳 None
        super().__init__(rawData, keywordNums)
        self.db = db
        self.modelFactory = modelFactory
        self.batch_id = batch_id
    
    def getGoldenSet(self):
        # 1. 隨機採樣
        # 注意：如果 rawData 數量小於 keywordNums，sample 會報錯，需做防呆
        target_num = min(len(self.rawData), self.keywordNums)
        sample_data = random.sample(self.rawData, target_num)

        # 準備 Models
        MetricQuery = self.modelFactory.create_metric_queries()
        MetricURL = self.modelFactory.create_metric_url()
        MetricBatch = self.modelFactory.create_metric_batches()

        print(f"Processing Random Strategy for Batch {self.batch_id} (Target: {target_num})...")
        pbar = tqdm(total=target_num)

        with self.db.session() as session:
            for s in sample_data:
                key = s['keyword']
                
                # A. 呼叫 SerpApi 取得 URL
                url_list = self.getQuery(key)
                
                # B. 處理 MetricQuery (Upsert Logic)
                query_obj = session.query(MetricQuery).filter_by(
                    batch_id=self.batch_id, 
                    keyword=key
                ).first()

                if query_obj:
                    # 情境 1: 關鍵字已存在
                    # 更新 tags：如果 "random" 不在裡面，就加進去
                    current_tags = list(query_obj.tags)
                    if "random" not in current_tags:
                        current_tags.append("random")
                        query_obj.tags = current_tags
                else:
                    # 情境 2: 關鍵字不存在
                    query_obj = MetricQuery(
                        batch_id=self.batch_id,
                        keyword=key,
                        geo=s.get('geo', []),
                        frequency=s['frequency'],
                        tags=["random"] # 標記為 random
                    )
                    session.add(query_obj)
                    session.flush() # 取得 ID
                
                # C. 處理 MetricURL
                # 先清空舊 URL (防重複執行)
                session.query(MetricURL).filter_by(query_id=query_obj.id).delete()

                # 寫入新 URL
                for idx, u in enumerate(url_list):
                    url_obj = MetricURL(
                        query_id=query_obj.id,
                        url=u,
                        rank=idx + 1
                    )
                    session.add(url_obj)

                # Commit 每筆交易
                session.commit()
                pbar.update(1)

            # D. 最後更新 Batch Metadata
            self._update_batch_stats(session, MetricBatch, MetricQuery, MetricURL)
            
        pbar.close()

    def _update_batch_stats(self, session, MetricBatch, MetricQuery, MetricURL):
        """
        重新計算並更新 Batch 的 Metadata
        """
        batch = session.get(MetricBatch, self.batch_id)
        if not batch:
            return

        # 1. 計算 Random Set 的統計數據
        random_q_count = session.query(MetricQuery).filter(
            MetricQuery.batch_id == self.batch_id,
            MetricQuery.tags.contains(["random"])
        ).count()

        random_u_count = session.query(MetricURL).join(MetricQuery).filter(
            MetricQuery.batch_id == self.batch_id,
            MetricQuery.tags.contains(["random"])
        ).count()

        # 2. 更新 meta_tag_stats
        # 必須先讀取現有的 dict，再更新 random 的部分，以免覆蓋掉 head 的數據
        current_stats = dict(batch.meta_tag_stats) if batch.meta_tag_stats else {}
        
        current_stats['random'] = {
            "queries": random_q_count,
            "urls": random_u_count
        }
        batch.meta_tag_stats = current_stats

        # 3. 更新全域總量 (Total Stats)
        total_q = session.query(MetricQuery).filter_by(batch_id=self.batch_id).count()
        total_u = session.query(MetricURL).join(MetricQuery).filter(
            MetricQuery.batch_id == self.batch_id
        ).count()

        batch.meta_total_queries = total_q
        batch.meta_total_urls = total_u

        session.commit()
        print(f"Batch {self.batch_id} Stats Updated: Random Queries={random_q_count}, Random URLs={random_u_count}")