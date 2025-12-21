# Filename: Chain/Ingestion.py
from IndexSelection.Chain.Handler import Handler
from IndexSelection.Chain.PipelineResult import PipelineResult

class Ingestion(Handler):
    """
    Stage 6: 準備寫入資料
    
    [新增至 data._index_content 的資料]:
    - typesense_document (dict): 最終要送給 Typesense 的乾淨字典
    """
    def __init__(self):
        super().__init__()
        self.name = "Ingestion"
        self.processors = [
            self._prepare_typesense_doc
        ]

    def handle(self, data) -> PipelineResult:
        try:
            for processor in self.processors:
                processor(data)
        except Exception as e:
            return PipelineResult(success=False, stage=self.name, reason=f"Ingest Error")

        return super().handle(data)

    def _prepare_typesense_doc(self, data):
        ic = data._index_content
        
        # 組裝最終文件
        document = {
            'id': data.url, # 使用 URL 作為 ID
            'title': ic.get('title'),
            'content': ic.get('content'),
            'url': data.url,
            'domain': data.domain,
            'published_at': ic.get('published_at'), # 這裡應該要是 int64 timestamp 比較好，暫用字串
            'popularity_score': data.index_priority, # 來自 Stage 4
            'inlink_count': data.inlink_count,
            # 'entities': ic.get('entities', []),
            # 'group_id': ic.get('diversity_group') # 來自 Stage 5
        }
        
        # 移除 None 的欄位，Typesense 不喜歡 None
        document = {k: v for k, v in document.items() if v is not None}
        
        data._index_content['typesense_document'] = document