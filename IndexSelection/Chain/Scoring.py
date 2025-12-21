# Filename: Chain/Scoring.py
from IndexSelection.Chain.Handler import Handler
from IndexSelection.Chain.PipelineResult import PipelineResult
import math

class Scoring(Handler):
    """
    Stage 4: 多因子評分
    
    [新增至 data._index_content 的資料]:
    - score_breakdown (dict): 分數組成的詳細數據 (Debug用)
    
    [更新 data (UrlStateMixin) 的欄位]:
    - index_priority: 最終計算出的分數
    """
    def __init__(self):
        super().__init__()
        self.name = "Scoring"
        self.processors = [
            self._calculate_hybrid_score
        ]

    def canHandle(self, data):
        # 確保 UrlStateMixin 的欄位存在
        return hasattr(data, 'inlink_count') and hasattr(data, 'domain_score')

    def handle(self, data) -> PipelineResult:
        try:
            for processor in self.processors:
                processor(data)
        except Exception as e:
            return PipelineResult(success=False, stage=self.name, reason=f"Error: {str(e)}")

        return super().handle(data)

    def _calculate_hybrid_score(self, data):
        # 1. 提取特徵
        inlinks = data.inlink_count if data.inlink_count else 0
        domain_score = data.domain_score if data.domain_score else 0.0
        content_len = data._index_content.get('content_length', 0)
        
        # 2. 歸一化與計算
        # Inlinks 取 Log (避免大站獨大)
        link_score = math.log(1 + inlinks)
        
        # Content Quality (簡單模擬: 長度越長分數越高，上限 1.0)
        quality_score = min(content_len / 3000.0, 1.0)
        
        # 權重設定
        w_link = 0.4
        w_domain = 0.3
        w_content = 0.3
        
        final_score = (link_score * w_link) + (domain_score * w_domain) + (quality_score * w_content)
        
        # 3. 更新 SQL Model 欄位
        data.index_priority = round(final_score, 4)
        
        # 4. 紀錄詳細資訊供 Debug
        data._index_content['score_breakdown'] = {
            'link_score_raw': link_score,
            'quality_score_raw': quality_score,
            'domain_score_raw': domain_score,
            'final': data.index_priority
        }