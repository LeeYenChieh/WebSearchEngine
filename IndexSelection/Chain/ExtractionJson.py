from IndexSelection.Chain.Handler import Handler
from IndexSelection.Chain.PipelineResult import PipelineResult
import re
import datetime

class ExtractionJson(Handler):
    """
    Stage 1: 負責提取與清洗
    
    [新增至 data._index_content 的資料]:
    - title (str): 清洗後的標題
    - content (str): 清洗後的內文
    - content_length (int): 內文長度
    - published_at (str): ISO 8601 時間字串
    - entities (list): 簡單提取的實體列表 (模擬 NER)
    - domain_consistent (bool): 檢查 domain 與 canonical 是否一致
    """
    def __init__(self):
        super().__init__()
        self.name = "Extraction"
        self.processors = [
            self._remove_boilerplate,
            self._normalize_date,
            # self._extract_entities,
            # self._check_domain_consistency
        ]

    def canHandle(self, data):
        # 假設 raw content 裡有 type 欄位，或者預設都能處理
        return data._index_content and isinstance(data._index_content, dict)

    def handle(self, data) -> PipelineResult:
        if not self.canHandle(data):
            return super().handle(data)

        try:
            for processor in self.processors:
                processor(data)

        except Exception as e:
            return PipelineResult(success=False, stage=self.name, reason=f"Error: {str(e)}")

        return super().handle(data)

    def _remove_boilerplate(self, data):
        raw_file = data._index_content.get('filecontent', {})
        content = raw_file.get('content', '') or ''
        title = raw_file.get('title', '') or ''
        
        # 簡單清洗
        content = re.sub(r'Copyright ©.*', '', content, flags=re.IGNORECASE).strip()
        if content.startswith(title):
            content = content[len(title):].strip()
            
        data._index_content['title'] = title
        data._index_content['content'] = content
        data._index_content['content_length'] = len(content)

    def _normalize_date(self, data):
        ts = data._index_content.get('filecontent', {}).get('timestamp')
        if ts and isinstance(ts, (int, float)):
            # 假設 timestamp 是毫秒
            data._index_content['published_at'] = datetime.datetime.fromtimestamp(ts / 1000.0).isoformat()
        else:
            data._index_content['published_at'] = datetime.datetime.now().isoformat()

    def _extract_entities(self, data):
        content = data._index_content.get('content', '')
        entities = []
        # 模擬 NER
        target_entities = ['Tesla', 'BMW', 'Apple', 'iPhone', 'Microsoft']
        for ent in target_entities:
            if ent in content:
                entities.append(ent)
        data._index_content['entities'] = list(set(entities))

    def _check_domain_consistency(self, data):
        # 檢查 data.domain 與 meta 中的 canonical 是否衝突
        canonical = data._index_content.get('filecontent', {}).get('meta', {}).get('canonical')
        if canonical and data.domain not in canonical:
            data._index_content['domain_consistent'] = False
        else:
            data._index_content['domain_consistent'] = True