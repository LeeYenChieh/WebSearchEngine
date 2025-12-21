from IndexSelection.Chain.Handler import Handler
from IndexSelection.Chain.PipelineResult import PipelineResult
import json

class ContentRead(Handler):
    """
    新增
    filecontent: Content in file,
    type: html or json
    in data._index_content
    """
    def __init__(self):
        super().__init__()
        self.name = "readcontent"
        # 定義這個 Stage 內部的 "微產線" (Micro-Pipeline)
        self.processors = [
            self._readData
        ]

    def canHandle(self, data):
        return True

    def handle(self, data) -> PipelineResult:
        if not self.canHandle(data):
            return super().handle(data)

        try:
            for processor in self.processors:
                processor(data)

        except Exception as e:
            return PipelineResult(success=False, stage=self.name, reason=f"read content error")

        return super().handle(data)

    def _readData(self, data):
        data._index_content = {}
        if data.content_path.lower().endswith('.json'):
            data._index_content['type'] = 'json'
            with open(data.content_path, 'r', encoding='utf-8') as f:
                data._index_content['filecontent'] = json.load(f)
            