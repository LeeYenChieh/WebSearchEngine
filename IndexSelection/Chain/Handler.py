from IndexSelection.Chain.PipelineResult import PipelineResult

class Handler:
    def __init__(self):
        self.next: Handler = None
        # 方便 Debug，自動取得 class 名稱 (e.g., "ExtractionJson")
        self.name = self.__class__.__name__ 
    
    def setNext(self, h):
        self.next = h
        return h
    
    def handle(self, data) -> PipelineResult:
        if self.next:
            return self.next.handle(data)
        return PipelineResult(success=True, data=data, stage="indexed")
    
    def canHandle(self, data):
        return True