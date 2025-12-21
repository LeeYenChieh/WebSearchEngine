class PipelineResult:
    def __init__(self, success: bool, data=None, stage=None, reason=None):
        self.success = success
        self.data = data
        self.stage = stage
        self.reason = reason

    def __repr__(self):
        if self.success:
            return f"<Success in {self.stage}>"
        else:
            return f"<Dropped in {self.stage}: {self.reason}>"