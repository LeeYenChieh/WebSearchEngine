from Measure.Measure import Measure

class MeasureContext:
    def __init__(self):
        self.measure: Measure = None
    
    def setMeasure(self, m: Measure):
        self.measure = m

    def test(self):
        self.measure.test()