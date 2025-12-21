from Dataset.Dataset import Dataset
from Query.QueryStrategy import QueryStrategy

class QueryContext:
    def __init__(self):
        self.queryStrategy: QueryStrategy = None

    def setQueryStrategy(self, queryStrategy: QueryStrategy):
        self.queryStrategy = queryStrategy
    
    def getGoldenSet(self):
        self.queryStrategy.getGoldenSet()
