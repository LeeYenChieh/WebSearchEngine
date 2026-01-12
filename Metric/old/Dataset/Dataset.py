class Dataset:
    def __init__(self, path):
        self.query: dict = None
        self.path = path
    
    def store(self, key, val):
        if self.query == None:
            self.load()
        
        self.query[key] = val

    def get(self, key):
        if self.query == None:
            self.load()
        result = self.query.get(key)
        # if result == None:
        #     print(f'Can not find key in our dataset')
        return result
    
    def clear(self):
        self.query = {}

    def getKeys(self):
        if self.query == None:
            self.load()
        return self.query.keys()

    def load(self):
        raise NotImplementedError

    def dump(self):
        raise NotImplementedError