from Dataset.Dataset import Dataset
import json
import os

class JsonDataset(Dataset):
    def __init__(self, path):
        super().__init__(path)
    
    def load(self):
        if not os.path.exists(self.path):
            self.query = {}
            return

        with open(self.path, 'r', encoding='utf-8') as f:
            self.query = json.load(f)
    
    def dump(self):
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump(self.query, f, ensure_ascii=False, indent=5)  # indent 可以調整縮排層級

