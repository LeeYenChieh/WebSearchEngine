from Dataset.Dataset import Dataset
import pandas as pd
from datetime import datetime
import os

class CSVDataset(Dataset):
    def __init__(self, path):
        super().__init__(path)
    
    def load(self):
        if not os.path.exists(self.path):
            self.query = {}
            return

        df = pd.read_csv(self.path)
        result = {}
        for _, row in df.iterrows():
            urls_str = row.get("urls")

            if pd.isna(urls_str) or urls_str == "":
                urls = []
            else:
                urls = [u.strip() for u in urls_str.split(",") if u.strip()]

            result[row["keyword"]] = urls

        self.query = result
    
    def dump(self):
        df = pd.DataFrame({
            "keyword": list(self.query.keys()),
            "urls": [",".join(v) for v in self.query.values()]
        })
        df.to_csv(self.path, index=False)

