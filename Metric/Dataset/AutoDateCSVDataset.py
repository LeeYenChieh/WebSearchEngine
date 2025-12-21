from Dataset.CSVDataset import CSVDataset
from datetime import datetime
import os

class AutoDateCSVDataset(CSVDataset):
    def __init__(self, path):
        super().__init__(path)
        now = datetime.now()
        base, ext = os.path.splitext(self.path)   # 將 head.csv → ("head", ".csv")
        new_path = f"{base}_{now.strftime('%Y%m%d')}{ext}"
        self.path = new_path