from Dataset.Dataset import Dataset
from Dataset.CSVDataset import CSVDataset
from Dataset.AutoDateCSVDataset import AutoDateCSVDataset
from Dataset.JsonDataset import JsonDataset
from Dataset.AutoDateJsonDataset import AutoDateJsonDataset
import os

class DatasetFactory:
    def __init__(self):
        pass

    def getDataset(self, path, autoDate=False) -> Dataset:
        if os.path.splitext(path)[1] == '.csv':
            if not autoDate:
                return CSVDataset(path)
            else:
                return AutoDateCSVDataset(path)
        elif os.path.splitext(path)[1] == '.json':
            if not autoDate:
                return JsonDataset(path)
            else:
                return AutoDateJsonDataset(path)
        
