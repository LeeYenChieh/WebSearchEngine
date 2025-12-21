from Metric.Dataset.Dataset import Dataset
from Metric.Dataset.CSVDataset import CSVDataset
from Metric.Dataset.AutoDateCSVDataset import AutoDateCSVDataset
from Metric.Dataset.JsonDataset import JsonDataset
from Metric.Dataset.AutoDateJsonDataset import AutoDateJsonDataset
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
        
