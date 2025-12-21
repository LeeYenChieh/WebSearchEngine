from Metric.Measure.Measure import Measure
from Metric.Dataset.Dataset import Dataset
from Metric.Dataset.DatasetFactory import DatasetFactory

from Metric.Measure.CrawlerAllMetricMeasure import CrawlerAllMetricMeasure
from Metric.Measure.TypesenseRankMeasure import TypesenseRankMeasure
from tqdm import tqdm
import os

class SearchEngineAllMetricMeasure(Measure):
    def __init__(self, dataset: Dataset, db, typesense_url, resultDataset: Dataset):
        super().__init__()
        self.dataset = dataset
        self.db = db
        self.typesense_url = typesense_url
        self.resultDataset = resultDataset
        self.resultDataset.clear()
    
    def test(self):
        crawlerTempDataset = DatasetFactory().getDataset('crawler_temp.json')
        typesenseTempDataset = DatasetFactory().getDataset('typesense_temp.json')
        CrawlerAllMetricMeasure(self.dataset, self.db, crawlerTempDataset).test()
        TypesenseRankMeasure(self.dataset, self.typesense_url, typesenseTempDataset).test()

        os.remove('crawler_temp.json')
        os.remove('typesense_temp.json')
        for keyword in self.dataset.getKeys():
            crawlerdata = crawlerTempDataset.get(keyword)
            typesensedata = typesenseTempDataset.get(keyword)

            result = []
            for idx in range(len(self.dataset.get(keyword)['url'])):
                result.append(crawlerdata[idx] | typesensedata[idx])
            self.resultDataset.store(keyword, result)
        result_total = crawlerTempDataset.get("__total__") | typesenseTempDataset.get("__total__")
        self.resultDataset.store("__total__", result_total)
        self.resultDataset.dump()