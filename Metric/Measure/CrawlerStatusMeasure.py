from Metric.Measure.Measure import Measure
from Metric.Dataset.Dataset import Dataset
from Database.Database import Database
from models import create_url_state_model
from datetime import datetime
import requests

class CrawlerStatusMeasure(Measure):
    def __init__(self, db, resultDataset: Dataset):
        super().__init__()
        self.db: Database = db
        self.resultDataset = resultDataset
    
    def test(self):
        now = datetime.now()
        date = now.strftime('%Y-%m-%d')
        datedata = {
            "discovered": 0,
            "crawled": 0,
            "indexed": 0
        }

        print('Start Measuring Status')
        with self.db.session() as s:
            for i in range(256):
                table_name = f'url_state_{i:03}'
                UrlState = create_url_state_model(table_name)

                datedata["discovered"] += s.query(UrlState).count()
                datedata["crawled"] += s.query(UrlState).filter(UrlState.fetch_ok > 0).count()
                datedata["indexed"] += s.query(UrlState).filter(UrlState.indexed > 0).count()

            self.resultDataset.store(date, datedata)
        self.resultDataset.dump()