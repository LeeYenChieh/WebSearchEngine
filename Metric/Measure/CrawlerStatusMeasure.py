from Measure.Measure import Measure
from Dataset.Dataset import Dataset
from datetime import datetime
import requests

class CrawlerStatusMeasure(Measure):
    def __init__(self, url, resultDataset: Dataset):
        super().__init__()
        self.url = url
        self.resultDataset = resultDataset
    
    def test(self):
        now = datetime.now()
        date = now.strftime('%Y-%m-%d')
        datedata = {}

        print('Start Measuring Status')
        response = requests.get(f'{self.url}/status')
        if response.status_code == 200:
            data = response.json()
            datedata["discovered"] = data["total_urls"]
            datedata["crawled"] = data["fetched_urls"]
            datedata["indexed"] = data["uploaded_urls"]
            
            for daily in data["daily"]:
                old_datedata = self.resultDataset.get(daily["stat_date"])
                if daily["stat_date"] == date:
                    old_datedata = datedata
                if old_datedata == None:
                    old_datedata = {}
                old_datedata["detail"] = daily
                self.resultDataset.store(daily["stat_date"], old_datedata)
            self.resultDataset.store(date, datedata)

        else:
            print("Response Status Code: " + response.status_code)
            datedata["error"] = response
            self.resultDataset.store(date, datedata)
        

        self.resultDataset.dump()