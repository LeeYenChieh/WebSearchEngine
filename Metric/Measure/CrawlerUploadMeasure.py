from Measure.Measure import Measure
from Dataset.Dataset import Dataset
from tqdm import tqdm
import requests

class CrawlerUploadMeasure(Measure):
    def __init__(self, dataset: Dataset, url, resultDataset: Dataset):
        super().__init__()
        self.dataset = dataset
        self.url = url
        self.resultDataset = resultDataset
        self.resultDataset.clear()
    
    def test(self):
        correct = 0
        total = 0
        pbar = tqdm(total=len(self.dataset.getKeys()))
        for keyword in self.dataset.getKeys():
            for goldenurl in self.dataset.get(keyword)['url']:
                total += 1
                response = requests.get(f'{self.url}/status/url?url={goldenurl}')

                fail = True
                if response.status_code == 200:
                    data = response.json()
                    if data['uploaded']:
                        correct += 1
                        fail = False
                else:
                    print("Response Status Code: " + response.status_code)

                if self.resultDataset.get(keyword) == None:
                    self.resultDataset.store(keyword, [{'url': goldenurl, 'find': not fail}])
                else:
                    self.resultDataset.get(keyword).append({'url': goldenurl, 'find': not fail})
            pbar.update(1)
        pbar.close()
        print(f'Performance: {correct} / {total}')
        self.resultDataset.dump()