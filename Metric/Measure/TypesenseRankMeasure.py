from Measure.Measure import Measure
from Dataset.Dataset import Dataset
import typesense
from tqdm import tqdm
import os

class TypesenseRankMeasure(Measure):
    def __init__(self, dataset: Dataset, url, resultDataset: Dataset):
        super().__init__()
        self.dataset = dataset
        self.url = url
        self.resultDataset = resultDataset
        self.resultDataset.clear()
    
    def test(self):
        correct = 0
        total = 0
        parts = self.url.split(':', 1)
        client = typesense.Client({
            'nodes': [{
                'host': os.getenv("TYPESENSE_HOST", parts[0]),
                'port': os.getenv("TYPESENSE_PORT", int(parts[1])),
                'protocol': "http",
            }],
            'api_key': os.getenv("TYPESENSE_API_KEY", "apiapiapi"),
            'connection_timeout_seconds': 10
        })

        print(f'Start Measuring Rank, dataset: {self.dataset.path}')
        pbar = tqdm(total=len(self.dataset.getKeys()))

        for keyword in self.dataset.getKeys():
            search_parameters = {
                'q': keyword,             # **核心參數：您要搜尋的關鍵詞 (keyword)**
                'query_by': 'content', # **核心參數：要在哪些欄位中搜尋？**
                'sort_by': '_text_match:desc',       # 可選：按某個欄位排序，例如點閱數降序
                'per_page': 10,                    # 可選：每頁結果數
            }

            search_result = client.collections["webpages"].documents.search(search_parameters)
            for goldenurl in self.dataset.get(keyword)['url']:
                total += 1
                find = False
                for hit in search_result['hits']:
                    document_data = hit['document']['url']
                    if document_data == goldenurl:
                        correct += 1
                        find = True
                        break
                if self.resultDataset.get(keyword) == None:
                    self.resultDataset.store(keyword, [{'url': goldenurl, 'rank_find': find}])
                else:
                    self.resultDataset.get(keyword).append({'url': goldenurl, 'rank_find': find})
            pbar.update(1)
        pbar.close()
        print(f'Rank Performance: {correct} / {total}')
        self.resultDataset.store("__total__", {
            "rank_find": correct,
            "total": total
        })
        self.resultDataset.dump()