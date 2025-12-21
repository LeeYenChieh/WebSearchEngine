from Query.QueryStrategy import QueryStrategy
from Dataset.Dataset import Dataset
from tqdm import tqdm


class HeadQueryStrategy(QueryStrategy):
    def __init__(self, dataset, rawData, keywordNums):
        super().__init__(dataset, rawData, keywordNums)
    
    def getGoldenSet(self):
        sorted_data = sorted(self.rawData, key=lambda x: x["frequency"], reverse=True)
        key_num = len(self.dataset.getKeys())

        pbar = tqdm(total=self.keywordNums)
        for s in sorted_data[key_num:key_num + self.keywordNums]:
            key = s['keyword']
            # print("=" * 30)
            # print(f'KeyWord: {key}')

            urlSet = self.getQuery(key)
            result = {
                "frequency": s['frequency'],
                "started": s['started'],
                'url nums': len(urlSet),
                "url": urlSet,
            }
            self.dataset.store(key, result)
            pbar.update(1)
        pbar.close()

            # print(f'Golden URL Nums: {len(urlSet)}')
            # print(f'Golden URL Set: {urlSet}')
            # print("=" * 30)
        self.dataset.dump()