from Measure.Measure import Measure
from Dataset.Dataset import Dataset
from tqdm import tqdm
import time
import requests

class CrawlerAllMetricMeasure(Measure):
    def __init__(self, dataset: Dataset, url, resultDataset: Dataset):
        super().__init__()
        self.dataset = dataset
        self.url = url
        self.resultDataset = resultDataset
        self.resultDataset.clear()
    
    def test(self):
        discover_correct = 0
        fetch_correct = 0
        upload_correct = 0
        total = 0
        MAX_RETRIES = 3 # 設定最大重試次數
        RETRY_DELAY = 30 # 設定重試間隔（秒）

        print(f'Start Measuring Crawler, data: {self.dataset.path}')
        pbar = tqdm(total=len(self.dataset.getKeys()))
        for keyword in self.dataset.getKeys():
            for goldenurl in self.dataset.get(keyword)['url']:
                total += 1

                # --- 重試邏輯開始 ---
                response = None
                for attempt in range(MAX_RETRIES):
                    try:
                        # 嘗試發送請求
                        response = requests.get(f'{self.url}/status/url?url={goldenurl}', timeout=60) # 建議增加 timeout
                        
                        # 檢查回應是否成功 (例如: 200)
                        if response.status_code == 200:
                            break # 成功，跳出重試迴圈
                        
                        # 如果狀態碼不是 200，則視為需要重試
                        print(f"URL: {goldenurl} returned status code {response.status_code}. Retrying in {RETRY_DELAY} seconds (Attempt {attempt + 1}/{MAX_RETRIES}).")
                        
                    except requests.exceptions.RequestException as e:
                        # 捕獲網路錯誤、連線超時等 Requests 相關異常
                        print(f"URL: {goldenurl} failed with error: {e}. Retrying in {RETRY_DELAY} seconds (Attempt {attempt + 1}/{MAX_RETRIES}).")
                        
                    # 如果不是最後一次嘗試，則等待
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                    else:
                        print(f"URL: {goldenurl} failed after {MAX_RETRIES} attempts.")
                        response = None # 確保在達到最大重試次數後，response 設為 None 或處理為失敗

                discover_fail = True
                fetch_fail = True
                upload_fail = True
                if response.status_code == 200:
                    data = response.json()
                    if data['in_db']:
                        discover_correct += 1
                        discover_fail = False
                    if data['fetched']:
                        fetch_correct += 1
                        fetch_fail = False
                    if data['uploaded']:
                        upload_correct += 1
                        upload_fail = False
                else:
                    print("Response Status Code: " + response.status_code)

                if self.resultDataset.get(keyword) == None:
                    self.resultDataset.store(keyword, [{'url': goldenurl, 'discover_find': not discover_fail, 'fetch_find': not fetch_fail, 'upload_find': not upload_fail}])
                else:
                    self.resultDataset.get(keyword).append({'url': goldenurl, 'discover_find': not discover_fail, 'fetch_find': not fetch_fail, 'upload_find': not upload_fail})
            pbar.update(1)
        pbar.close()
        print(f'Discover Performance: {discover_correct} / {total}')
        print(f'Fetch Performance: {fetch_correct} / {total}')
        print(f'Upload Performance: {upload_correct} / {total}')
        self.resultDataset.store("__total__", {
            "discover_find": discover_correct,
            "fetch_find": fetch_correct,
            "upload_find": upload_correct,
            "total": total
        })
        self.resultDataset.dump()