from WebSearchEngineMetric.RawDataReader.AutoUpdateCSVRawDataReader import AutoUpdateRawDataReader
import random


rawDataReader = AutoUpdateRawDataReader(14, 'trendingRaw')
rawData = rawDataReader.readData()


data1 = random.sample(rawData, 110)
data2 = sorted_data = sorted(rawData, key=lambda x: x["frequency"], reverse=True)[:110]
keys1 = {d['keyword'] for d in data1}
keys2 = {d['keyword'] for d in data2}

# 2. 取交集並計算數量
common_count = len(keys1 & keys2)
common_keys = keys1 & keys2

print(f"重複的 keyword 數量: {common_count}") # 結果: 1
print(f"重複的是: {common_keys}")