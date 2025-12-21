from Dataset.JsonDataset import JsonDataset


d1 = JsonDataset("metricData/head_20251201.json")
d2 = JsonDataset("metricData/random_20251201.json")
d1.load()
d2.load()

dict1 = d1.query
dict2 = d2.query

common_keys = dict1.keys() & dict2.keys()

print(f"重複的 Key 數量: {len(common_keys)}") # 結果: 2 (因為 'b' 和 'c' 重複)

result = JsonDataset("Union.json")
for k in common_keys:
    result.store(k, dict1.get(k))
result.dump()