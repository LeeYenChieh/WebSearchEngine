from Dataset.JsonDataset import JsonDataset
import json

d = JsonDataset('metricData/random_20251201.json')
d.load()

# 安全排序：沒有 frequency 就視為 0
sorted_data = dict(
    sorted(
        d.query.items(),
        key=lambda item: item[1].get("frequency", 0),
        reverse=True
    )
)

# 寫入排序後的 JSON
with open('sort.json', 'w', encoding='utf-8') as f:
    json.dump(sorted_data, f, ensure_ascii=False, indent=4)
