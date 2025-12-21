from Dataset.DatasetFactory import DatasetFactory


dataset = DatasetFactory().getDataset('status.json')
date = "2025-12-03"
date2 = "2025-12-02"
result = {
    "discovered": dataset.get(date)["discovered"] - dataset.get(date)["detail"]["new_links"],
    "crawled": dataset.get(date)["crawled"] - dataset.get(date)["detail"]["fetch_ok"],
    "indexed": dataset.get(date)["indexed"] - dataset.get(date)["detail"]["typesense_ok"],
}

dataset.store(date2, result)
dataset.dump()