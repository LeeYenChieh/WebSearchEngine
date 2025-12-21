from Metric.Dataset.Dataset import Dataset
from Metric.Dataset.DatasetFactory import DatasetFactory

from Metric.RawDataReader.RawDataReader import RawDataReader
from Metric.RawDataReader.CSVFileRawDataReader import CSVFileRawDataReader
from Metric.RawDataReader.AutoUpdateJsonRawDataReader import AutoUpdateJsonRawDataReader
from Metric.RawDataReader.AutoUpdateCSVRawDataReader import AutoUpdateCSVRawDataReader

from Metric.Query.QueryContext import QueryContext
from Metric.Query.RandomQueryStrategy import RandomQueryStrategy
from Metric.Query.HeadQueryStrategy import HeadQueryStrategy

from Metric.Measure.MeasureContext import MeasureContext
from Metric.Measure.CrawlerDiscoverMeasure import CrawlerDiscoverMeasure
from Metric.Measure.CrawlerFetchMeasure import CrawlerFetchMeasure
from Metric.Measure.CrawlerUploadMeasure import CrawlerUploadMeasure
from Metric.Measure.TypesenseRankMeasure import TypesenseRankMeasure
from Metric.Measure.CrawlerAllMetricMeasure import CrawlerAllMetricMeasure
from Metric.Measure.SearchEngineAllMetricMeasure import SearchEngineAllMetricMeasure
from Metric.Measure.CrawlerStatusMeasure import CrawlerStatusMeasure

from Metric.utils.getLastest import get_latest_dataset_file

from argparse import ArgumentParser

import os

def parseArgs():
    parser = ArgumentParser()

    parser.add_argument("--datadir", help="Metric data dir path")
    parser.add_argument("--strategy", nargs='+', choices=['random', 'head'], default=[], help="raw data path")
    parser.add_argument("--measure", nargs='+', choices=['status', 'discover', 'fetch', 'upload', 'rank', 'crawler_all', 'all'], help="raw data path")

    parser.add_argument("--create", action='store_true', help="create dataset")
    parser.add_argument("--rawdatareader", choices=['csvfile', 'json', 'csv'], help="raw data reader strategy")
    parser.add_argument("--rawdatapath", help="Raw data path")
    parser.add_argument("--rawdatadir", default='.', help="Auto generator raw data dir")
    parser.add_argument("--update", type=int, default=14, help="auto generator days")
    parser.add_argument("--keywordNums", type=int, default=100, help="Metric Data Keyword Nums")

    parser.add_argument("--test", action='store_true', help="test performance")
    parser.add_argument("--crawler_url", help="crawler url")
    parser.add_argument("--typesense_url", help="typesense url")
    parser.add_argument("--resultdir", help="Result Dir")

    args = parser.parse_args()
    return args

def createDataset(args):
    rawDataReader: RawDataReader = None
    if args.rawdatareader == "csvfile":
        rawDataReader = CSVFileRawDataReader(args.rawdatapath)
    elif args.rawdatareader == "json":
        rawDataReader = AutoUpdateJsonRawDataReader(args.rawdatadir, args.update)
    elif args.rawdatareader == "csvs":
        rawDataReader = AutoUpdateCSVRawDataReader(args.update, args.rawdatadir)
    rawData = rawDataReader.readData()

    context: QueryContext = QueryContext()

    if 'random' in args.strategy:
        dataset: Dataset  = DatasetFactory().getDataset(f'{args.datadir}/random.json', True)
        context.setQueryStrategy(RandomQueryStrategy(dataset, rawData, args.keywordNums))
        context.getGoldenSet()
    if 'head' in args.strategy:
        dataset: Dataset  = DatasetFactory().getDataset(f'{args.datadir}/head.json', True)
        context.setQueryStrategy(HeadQueryStrategy(dataset, rawData, args.keywordNums))
        context.getGoldenSet()

def test(args):
    dataset: list  = []
    resultDataset: list  = []
    context: MeasureContext = MeasureContext()

    if 'random' in args.strategy:
        dataset.append(DatasetFactory().getDataset(get_latest_dataset_file(args.datadir, 'random', '.json')))
        resultDataset.append({
            "discover": DatasetFactory().getDataset(f'{args.resultdir}/random_discover.json', True),
            "fetch": DatasetFactory().getDataset(f'{args.resultdir}/random_fetch.json', True),
            "upload": DatasetFactory().getDataset(f'{args.resultdir}/randomd_upload.json', True),
            "rank": DatasetFactory().getDataset(f'{args.resultdir}/random_rank.json', True),
            "crawler_all": DatasetFactory().getDataset(f'{args.resultdir}/random_crawler_all.json', True),
            "all": DatasetFactory().getDataset(f'{args.resultdir}/random_all.json', True),
        })
    if 'head' in args.strategy:
        dataset.append(DatasetFactory().getDataset(get_latest_dataset_file(args.datadir, 'head', '.json')))
        resultDataset.append({
            "discover": DatasetFactory().getDataset(f'{args.resultdir}/head_discover.json', True),
            "fetch": DatasetFactory().getDataset(f'{args.resultdir}/head_fetch.json', True),
            "upload": DatasetFactory().getDataset(f'{args.resultdir}/head_upload.json', True),
            "rank": DatasetFactory().getDataset(f'{args.resultdir}/head_rank.json', True),
            "crawler_all": DatasetFactory().getDataset(f'{args.resultdir}/head_crawler_all.json', True),
            "all": DatasetFactory().getDataset(f'{args.resultdir}/head_all.json', True),
        })

    if 'status' in args.measure:
        statusResultDataset = DatasetFactory().getDataset(f'{args.resultdir}/status.json')
        context.setMeasure(CrawlerStatusMeasure(args.crawler_url, statusResultDataset))
        context.test()

    if 'discover' in args.measure:
        for i in range(len(dataset)):
            context.setMeasure(CrawlerDiscoverMeasure(dataset[i], args.crawler_url, resultDataset[i]["discover"]))
            context.test()
    if 'fetch' in args.measure:
        for i in range(len(dataset)):
            context.setMeasure(CrawlerFetchMeasure(dataset[i], args.crawler_url, resultDataset[i]["fetch"]))
            context.test()
    if 'upload' in args.measure:
        for i in range(len(dataset)):
            context.setMeasure(CrawlerUploadMeasure(dataset[i], args.crawler_url, resultDataset[i]["upload"]))
            context.test()
    if 'rank' in args.measure:
        for i in range(len(dataset)):
            context.setMeasure(TypesenseRankMeasure(dataset[i], args.typesense_url, resultDataset[i]["rank"]))
            context.test()
    if 'crawler_all' in args.measure:
        for i in range(len(dataset)):
            context.setMeasure(CrawlerAllMetricMeasure(dataset[i], args.crawler_url, resultDataset[i]["crawler_all"]))
            context.test()
    if 'all' in args.measure:
        for i in range(len(dataset)):
            context.setMeasure(SearchEngineAllMetricMeasure(dataset[i], args.crawler_url, args.typesense_url, resultDataset[i]["all"]))
            context.test()

def main():
    args = parseArgs()

    if args.create:
        createDataset(args)
    if args.test:
        test(args)

if __name__ == '__main__':
    main()
