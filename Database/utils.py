from Database.ModelFactory.AppModelFactory import AppModelFactory
from Database.Database import Database

def createAllMetricModel(modelFactory: AppModelFactory):
    suffixes = ['Total', 'A', 'B']
    metric_types = ['RandomSet', 'HeadSet']

    for suffix in suffixes:
        modelFactory.create_crawler_stat_model(suffix)

        for metric_type in metric_types:
            modelFactory.create_metric_coverage_model(metric_type, suffix)

def createDB(user, password, url, name, createTable: bool=False, base=None) -> Database:
    DATABASE_URL = f"postgresql+psycopg2://{user}:{password}@{url}/{name}"
    db = Database(DATABASE_URL)

    if createTable:
        db.create_tables(base)
    return db

