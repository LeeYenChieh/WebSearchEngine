from Database.ModelFactory.DynamicModelFactory import DynamicModelFactory
from Database.MetricModels import CrawlerStatMixin, MetricCoverageMixin, MetricBatch, MetricQuery, MetricURL
from Database.CrawlerModels import UrlStateMixin, DomainStatsMixin, DomainStatsDailyMixin, SummaryDaily, UrlLink

class AppModelFactory():
    def __init__(self, crawlerBase, metricBase):
        self.crawlerModelFactory = DynamicModelFactory(crawlerBase)
        self.metricModelFactory = DynamicModelFactory(metricBase)

        self.crawlerBase = crawlerBase
        self.metricBase = metricBase
    
    def getCrawlerBase(self):
        return self.crawlerBase
    
    def getMetricBase(self):
        return self.metricBase

    def create_crawler_stat_model(self, suffix: str):
        return self.metricModelFactory.get_or_create(
            mixin=CrawlerStatMixin,
            table_base_name="crawler_stat",
            class_base_name="CrawlerStat",
            suffix=suffix
        )
    
    def create_metric_coverage_model(self, set_type: str, suffix: str):
        """HeadSet/RandomSet"""
        return self.metricModelFactory.get_or_create(
            mixin=MetricCoverageMixin,
            table_base_name=f"metric_{set_type.lower()}",
            class_base_name=f"Metric{set_type}",
            suffix=suffix
        )
    
    def create_metric_batches(self):
        return MetricBatch
    
    def create_metric_queries(self):
        return MetricQuery
    
    def create_metric_url(self):
        return MetricURL
    
    def create_url_state_model(self, idx: int):
        """Dynamically create UrlState ORM class for a given shard table."""
        return self.crawlerModelFactory.get_or_create(
            mixin=UrlStateMixin,
            table_base_name=f'url_state',
            class_base_name=f'url_state',
            suffix=f'{idx:03}'
        )

    def create_domain_stats_model(self, idx: int):
        return self.crawlerModelFactory.get_or_create(
            mixin=DomainStatsMixin,
            table_base_name=f'domain_stats',
            class_base_name=f'domain_stats',
            suffix=f'{idx:03}'
        )

    def create_domain_daily_model(self, idx: int):
        return self.crawlerModelFactory.get_or_create(
            mixin=DomainStatsDailyMixin,
            table_base_name=f'domain_stats_daily',
            class_base_name=f'domain_stats_daily',
            suffix=f'{idx:03}'
        )
    
    def create_summary_model(self):
        return SummaryDaily
    
    def create_url_link_model(self):
        return UrlLink