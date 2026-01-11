from sqlalchemy import Column, Date, Integer, BigInteger, Float, String, ForeignKey, Text, DateTime, JSONB
from sqlalchemy.orm import declarative_base, declarative_mixin, relationship
from datetime import date

# 建立 Base
Base = declarative_base()

# ==========================================
# 1. 定義 Mixin: Crawler Stat (爬蟲統計)
# ==========================================
# 這個 Mixin 包含第一組 (Total, A, B) 共用的欄位
@declarative_mixin
class CrawlerStatMixin:
    # 共同的主鍵
    stat_date = Column(Date, primary_key=True)

    # 基礎數據
    discovered = Column(BigInteger, nullable=True)
    crawled    = Column(BigInteger, nullable=True)
    indexed    = Column(BigInteger, nullable=True)

    # 每日 Fetch 狀態
    fetch_ok    = Column(Integer, nullable=True)
    fetch_fail  = Column(Integer, nullable=True)
    fetch_total = Column(BigInteger, nullable=True)

    # 7天滾動統計
    fetch_ok_7    = Column(BigInteger, nullable=True)
    fetch_fail_7  = Column(BigInteger, nullable=True)
    fetch_total_7 = Column(BigInteger, nullable=True)

    # 30天滾動統計
    fetch_ok_30    = Column(BigInteger, nullable=True)
    fetch_fail_30  = Column(BigInteger, nullable=True)
    fetch_total_30 = Column(BigInteger, nullable=True)

    # Error Log
    http_error_404    = Column(Integer, nullable=True)
    http_error_404_7  = Column(Integer, nullable=True)
    http_error_404_30 = Column(Integer, nullable=True)
    http_error_505    = Column(Integer, nullable=True)
    http_error_505_7  = Column(Integer, nullable=True)
    http_error_505_30 = Column(Integer, nullable=True)


# ==========================================
# 2. 定義 Mixin: Metric Coverage (覆蓋率指標)
# ==========================================
# 這個 Mixin 包含第二組 (Total, A, B) 共用的欄位
# 包含 HeadSet 與 RandomSet 的統計
@declarative_mixin
class MetricCoverageMixin:
    stat_date = Column(Date, primary_key=True)

    total           = Column(BigInteger, nullable=True)
    discovered_num  = Column(BigInteger, nullable=True)
    discovered_rate = Column(Float, nullable=True)
    crawled_num     = Column(BigInteger, nullable=True)
    crawled_rate    = Column(Float, nullable=True)
    indexed_num     = Column(BigInteger, nullable=True)
    indexed_rate    = Column(Float, nullable=True)
    ranked_num      = Column(BigInteger, nullable=True)
    ranked_rate     = Column(Float, nullable=True)

class MetricBatch(Base):
    __tablename__ = 'metric_batches'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=date.today)
    
    meta_total_keywords = Column(Integer, default=0)
    
    meta_total_urls = Column(Integer, default=0)
    
    meta_geo_counts = Column(JSONB, default=dict)

    # 關聯
    queries = relationship("MetricQuery", back_populates="batch")


class MetricQuery(Base):
    __tablename__ = 'metric_queries'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    batch_id = Column(BigInteger, ForeignKey('metric_batches.id'), index=True)
    
    keyword = Column(String, nullable=False)
    geo = Column(JSONB, default=list)
    frequency = Column(Integer, default=0)

    # 關聯：一個 Task 可能會有多個 Result (1對多)
    results = relationship("MetricURL", back_populates="query")
    batch = relationship("MetricBatch", back_populates="queries")


class MetricURL(Base):
    __tablename__ = 'metric_url'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # 連結回 Task (這樣就知道是哪個 Keyword + Geo 產生的結果)
    query_id = Column(BigInteger, ForeignKey('metric_queries.id'), index=True)

    url = Column(Text)
    rank = Column(Integer)
    
    query = relationship("MetricQuery", back_populates="results")