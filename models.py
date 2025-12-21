from sqlalchemy import Column, String, Integer, Boolean, Float, DateTime, Date, Computed
from sqlalchemy.dialects.postgresql import JSONB, BYTEA
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base, declarative_mixin
from datetime import datetime

Base = declarative_base()

@declarative_mixin
class UrlStateMixin:
    url = Column(String, primary_key=True)
    domain = Column(String, index=True)

    # Typesense contents
    content_path = Column(String)

    # Scheduler timestamps
    first_seen = Column(DateTime(timezone=True), server_default=func.now())
    last_offered = Column(DateTime(timezone=True), default=datetime.min, index=True)
    last_fetched = Column(DateTime(timezone=True), default=datetime.min, index=True)
    last_typesense_push = Column(DateTime(timezone=True), default=datetime.min, index=True)
    next_recrawl = Column(DateTime(timezone=True), default=datetime.min, index=True)

    # Status
    fetch_ok = Column(Integer, default=0)
    fetch_fail = Column(Integer, default=0)
    update_count = Column(Integer, default=0)
    typesense_ok = Column(Integer, default=0)
    typesense_fail = Column(Integer, default=0)
    status = Column(String, default="new", index=True)  # new / queued / ok / failed
    failed_reason = Column(String)
    content_hash = Column(String, index=True)
    has_update = Column(Boolean, default=False, index=True)

    # Link count
    inlink_count = Column(Integer, default=0)
    outlink_count = Column(Integer, default=0)

    # Scheduling parameters
    crawl_priority = Column(Float, default=0.0, index=True)
    index_priority = Column(Float, default=0.0, index=True)
    domain_score = Column(Float, default=0.0, index=True)

    indexed = Column(Integer, default=0, index=True)
    indexed_reason = Column(String, default="", index=True)

@declarative_mixin
class DomainStatsMixin:
    domain = Column(String, primary_key=True)
    url_count = Column(Integer, default=0)
    domain_score = Column(Float, default=0.0)
    offered_count = Column(Integer, default=0)
    fetch_ok = Column(Integer, default=0)
    fetch_fail = Column(Integer, default=0)
    update_count = Column(Integer, default=0)
    typesense_ok = Column(Integer, default=0)
    typesense_fail = Column(Integer, default=0)
    failed_rate = Column(Float, default=0.0)
    update_rate = Column(Float, default=0.0)
    fail_reasons = Column(JSONB, default=dict)

@declarative_mixin
class DomainStatsDailyMixin:
    domain = Column(String, primary_key=True)
    stat_date = Column(Date, primary_key=True)
    offered_count = Column(Integer, default=0)
    fetch_ok = Column(Integer, default=0)
    fetch_fail = Column(Integer, default=0)
    update_count = Column(Integer, default=0)
    typesense_ok = Column(Integer, default=0)
    typesense_fail = Column(Integer, default=0)
    failed_rate = Column(Float, default=0.0)
    update_rate = Column(Float, default=0.0)
    fail_reasons = Column(JSONB, default=dict)

class SummaryDaily(Base):
    __tablename__ = "summary_daily"
    stat_date = Column(Date, primary_key=True)

    # Crawling status
    new_links = Column(Integer, default=0)
    offered_count = Column(Integer, default=0)
    fetch_ok = Column(Integer, default=0)
    fetch_fail = Column(Integer, default=0)
    update_count = Column(Integer, default=0)
    failed_rate = Column(Float, default=0.0)
    update_rate = Column(Float, default=0.0)
    fail_reasons = Column(JSONB, default=dict)

    # Indexing status
    typesense_ok = Column(Integer, default=0)
    typesense_fail = Column(Integer, default=0)

    # scheduler server error
    error_count = Column(Integer, default=0)
    ingest_error = Column(Integer, default=0)
    upload_error = Column(Integer, default=0)
    refill_error = Column(Integer, default=0)
    offer_error = Column(Integer, default=0)
    update_error = Column(Integer, default=0)
    json_read_error = Column(Integer, default=0)

class UrlLink(Base):
    __tablename__ = "url_link"

    src_url = Column(String, primary_key=True)
    dst_url = Column(String, primary_key=True)
    anchor_hash = Column(BYTEA, Computed("decode(md5(anchor_text), 'hex')", persisted=True), primary_key=True)

    anchor_text = Column(String)

    first_seen = Column(DateTime(timezone=True), server_default=func.now())
    last_seen = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


_model_cache = {}

def create_url_state_model(table_name: str):
    """Dynamically create UrlState ORM class for a given shard table."""
    if table_name in _model_cache:
        return _model_cache[table_name]

    cls = type(
        table_name,
        (UrlStateMixin, Base),
        {
            "__tablename__": table_name,
        },
    )
    _model_cache[table_name] = cls
    return cls


def create_domain_stats_model(table_name: str):
    if table_name in _model_cache:
        return _model_cache[table_name]

    cls = type(
        table_name,
        (DomainStatsMixin, Base),
        {
            "__tablename__": table_name,
        },
    )
    _model_cache[table_name] = cls
    return cls

def create_domain_daily_model(table_name: str):
    if table_name in _model_cache:
        return _model_cache[table_name]

    cls = type(
        table_name,
        (DomainStatsDailyMixin, Base),
        {
            "__tablename__": table_name,
        },
    )
    _model_cache[table_name] = cls
    return cls