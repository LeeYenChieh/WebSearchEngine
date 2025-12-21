from argparse import ArgumentParser
from itertools import islice
import json
import time
import os
from concurrent.futures import ProcessPoolExecutor

# Database
from Database.Database import Database
from models import create_url_state_model, UrlStateMixin

# Chain Handlers
from Chain.Handler import Handler
from Chain.PipelineResult import PipelineResult
from Chain.ContentRead import ContentRead
from Chain.ExtractionJson import ExtractionJson
from Chain.QualityFilter import QualityFilter
from Chain.Scoring import Scoring
from Chain.Ingestion import Ingestion

def parseArgs():
    parser = ArgumentParser()
    parser.add_argument("--database", type=str, default='ws2.csie.ntu.edu.tw:22224', help="Database URL")
    parser.add_argument("--limit", type=int, default=0, help="Total limit rows per table for testing (0 for no limit)")
    parser.add_argument("--range", type=int, default=256, help="Limit number of tables")
    parser.add_argument("--batch_size", type=int, default=100, help="Process batch size")
    parser.add_argument("--workers", type=int, default=4, help="Number of processes") # 新增 worker 參數
    parser.add_argument("--reset", action="store_true", help="Reset typesense status before processing")

    args = parser.parse_args()
    return args

def process_single_table(table_index: int, db_url: str, args):
    """
    Worker Function: 獨立處理一張 Table 的所有邏輯
    """
    # 1. 在 Process 內部建立獨立的 DB 連線
    db = Database(db_url)
    
    # 2. 在 Process 內部建立獨立的 Pipeline
    h1: Handler = ContentRead()
    h2: Handler = ExtractionJson()
    h3: Handler = QualityFilter()
    h4: Handler = Scoring()
    h5: Handler = Ingestion()

    h1.setNext(h2).setNext(h3).setNext(h4).setNext(h5)

    # 統計變數
    error_breakdown = {}
    stage_breakdown = {}
    
    table_name = f'url_state_{table_index:03}'
    UrlState = create_url_state_model(table_name)

    # =================================================
    # Reset Logic (如果需要)
    # =================================================
    if args.reset:            
        reset_batch_size = 5000
        while True:
            with db.session() as s:
                subquery = s.query(UrlState.url)\
                    .filter(UrlState.fetch_ok > 0)\
                    .filter(UrlState.indexed != 0)\
                    .limit(reset_batch_size)\
                    .with_for_update(skip_locked=True)
                    
                target_urls = [row.url for row in subquery]
                
                if not target_urls:
                    break 

                s.query(UrlState)\
                    .filter(UrlState.url.in_(target_urls))\
                    .update(
                        {
                            UrlState.indexed: 0, 
                        },
                        synchronize_session=False
                    )
                s.commit()

    # =================================================
    # Processing Logic
    # =================================================
    total_processed_in_table = 0

    while True:
        if args.limit > 0 and total_processed_in_table >= args.limit:
            break

        with db.session() as s:
            query = s.query(UrlState)\
                .filter(UrlState.fetch_ok > 0)\
                .filter(UrlState.indexed == 0)\
                .order_by(UrlState.url.asc()) \
                .limit(args.batch_size)\
                .with_for_update(skip_locked=True)
            
            batch_data: list[UrlStateMixin] = query.all()

            if not batch_data:
                break

            for data in batch_data:
                last_url = data.url
                
                try:
                    result: PipelineResult = h1.handle(data)

                    # 統計 Stage
                    if result.stage not in stage_breakdown:
                        stage_breakdown[result.stage] = 0
                    stage_breakdown[result.stage] += 1

                    if result.success:
                        data.indexed = 1
                    else:
                        if result.reason not in error_breakdown:
                            error_breakdown[result.reason] = 0
                        error_breakdown[result.reason] += 1
                        
                        data.indexed = -1
                        data.indexed_reason = result.reason
                        data.index_priority = -1 

                except Exception:
                    data.indexed = -1 

            s.commit()
            total_processed_in_table += len(batch_data)

    # =================================================
    # 輸出該 Table 的統計結果
    # 檔名格式: breakdown_000.json
    # =================================================
    output_filename = f'result/breakdown_{table_index:03}.json'
    final_report = {
        "table": table_name,
        "total_processed": total_processed_in_table,
        "stage_breakdown": stage_breakdown,
        "error_breakdown": error_breakdown
    }
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(final_report, f, ensure_ascii=False, indent=4)


def main():
    args = parseArgs()
    DB_USER = "crawler"
    DB_PASS = "crawler"
    DB_NAME = "crawlerdb"
    # 組合 DB URL 傳給 worker，讓 worker 自己建立連線
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{args.database}/{DB_NAME}"
    
    # 使用 ProcessPoolExecutor 進行多進程並行
    # max_workers 建議設定為 CPU 核心數，或根據 DB 連線數限制調整
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for i in range(args.range):
            # 提交任務
            futures.append(
                executor.submit(process_single_table, i, DATABASE_URL, args)
            )
        
        # 等待所有任務完成
        for future in futures:
            try:
                future.result()
            except Exception as e:
                # 這裡可以 catch worker 拋出的 exception，但依照需求不 print
                print(e)

if __name__ == '__main__':
    main()