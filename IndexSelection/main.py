from argparse import ArgumentParser
import json

# Database
from Database.Database import Database
from models import create_url_state_model, UrlStateMixin

# Chain Handlers
from IndexSelection.Chain.Handler import Handler
from IndexSelection.Chain.PipelineResult import PipelineResult
from IndexSelection.Chain.ContentRead import ContentRead
from IndexSelection.Chain.ExtractionJson import ExtractionJson
from IndexSelection.Chain.QualityFilter import QualityFilter
from IndexSelection.Chain.Scoring import Scoring
from IndexSelection.Chain.Ingestion import Ingestion

def parseArgs():
    parser = ArgumentParser()
    parser.add_argument("--database", type=str, default='ws2.csie.ntu.edu.tw:22224', help="Database URL")
    # limit 改為每個 batch 的大小，或者你可以保留用來做總量限制
    parser.add_argument("--limit", type=int, default=0, help="Total limit rows per table for testing (0 for no limit)")
    parser.add_argument("--range", type=int, default=256, help="Limit number of tables")
    parser.add_argument("--batch_size", type=int, default=100, help="Process batch size")
    parser.add_argument("--reset", action="store_true", help="Reset typesense status before processing")

    args = parser.parse_args()
    return args

def main():
    args = parseArgs()
    DB_USER = "crawler"
    DB_PASS = "crawler"
    DB_NAME = "crawlerdb"
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{args.database}/{DB_NAME}"
    
    # 初始化 Database (使用你原本的 Class)
    db = Database(DATABASE_URL)

    # 建立處理鏈
    h1: Handler = ContentRead()
    h2: Handler = ExtractionJson()
    h3: Handler = QualityFilter()
    h4: Handler = Scoring()
    h5: Handler = Ingestion()

    h1.setNext(h2).setNext(h3).setNext(h4).setNext(h5)

    error_breakdown = {}
    stage_breakdown = {}
    
    # 遍歷分片表 (0 ~ range)
    for i in range(args.range):
        table_name = f'url_state_{i:03}'
        # print(f"Processing table: {table_name}")
        
        # 動態取得 Model Class
        UrlState = create_url_state_model(table_name)

        # 邏輯優化：只有在有下 --reset 參數時才歸零
        if args.reset:            
            # 使用迴圈分批更新，避免鎖死整張表
            batch_size = 5000
            while True:
                with db.session() as s:
                    # 1. 找出還沒被重置的 ID (只找一批)
                    # 我們利用 subquery 找出 fetch_ok > 0 且 typesense 狀態不為 0 的資料
                    subquery = s.query(UrlState.url)\
                        .filter(UrlState.fetch_ok > 0)\
                        .filter((UrlState.typesense_ok != 0) | (UrlState.typesense_fail != 0))\
                        .limit(batch_size)\
                        .with_for_update(skip_locked=True) # 關鍵：跳過被爬蟲鎖住的行
                        
                    target_urls = [row.url for row in subquery]
                    
                    if not target_urls:
                        break # 全部重置完成

                    # 2. 針對這批 ID 進行更新
                    s.query(UrlState)\
                        .filter(UrlState.url.in_(target_urls))\
                        .update(
                            {
                                UrlState.typesense_ok: 0, 
                                UrlState.typesense_fail: 0
                            },
                            synchronize_session=False
                        )
                    s.commit()
        
        total_processed_in_table = 0
        last_url = "" # 用來做 Keyset Pagination 的指標

        while True:
            # 檢查是否達到測試用的 limit
            if args.limit > 0 and total_processed_in_table >= args.limit:
                break

            with db.session() as s:
                # =================================================
                # 核心改動：Keyset Pagination (分批讀取)
                # =================================================
                # 1. 每次只抓 batch_size 筆資料，轉成 list (這樣就沒有 Cursor 占用問題)
                # 2. 加上 typesense_ok == 0 避免重複處理
                # 3. 使用 url > last_url 來定位下一批，效能極佳
                
                query = s.query(UrlState)\
                    .filter(UrlState.fetch_ok > 0)\
                    .filter(UrlState.typesense_ok ==0, UrlState.typesense_fail ==0)\
                    .filter(UrlState.url > last_url)\
                    .order_by(UrlState.url.asc())\
                    .limit(args.batch_size)
                
                batch_data: list[UrlStateMixin] = query.all() # 這裡直接執行 SQL 拿到 List

                if not batch_data:
                    # 抓不到資料了，這張表處理完畢
                    break

                # 開始處理這一批
                for data in batch_data:
                    # 紀錄最後一筆的 URL，給下一輪 query 用
                    last_url = data.url
                    
                    try:
                        result: PipelineResult = h1.handle(data)

                        # 統計 Stage
                        if result.stage not in stage_breakdown:
                            stage_breakdown[result.stage] = 0
                        stage_breakdown[result.stage] += 1

                        if result.success:
                            # ✅ 成功時：標記 typesense_ok，避免下次重複抓
                            data.typesense_ok = 1
                            # 可以記錄時間
                            # data.last_typesense_push = datetime.now()
                            # print(f"[OK] {data.url}: {data.index_priority}")
                        else:
                            # ❌ 失敗時：統計錯誤原因
                            if result.reason not in error_breakdown:
                                error_breakdown[result.reason] = 0
                            error_breakdown[result.reason] += 1
                            
                            # 標記失敗，避免無限重試 (或者你可以設計重試邏輯)
                            data.typesense_fail = 1
                            data.index_priority = -1 
                            # data.failed_reason = result.reason
                            
                            # print(f"[FAIL] {result.stage}: {result.reason}")

                    except Exception as e:
                        # print(f"Exception processing {data.url}: {e}")
                        data.typesense_fail = 1 # 標記失敗

                # =================================================
                # 提交這一批次
                # =================================================
                s.commit() # 這裡 commit 安全了，因為我們沒有正在跑的 Cursor
                
                total_processed_in_table += len(batch_data)
                # print(f"  Committed batch of {len(batch_data)}. Total: {total_processed_in_table}")

    # 寫入統計結果
    with open('error_breakdown.json', 'w', encoding='utf-8') as f:
        json.dump(error_breakdown, f, ensure_ascii=False, indent=4)
    with open('stage_breakdown.json', 'w', encoding='utf-8') as f:
        json.dump(stage_breakdown, f, ensure_ascii=False, indent=4)

if __name__ == '__main__':
    main()