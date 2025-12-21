import time
from sqlalchemy import create_engine, text
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor, as_completed

def parseArgs():
    parser = ArgumentParser()
    parser.add_argument("--database", type=str, default='ws2.csie.ntu.edu.tw:22224', help="Database URL")
    parser.add_argument("--workers", type=int, default=16, help="Parallel workers")
    return parser.parse_args()

def process_single_table(table_index, db_url):
    """
    單一 Table 的處理邏輯 (Worker Function)
    """
    table_name = f'url_state_{table_index:03}'
    
    # 建立一個不使用 Connection Pool 的 Engine (避免多進程共用 Pool 問題)
    # 並且設定 isolation_level="AUTOCOMMIT"
    engine = create_engine(db_url, isolation_level="AUTOCOMMIT")
    
    # 設定 Lock Timeout (例如 3秒)，拿不到鎖就報錯，不要死等
    # 這樣可以避免卡住整個資料庫
    sql_timeout = text("SET lock_timeout = '3s';")
    
    sql_alter = text(f"""
        ALTER TABLE {table_name} 
        DROP COLUMN IF EXISTS reason;
    """)

    max_retries = 10
    for attempt in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(sql_timeout) # 設定這次連線的超時
                conn.execute(sql_alter)   # 執行修改
                return f"✅ {table_name} 更新成功"
        except Exception as e:
            if "lock timeout" in str(e).lower():
                time.sleep(1) # 休息一下再試
                continue
            return f"❌ {table_name} 失敗: {e}"
            
    return f"⚠️ {table_name} 超時放棄 (可能爬蟲正在大量佔用)"

def main():
    args = parseArgs()
    DB_USER = "crawler"
    DB_PASS = "crawler"
    DB_NAME = "crawlerdb"
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{args.database}/{DB_NAME}"
    
    print(f"🚀 開始並行更新 (Workers: {args.workers})...")
    
    start_time = time.time()
    
    # 使用 ProcessPoolExecutor 平行處理
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_single_table, i, DATABASE_URL): i 
            for i in range(256)
        }
        
        success_count = 0
        fail_count = 0

        for future in as_completed(futures):
            result = future.result()
            print(result)
            if "✅" in result:
                success_count += 1
            else:
                fail_count += 1

    end_time = time.time()
    print(f"\n🎉 處理完成！耗時: {end_time - start_time:.2f} 秒")
    print(f"成功: {success_count}, 失敗/跳過: {fail_count}")

if __name__ == "__main__":
    main()