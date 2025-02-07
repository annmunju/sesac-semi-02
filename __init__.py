import time
from datetime import datetime

# custom code
from extract.load_dynamodb import query_items_by_date # 다이나모 DB에서 날짜 데이터 조회
from transform.convert_raw_df import convert_items_to_df # raw data를 dataframe 형태로 변환
from transform.convert_visit_dfs import change_tms_df, change_t_df, change_ta_df # 테이블 별 변환 코드 작성
from load.save_rdb import insert_table

def run_etl_pipeline(date, dynamodb_tb='semi-raw-data'):
    items = query_items_by_date(date, dynamodb_tb)
    raw_df = convert_items_to_df(items)
    func_ls = [change_tms_df, change_t_df, change_ta_df]
    for func in func_ls:
        df, tb_name = func(raw_df)
        insert_table(df, tb_name) # 테이블로 삽입

if __name__ == "__main__":
    while True:
        # stand_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        stand_date = datetime.now().strftime('%Y-%m-%d')
        run_etl_pipeline(stand_date)
        time.sleep(86400)  # 24시간 대기