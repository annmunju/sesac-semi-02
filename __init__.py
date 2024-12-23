import sys
from extract.load_dynamodb import query_items_by_date
from transform.convert_raw_df import convert_items_to_df
from transform.convert_visit_area_info import *
from load.save_rdb import insert_table

def run_etl_pipeline(date):
    items = query_items_by_date(date)
    raw_df = convert_items_to_df(items)
    func_ls = [change_tms_df, change_t_df, change_ta_df]
    for func in func_ls:
        df, tb_name = func(raw_df)
        insert_table(df, tb_name)

if __name__ == "__main__":
    run_etl_pipeline(sys.argv[1])