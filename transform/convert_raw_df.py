import pandas as pd
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

# RDS 연결 정보
rds_host = os.getenv('RDS_HOST')
rds_port = int(os.getenv('RDS_PORT'))
rds_username = os.getenv('RDS_USERNAME')
rds_password = os.getenv('RDS_PASSWORD')
rds_database = os.getenv('RDS_DATABASE')

def get_travel_id_cnt():
    try:
        conn = psycopg2.connect(host=rds_host, user=rds_username, password=rds_password, port=rds_port, database=rds_database)
        cur = conn.cursor()
        sql_query = "select travel_id from travel;"
        cur.execute(sql_query)
        data = cur.fetchall()
        id_len = len(data)
        cur.close()
        return id_len
    except Exception as e:
        print(f"데이터베이스 연결 오류: {e}")
        return 0

def convert_items_to_df(items):
    df = pd.DataFrame(columns=['id', 'traveler_id', 'age', 'gender', 'travel_status_res', 'travel_status_acr', 'visit_areas', 'visit_ymd', 'mvmn_nm', 'travel_reason', 'created_at'])
    for item in items:
        df = pd.concat([df, pd.DataFrame([{k:v_dict.get('S') for k, v_dict in item.items()}])], ignore_index=True)
    id_cnt = get_travel_id_cnt()
    df['travel_id'] = [f'V{i}' for i in range(id_cnt+1, id_cnt+len(df)+1)]
    return df
