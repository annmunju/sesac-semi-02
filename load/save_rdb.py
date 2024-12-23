'''
테이블에 저장
'''
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


def insert_table(converted_df, table_name):
    column_ls = converted_df.columns.tolist()
    for _, r in converted_df.iterrows():
        conn =  psycopg2.connect(host=rds_host, user=rds_username, password=rds_password, port=rds_port, database=rds_database)
        cur = conn.cursor()
        sql_query = f"""INSERT INTO {table_name} 
                    ({','.join(column_ls)}) 
                VALUES ({','.join(['%s' for _ in range(len(column_ls))])})"""
        try:
            cur.execute(sql_query, r.values.tolist())
            print('done', column_ls)
            conn.commit()
        except psycopg2.errors.UniqueViolation as unq:
            print(unq)
        except psycopg2.errors.InFailedSqlTransaction as ifq:
            print(ifq)

        cur.close()