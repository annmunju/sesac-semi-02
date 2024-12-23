'''
데이터가 생성된 일자에 해당하는 항목 전체를 테이블에서 호출
'''

import boto3
import os
from dotenv import load_dotenv

load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region_name = os.getenv('REGION')

# DynamoDB 클라이언트 생성
dynamodb_client = boto3.client(
    'dynamodb',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

def query_items_by_date(date, table_name = 'MyTable'):
    """
    해당 데이터가 생성된 일자에 해당하는 모든 항목을 DynamoDB 테이블에서 조회하는 함수
    :param date: 조회할 일자 (YYYY-MM-DD 형식의 문자열)
    :param table_name: 조회할 테이블 이름
    """
    try:
        response = dynamodb_client.scan(
            TableName=table_name,
            FilterExpression='created_at = :created_at',
            ExpressionAttributeValues={
                ':created_at': {'S': date}
            }
        )
        return response.get('Items', [])
    except Exception as e:
        print(f"Error querying items: {e}")
        return []

if __name__ == "__main__":
    date = '2024-12-22'  # 조회할 일자
    items = query_items_by_date('MyTable', date)
    print(items)
