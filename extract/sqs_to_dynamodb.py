'''
lambda에서 사용할 sqs 데이터 불러서 dynamodb에 저장하기 함수
'''

import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
import json

load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region_name = os.getenv('REGION')

# SQS 클라이언트 생성
sqs_client = boto3.client(
    'sqs',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# DynamoDB 클라이언트 생성
dynamodb_client = boto3.client(
    'dynamodb',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# SQS 큐 URL
queue_url = os.getenv('SQS_QUEUE_URL')

class Item:
    def __init__(self, id, traveler_id=None, age=None, gender=None, travel_status_res=None, 
                 travel_status_acr=None, visit_areas=None, visit_ymd=None):
        self.id = id  # DynamoDB의 기본 키로 사용될 id 필드 추가
        self.created_at = datetime.now().strftime('%Y-%m-%d')
        self.traveler_id = traveler_id
        self.age = age
        self.gender = gender
        self.travel_status_res = travel_status_res
        self.travel_status_acr = travel_status_acr
        self.visit_areas = visit_areas
        self.visit_ymd = visit_ymd

def insert_item_to_dynamodb(table_name, item):
    """
    DynamoDB 테이블에 Item 객체의 정보를 삽입하는 함수
    :param table_name: 삽입할 테이블 이름
    :param item: 삽입할 Item 객체
    """
    # Item 객체를 dictionary로 변환
    item_dict = item.__dict__

    try:
        response = dynamodb_client.put_item(
            TableName=table_name,
            Item={k: {'S': str(v)} for k, v in item_dict.items() if v is not None}
        )
        return response
    except Exception as e:
        print(f"Error inserting item: {e}")
        return None

def process_sqs_messages():
    """
    SQS 큐에서 메시지를 가져와서 DynamoDB에 저장하는 함수
    """
    try:
        # SQS 메시지 수신
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10
        )

        messages = response.get('Messages', [])
        for message in messages:
            body = message['Body']
            # 메시지 본문을 Item 객체로 변환 (여기서는 예시로 JSON 형식으로 가정)
            item_data = json.loads(body)
            item = Item(
                id=item_data['id'],
                traveler_id=item_data.get('traveler_id'),
                age=item_data.get('age'),
                gender=item_data.get('gender'),
                travel_status_res=item_data.get('travel_status_res'),
                travel_status_acr=item_data.get('travel_status_acr'),
                visit_areas=item_data.get('visit_areas'),
                visit_ymd=item_data.get('visit_ymd')
            )
            # DynamoDB에 아이템 삽입
            insert_item_to_dynamodb('MyTable', item)
            # 메시지 삭제
            sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
    except Exception as e:
        print(f"Error processing SQS messages: {e}")
        sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=message['ReceiptHandle']
        )
        
if __name__ == "__main__":
    process_sqs_messages()
