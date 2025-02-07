# 새싹 세미프로젝트 소스코드 (ETL)

### 코드 실행 방법
- `python __init__.py`

### 빌드부터 ECR 이미지 업로드까지
- 도커 이미지 테스트
    - `docker build -t python-etl-dev --build-arg profile=dev .` : 개발용 빌드
    - `docker run python-etl-dev` : 컨테이너 실행
- ECR 이미지 업로드
    - `docker build --platform linux/amd64 -t python-etl --build-arg profile=dev .` : ECR용 빌드
    - `docker tag python-etl 831926607501.dkr.ecr.ap-northeast-2.amazonaws.com/semi-ecr/etl:python-etl` : ECR용 태그
    - `docker push 831926607501.dkr.ecr.ap-northeast-2.amazonaws.com/semi-ecr/etl:python-etl` : ECR 업로드

