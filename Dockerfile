FROM python:3-slim
WORKDIR /app
COPY . /app
RUN apt-get update && apt-get install -y libpq-dev gcc
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT ["python", "__init__.py"]