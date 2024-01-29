FROM python:3.10

WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY data_ingestion.py ./

ENTRYPOINT ["python", "./data_ingestion.py"]