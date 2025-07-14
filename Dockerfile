FROM python:3.11.0
WORKDIR /data_transform
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "process_pipeline.py"]