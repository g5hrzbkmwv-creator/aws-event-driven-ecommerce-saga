FROM python:3.11-slim
# 設定工作目錄
WORKDIR /app

# 安裝 boto3 以便連線 SQS/SNS
RUN pip install --no-cache-dir boto3

# 複製程式碼
COPY order_failed_worker.py /app/order_failed_worker.py

# 執行 Worker
CMD ["python", "order_failed_worker.py"]
