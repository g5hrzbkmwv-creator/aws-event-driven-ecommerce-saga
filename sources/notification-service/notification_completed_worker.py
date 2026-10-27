import json
import logging
import os
import time
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# 初始化 AWS Clients
sqs_client = boto3.client(
    "sqs", region_name=os.getenv("AWS_REGION", "ap-southeast-2")
)

# 從環境變數取得 SQS 佇列 URL 與 SNS Topic ARN
QUEUE_URL = os.getenv(
    "PAYMENT_COMPLETED_QUEUE_URL"
)

def handle_completed(order_id: str, user_id: str):
  logger.info(
      f"[Notification Completed Service] 訂單交易完全成功！更新狀態為 COMPLETED: Order"
      f" ID={order_id}, User={user_id}"
  )
  # TODO: 更新資料庫訂單狀態為 COMPLETED

def poll_queue():
  logger.info(f"Notification Completed Worker 開始監聽佇列: {QUEUE_URL}")

  while True:
    try:
      response = sqs_client.receive_message(
          QueueUrl=QUEUE_URL,
          MaxNumberOfMessages=1,
          WaitTimeSeconds=20,
          VisibilityTimeout=30,
      )

      messages = response.get("Messages", [])
      if not messages:
        continue

      for message in messages:
        receipt_handle = message["ReceiptHandle"]
        body = json.loads(message["Body"])

        if "Message" in body:
          event_data = json.loads(body["Message"])
        else:
          event_data = body

        order_id = event_data.get("order_id")
        user_id = event_data.get("user_id")

        if order_id:
          handle_completed(order_id, user_id)

        sqs_client.delete_message(
            QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle
        )
        logger.info(f"訂單完成狀態已記錄並刪除訊息, Order ID: {order_id}")

    except Exception as e:
      logger.error(f"輪詢 SQS 發生錯誤: {e}")
      time.sleep(5)


if __name__ == "__main__":
  poll_queue()
