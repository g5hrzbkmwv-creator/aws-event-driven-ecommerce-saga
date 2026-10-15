import json
import logging
import os
import time
import boto3
from botocore.exceptions import ClientError

# 設定日誌記錄
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
    "ORDER_CANCELLED_QUEUE_URL",
    "https://sqs.ap-southeast-2.amazonaws.com/315224414670/order-cancelled-queue",
)

def cancel_notification(order_id: str, items: list):
  logger.info(
      f"[Notification Cancellation] 收到庫存失敗通知，執行通知取消訂單: Order"
      f" ID={order_id}, Items={items}"
  )
  # TODO: 這裡實作取消訂單的邏輯 (Rollback)

def poll_queue():
  logger.info(f"Notification Cancelled Worker 開始監聽佇列: {QUEUE_URL}")

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
        items = event_data.get("items", [])

        if order_id:
          cancel_notification(order_id, items)

        sqs_client.delete_message(
            QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle
        )
        logger.info(f"已完成取消並刪除訊息, Order ID: {order_id}")

    except Exception as e:
      logger.error(f"輪詢 SQS 發生錯誤: {e}")
      time.sleep(5)


if __name__ == "__main__":
  poll_queue()