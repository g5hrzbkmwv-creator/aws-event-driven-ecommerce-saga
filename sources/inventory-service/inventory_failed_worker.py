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
sns_client = boto3.client(
    "sns", region_name=os.getenv("AWS_REGION", "ap-southeast-2")
)

# 從環境變數取得 SQS 佇列 URL 與 SNS Topic ARN
QUEUE_URL = os.getenv(
    "PAYMENT_FAILED_QUEUE_URL",
    "https://sqs.ap-southeast-2.amazonaws.com/315224414670/payment-failed-queue",
)
INVENTORY_TOPIC_ARN = os.getenv(
    "INVENTORY_TOPIC_ARN",
    "arn:aws:sns:ap-southeast-2:315224414670:inventory-events-topic",
)

def compensate_inventory(order_id: str, items: list):
  logger.info(
      f"[Inventory Compensation] 收到付款失敗通知，執行補償（釋放/退還庫存）: Order"
      f" ID={order_id}, Items={items}"
  )
  # TODO: 這裡實作資料庫庫存加回的邏輯 (Rollback)

def publish_inventory_event(event_type: str, order_data: dict):
  """發布事件到 inventory-events-topic (SNS)"""
  message_payload = {
      "order_id": order_data.get("order_id"),
      "user_id": order_data.get("user_id"),
      "items": order_data.get("items"),
  }

  try:
    response = sns_client.publish(
        TopicArn=INVENTORY_TOPIC_ARN,
        Message=json.dumps(message_payload),
        MessageAttributes={
            "event_type": {
                "DataType": "String",
                "StringValue": event_type,  #對應 Received 或 Failed
            }
        },
    )
    logger.info(
        f"成功發布 InventoryEvent [{event_type}], MessageId:"
        f" {response.get('MessageId')}"
    )
  except ClientError as e:
    logger.error(f"發布 SNS 事件失敗: {e}")
    raise e

def poll_queue():
  logger.info(f"Inventory Compensate Worker 開始監聽佇列: {QUEUE_URL}")

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
          compensate_inventory(order_id, items)
          publish_inventory_event("Failed", order_data)

        sqs_client.delete_message(
            QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle
        )
        logger.info(f"已完成補償並刪除訊息, Order ID: {order_id}")

    except Exception as e:
      logger.error(f"輪詢 SQS 發生錯誤: {e}")
      time.sleep(5)


if __name__ == "__main__":
  poll_queue()