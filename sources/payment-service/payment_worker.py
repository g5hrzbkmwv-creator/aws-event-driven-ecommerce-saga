import json
import logging
import os
import time
import boto3
from botocore.exceptions import ClientError

# 設定日誌
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

# 💡 修正：接收來自 inventory-received-queue 的訊息
QUEUE_URL = os.getenv(
  "INVENTORY_RECEIVED_QUEUE_URL"
)

# 💡 修正：將付款結果發布到 payment-events-topic
PAYMENT_TOPIC_ARN = os.getenv(
  "PAYMENT_TOPIC_ARN"
)

def process_payment(order_id: str, user_id: str, items: list) -> bool:
  logger.info(
      f"[Payment Service] 收到庫存完成通知，開始處理付款: Order ID={order_id},"
      f" User={user_id}"
  )
  
  # 模擬真實世界金流處理，故意暫停 10 秒
  time.sleep(10)

  # 模擬扣款邏輯
  payment_success = True
  logger.info(f"[Payment Worker] 訂單 {order_id} 扣款成功！")
  
  return payment_success


def publish_payment_event(order_id: str, success: bool, user_id: str, items: list):
  event_type = "Success" if success else "Failed"
  payload = {
      "order_id": order_id,
      "user_id": user_id,
      "items": items,
      "status": event_type,
  }

  try:
    sns_client.publish(
        TopicArn=PAYMENT_TOPIC_ARN,
        Message=json.dumps(payload),
        MessageAttributes={
            "event_type": {
              "DataType": "String", 
              "StringValue": event_type
            }
        },
    )
    logger.info(f"成功發布事件 {event_type} 至 Payment Topic, Order ID: {order_id}")
  except ClientError as e:
    logger.error(f"發布事件失敗 ({event_type}): {e}")


def poll_queue():
  logger.info(f"Payment Worker 開始監聽佇列: {QUEUE_URL}")

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

        # 解析 SNS 包裹在 SQS 裡的訊息內容
        if "Message" in body:
          event_data = json.loads(body["Message"])
        else:
          event_data = body

        order_id = event_data.get("order_id")
        user_id = event_data.get("user_id")
        items = event_data.get("items", [])

        if order_id:
          success = process_payment(order_id, user_id, items)
          publish_payment_event(order_id, success, user_id, items)

        sqs_client.delete_message(
            QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle
        )
        logger.info(f"已處理並刪除訊息, Order ID: {order_id}")

    except Exception as e:
      logger.error(f"輪詢 SQS 發生錯誤: {e}")
      time.sleep(5)


if __name__ == "__main__":
  poll_queue()
