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
dynamodb = boto3.resource(
    "dynamodb", region_name=os.getenv("AWS_REGION", "ap-southeast-2")
)


# 從環境變數取得 SQS 佇列 URL 與 SNS Topic ARN
QUEUE_URL = os.getenv(
    "INVENTORY_FAILED_QUEUE_URL",
    "https://sqs.ap-southeast-2.amazonaws.com/315224414670/inventory-failed-queue",
)
ORDER_TOPIC_ARN = os.getenv(
    "ORDER_TOPIC_ARN",
    "arn:aws:sns:ap-southeast-2:315224414670:order-events-topic",
)
TABLE_ORDER_NAME = os.getenv(
    "DYNAMODB_TABLE_ORDER_NAME",
    "Orders"
)
table = dynamodb.Table(TABLE_ORDER_NAME)


def cancel_order(order_id: str, items: list):
  logger.info(
      f"[Saga Rollback] 執行補償：退還庫存並取消訂單, Order ID: {order_id},"
      f" Items: {items}"
  )

  try:
    table.update_item(
        Key={"order_id": order_id},
        UpdateExpression="SET #st = :cancelled_status, updated_at = :timestamp",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":cancelled_status": "CANCELLED",
            ":timestamp": int(time.time()),
        },
    )
    logger.info(f"[Saga Rollback] 訂單狀態已更新為 CANCELLED, Order ID: {order_id}")
  except ClientError as e:
    logger.error(f"更新訂單狀態為 CANCELLED 失敗 (Order ID: {order_id}): {e}")



def publish_order_event(event_type: str, order_data: dict):
  event_type = "Cancelled"
  """發布事件到 order-events-topic (SNS)"""
  message_payload = {
      "order_id": order_data.get("order_id"),
      "user_id": order_data.get("user_id"),
      "items": order_data.get("items"),
  }

  try:
    response = sns_client.publish(
        TopicArn=ORDER_TOPIC_ARN,
        Message=json.dumps(message_payload),
        MessageAttributes={
            "event_type": {
                "DataType": "String",
                "StringValue": event_type,  #對應 Received 或 Failed
            }
        },
    )
    logger.info(
        f"成功發布 OrderEvent [{event_type}], MessageId:"
        f" {response.get('MessageId')}"
    )
  except ClientError as e:
    logger.error(f"發布 SNS 事件失敗: {e}")
    raise e

def poll_queue():
  logger.info(f"Order Failed Worker 開始監聽佇列: {QUEUE_URL}")

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
          cancel_order(order_id, items)
          publish_order_event(order_data)

        sqs_client.delete_message(
            QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle
        )
        logger.info(f"已完成補償並刪除訊息, Order ID: {order_id}")

    except Exception as e:
      logger.error(f"輪詢 SQS 發生錯誤: {e}")
      time.sleep(5)


if __name__ == "__main__":
  poll_queue()