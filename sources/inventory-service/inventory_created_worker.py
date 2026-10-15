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
    "ORDER_CREATED_QUEUE_URL",
    "https://sqs.ap-southeast-2.amazonaws.com/315224414670/order-created-queue",
)
INVENTORY_TOPIC_ARN = os.getenv(
    "INVENTORY_TOPIC_ARN",
    "arn:aws:sns:ap-southeast-2:315224414670:inventory-events-topic",
)
TABLE_ORDER_NAME = os.getenv(
    "DYNAMODB_TABLE_ORDER_NAME",
    "Orders"
)
table = dynamodb.Table(TABLE_ORDER_NAME)


def check_and_create_order_record(
    order_id: str, user_id: str, items: list
) -> bool:
  """透過 DynamoDB 的 ConditionExpression 實作冪等性檢查與原子性建立 PENDING 紀錄"""
  try:
    # 嘗試寫入一筆新的訂單。如果 order_id 已經存在，ConditionalCheckFailedException 會被觸發
    table.put_item(
        Item={
            "order_id": order_id,
            "user_id": user_id,
            "items": items,
            "status": "PENDING",
            "updated_at": int(time.time()),
        },
        ConditionExpression="attribute_not_exists(order_id)",
    )
    logger.info(
        f"[Idempotency Check] 訂單不存在，成功建立 PENDING 紀錄, Order ID: {order_id}"
    )
    return True
  except ClientError as e:
    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
      # 代表這張訂單已經被處理過或紀錄已存在（SQS 重複投遞）
      logger.warning(
          f"[Idempotency Check] 攔截到重複訊息/已存在的訂單，跳過處理, Order ID:"
          f" {order_id}"
      )
      return False
    else:
      logger.error(f"DynamoDB 寫入發生未預期錯誤: {e}")
      raise e


def reserve_inventory(items: list) -> bool:
  """模擬資料庫扣減庫存的商業邏輯"""
  logger.info(f"正在檢查並扣減庫存，商品明細: {items}")
  # 假設這裡實作資料庫扣減...若庫存不足回傳 False
  # 為了示範，預設扣庫存成功
  return True


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


def process_message(message):
  """處理單一 SQS 訊息的核心邏輯"""
  receipt_handle = message["ReceiptHandle"]
  body = json.loads(message["Body"])

  # 如果訊息是透過 SNS 轉發進 SQS 的，真正的內容通常包在 body['Message'] 裡
  if "Message" in body:
    order_data = json.loads(body["Message"])
  else:
    order_data = body

  order_id = order_data.get("order_id")
  user_id = order_data.get("user_id")
  items = order_data.get("items", [])

  logger.info(f"收到訂單建立事件，準備處理 Order ID: {order_id}")


  try:
    # 1. 冪等性檢查和建立新紀錄
    new_request = check_and_create_order_record(order_id, user_id, items)

    if new_request:
      # 2. 執行業務邏輯（扣庫存）
      success = reserve_inventory(items)

      if success:
        # 3a. 扣庫存成功 -> 發布 Received 事件
        publish_inventory_event("Received", order_data)
      else:
        # 3b. 庫存不足 -> 發布 Failed 事件（觸發 Saga 補償）
        publish_inventory_event("Failed", order_data)

    # 4. 處理成功後，從 SQS 刪除該訊息 (Acknowledge)
    delete_sqs_message(receipt_handle)

  except Exception as e:
    logger.error(f"處理訂單 {order_id} 時發生未預期錯誤: {e}")
    # 發生例外時不刪除訊息，讓 SQS 根據 Visibility Timeout 重試；超過 maxReceiveCount 後會自動進入 DLQ


def delete_sqs_message(receipt_handle):
  """從 SQS 刪除已處理完畢的訊息"""
  sqs_client.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)


def poll_queue():
  """長期輪詢 (Long Polling) 迴圈"""
  logger.info(f"Inventory Worker 已啟動，正在監聽佇列: {QUEUE_URL}")

  while True:
    try:
      # SQS Long Polling (WaitTimeSeconds=20 可最大化減少空的請求次數)
      response = sqs_client.receive_message(
          QueueUrl=QUEUE_URL,
          MaxNumberOfMessages=10,  # 批次最多拿 10 筆
          WaitTimeSeconds=20,
          AttributeNames=["All"],
      )

      messages = response.get("Messages", [])
      if not messages:
        continue

      for message in messages:
        process_message(message)

    except Exception as e:
      logger.error(f"輪詢 SQS 發生錯誤: {e}")
      time.sleep(5)  # 發生網路瞬斷時暫停 5 秒再重試


if __name__ == "__main__":
  poll_queue()