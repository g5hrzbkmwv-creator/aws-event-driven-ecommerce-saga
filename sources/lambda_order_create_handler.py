import json
import logging
import os
import uuid
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

sns_client = boto3.client("sns", region_name=os.getenv(
    "AWS_REGION", 
    "ap-southeast-2"))
ORDER_TOPIC_ARN = ORDER_TOPIC_ARN = os.getenv(
    "ORDER_TOPIC_ARN",
    "arn:aws:sns:ap-southeast-2:315224414670:order-events-topic"
)

def lambda_handler(event, context):
  try:
    # 1. 解析前端傳來的 JSON 請求主體
    body = json.loads(event.get("body", "{}"))
    order_id = body.get("order_id")
    user_id = body.get("user_id")
    items = body.get("items", [])

    if not order_id:
      order_id = str(uuid.uuid4())
      logger.info(f"未收到 order_id，已自動生成: {order_id}")

    if not items:
      return {
          "statusCode": 400,
          "body": json.dumps({"error": "Missing order_id or items"}),
      }

    # 2. 組合事件內容
    payload = {"order_id": order_id, "user_id": user_id, "items": items}

    # 3. 發布訊息到 SNS (帶有 event_type: Created)
    sns_client.publish(
        TopicArn=ORDER_TOPIC_ARN,
        Message=json.dumps(payload),
        MessageAttributes={
            "event_type": {
                "DataType": "String", 
                "StringValue": "Created"
            }
        },
    )

    # 4. 回傳成功接收回應
    return {
        "statusCode": 202,
        "body": json.dumps({
            "message": "Order accepted and processing started",
            "order_id": order_id,
        }),
    }

  except Exception as e:
    return {"statusCode": 500, "body": json.dumps({"error": str(e)})}