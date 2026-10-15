import json
import logging
import os
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

dynamodb = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION"))
TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "Orders")

if not TABLE_NAME:
  raise ValueError("DYNAMODB_TABLE_NAME environment variable is required.")

table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
  logger.info(f"收到查詢請求事件: {json.dumps(event)}")

  # 從 API Gateway 的路徑參數取得 order_id
  path_parameters = event.get("pathParameters") or {}
  order_id = path_parameters.get("order_id")

  if not order_id:
    return {
        "statusCode": 400,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({"error": "Missing order_id in path parameters"}),
    }

  try:
    # 從 DynamoDB 查詢訂單
    response = table.get_item(Key={"order_id": order_id})
    item = response.get("Item")

    if not item:
      return {
          "statusCode": 404,
          "headers": {
              "Content-Type": "application/json",
              "Access-Control-Allow-Origin": "*",
          },
          "body": json.dumps(
              {"error": "Order not found", "order_id": order_id}
          ),
      }

    # 成功找到訂單，回傳狀態
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(item, default=str),
    }

  except ClientError as e:
    logger.error(f"查詢 DynamoDB 發生錯誤: {e}")
    return {
        "statusCode": 500,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({"error": "Internal server error"}),
    }