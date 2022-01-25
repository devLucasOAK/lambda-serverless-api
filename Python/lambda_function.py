import json
import logging

import boto3

from custom_encoder import CustomEncoder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

table_name = "product-inventory"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(table_name)

# PATHS

path_health = "/health"
path_products = "/products"
path_product = "/product"


def lambda_handler(event, context):
    logger.info(event)
    http_method = event["httpMethod"]
    path = event["path"]
    request_body = json.loads(event["body"])

    if http_method == "GET" and path == path_health:
        build_response(200)

    elif http_method == "GET" and path == path_product:
        get_product(event["queryStringParameters"]["productId"])

    elif http_method == "GET" and path == path_products:
        get_products()

    elif http_method == "POST" and path == path_product:
        save_product(request_body)

    elif http_method == "PATCH" and path == path_product:
        modify_product(
            request_body["productId"],
            request_body["updatKey"],
            request_body["updateValue"],
        )

    elif http_method == "DELETE" and path == path_product:
        delete_product(request_body["productId"])

    else:
        build_response(404, "Not Found")


def get_product(productId):
    try:
        response = table.get_item(
            Key={
                "productId": productId,
            }
        )
        if "Item" in response:
            return build_response(200, response["Item"])
        else:
            return build_response(404, {"Message": "Product %s not Found" % productId})
    except:
        logger.exception("Error!")


def get_products():
    try:
        response = table.scan()
        result = response["Item"]

        while "LastEvaluatedValue" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvalauatedKey"])
            result.extend(response["Item"])

        body = {"products": response}

        return build_response(200, body)
    except:
        logger.exception("Error!")


def save_product(request_body):
    try:
        table.put_item(Item=request_body)
        body = {"Operation": "SAVE", "Message": "SUCCESS", "Item": request_body}
        return build_response(201, body)
    except:
        logger.exception("ERROR!")


def modify_product(productId, update_key, update_value):
    try:
        response = table.update_item(
            Key={"productId": productId},
            UpdateExpression="set %s = :value" % update_key,
            ExpressionAttributeValues={":value": update_value},
            ReturnValues="UPDATED_NEW",
        )
        body = {
            "Operation": "UPDATE",
            "Message": "SUCCESS",
            "UpdatedAttributes": response,
        }

        return build_response(200, body)
    except:
        logger.exception("ERROR!")


def delete_product(productId):
    try:
        response = table.delete_item(
            Key={"productId": productId}, ReturnValues="ALL_OLD"
        )
        body = {"Operation": "DELETE", "Message": "SUCCESS", "deletedItem": response}
        return build_response(200, body)
    except:
        logger.exception("ERROR!")


def build_response(status, body):
    response = {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
    }

    if body is not None:
        response["body"] = json.dumps(body, cls=CustomEncoder)
    return response
