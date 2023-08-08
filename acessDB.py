import json
import time
import botocore
import boto3
import random
from botocore.exceptions import ClientError

# intitalizing the services
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table_name = "A3-DB"
table = dynamodb.Table(table_name)


# we insert the value into db, if the value is present we update the value count
def update_dynamodb_table(data):
    for named_entity in data:
        # for every key value pair
        for key, value in named_entity.items():
            retries = 0
            while retries < 5:  # You can adjust the maximum number of retries as needed
                try:
                    # we get the fields by key
                    response = table.get_item(Key={"key": key})
                    if "Item" in response:
                        # Item exists, increment the count and update the value
                        current_count = response["Item"]["value"]
                        # increating count
                        new_value = current_count + value
                        table.update_item(
                            Key={"key": key},
                            UpdateExpression="SET #val = :newval",
                            ExpressionAttributeNames={"#val": "value"},
                            ExpressionAttributeValues={":newval": new_value},
                        )
                    else:
                        # Item does not exist, insert a new item with count 1 and value 1
                        table.put_item(Item={"key": key, "value": value})

                    break  # Exit the retry loop if the operation succeeds

                except botocore.exceptions.ClientError as e:
                    # Handle specific exceptions that might occur during DynamoDB operations
                    if (
                        e.response["Error"]["Code"]
                        == "ProvisionedThroughputExceededException"
                    ):
                        # Throttling - wait before retrying
                        retries += 1
                        backoff_time = (2**retries) + random.randint(0, 100) / 1000.0
                        time.sleep(backoff_time)
                    else:
                        raise


def lambda_handler(event, context):
    named_entities_batch = []

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        # Read the JSON file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        json_data = json.loads(content)
        print(json_data)

        # Extract named entities and their counts from the first key in the JSON object
        named_entities = json_data[list(json_data.keys())[0]]
        print(named_entities)

        # Accumulate named entities in the batch
        named_entities_batch.append(named_entities)

        print(named_entities_batch)

    try:
        # Update the DynamoDB table with named entities using batch processing
        update_dynamodb_table(named_entities_batch)
        print("success")

        return {
            "statusCode": 200,
            "body": "Named entities updated in DynamoDB table successfully.",
        }

    except Exception as e:
        print("Error:", str(e))
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
