import json
import boto3

# intitalizing the services
s3 = boto3.client("s3")
comprehend = boto3.client("comprehend")


# detect the entities from the file
def detect_named_entities(text):
    # calling comphrened API to detect entitites
    response = comprehend.detect_entities(Text=text, LanguageCode="en")

    named_entities = {}
    for entity in response["Entities"]:
        # Check if the entity name starts with an uppercase letter or is in full uppercase or contains numbers
        if entity["Text"][0].isupper() or entity["Text"].isupper():
            named_entity = entity["Text"]
            named_entities[named_entity] = named_entities.get(named_entity, 0) + 1

    return named_entities


def lambda_handler(event, context):
    # Retrieve the bucket and object key from the S3 event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]

    # Get the file content from S3
    response = s3.get_object(Bucket=bucket, Key=object_key)
    content = response["Body"].read().decode("utf-8")
    print(content)

    # Detect named entities using AWS Comprehend
    named_entities = detect_named_entities(content)
    print(named_entities)

    # fom file name we are concating "ne" to create the filename
    filename = object_key.split(".")[0] + "ne"
    json_array = {filename: named_entities}

    second_bucket = "tagsb00946176"
    second_key = filename + ".json"
    # calling s3 bucket to put the file
    s3.put_object(Bucket=second_bucket, Key=second_key, Body=json.dumps(json_array))

    return {
        "statusCode": 200,
        "body": "Named entities processed and JSON array uploaded successfully.",
    }
