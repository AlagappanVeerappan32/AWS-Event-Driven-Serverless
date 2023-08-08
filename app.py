import boto3
import botocore
import os
import time


# initializing all services that are required
aws_man_console = boto3.session.Session(profile_name="AlexVersion")
s3 = aws_man_console.client(service_name="s3")
lambda_cli = aws_man_console.client(service_name="lambda")
dynamodb = aws_man_console.resource("dynamodb")


# checking if th bucket exist
def check_bucket_exists(bucket_name):
    response = s3.list_buckets()

    buckets = [bucket["Name"] for bucket in response["Buckets"]]
    return bucket_name in buckets


# creating bucket
def create_bucket(bucket_name, function):
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "ca-central-1"},
    )
    # calling fucntions inorder to setup notificiation
    grant_permission_to_s3(bucket_name, function)
    setup_s3_event_trigger(bucket_name, function)

    print("bucket cresated")


# giving permission to configure bucket notification
def grant_permission_to_s3(bucket_name, function):
    response = lambda_cli.add_permission(
        FunctionName=function,
        StatementId="5",
        Action="lambda:InvokeFunction",
        Principal="s3.amazonaws.com",
        SourceArn=f"arn:aws:s3:::{bucket_name}",
        SourceAccount="569029980056",
    )

    print("Permission granted to S3 to invoke successfully.")


# setting up the noficifation
def setup_s3_event_trigger(bucket_name, function):
    lambda_arn = function

    # Define the S3 bucket notification configuration
    notification_configuration = {
        "LambdaFunctionConfigurations": [
            {"LambdaFunctionArn": lambda_arn, "Events": ["s3:ObjectCreated:Put"]}
        ]
    }
    s3.put_bucket_notification_configuration(
        Bucket=bucket_name, NotificationConfiguration=notification_configuration
    )

    print("S3 event trigger for set up successfully.")


# uploading the files to the s3 bucket with 100ms
def upload_files_to_s3(bucket_name, folder_path):
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        # if path is present it uploads
        if os.path.isfile(file_path):
            s3.upload_file(file_path, bucket_name, file_name)
            print(f"Uploaded {file_name} to S3 bucket {bucket_name}")
            # sleeps for 100ms before uploading
            time.sleep(0.1)


# emptying the bucket after completion
def empty_bucket(bucket_name):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        # checks if the content is present or not
        if "Contents" in response:
            objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
            s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
            print(f"Bucket '{bucket_name}' emptied successfully.")
        else:
            print(f"Bucket '{bucket_name}' is already empty.")
    except botocore.exceptions.ClientError as e:
        print(f"Failed to empty bucket '{bucket_name}': {e}")


# deletes the bucket after completion
def delete_bucket(bucket_name):
    try:
        empty_bucket(bucket_name)
        s3.delete_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' deleted successfully.")
    except botocore.exceptions.ClientError as e:
        print(f"Failed to delete bucket '{bucket_name}': {e}")


# checking if the dynamo db table exist
def check_table_exists(table_name):
    existing_tables = dynamodb.meta.client.list_tables()["TableNames"]
    return table_name in existing_tables


# creating table with fileds
def create_dynamodb_table(table_name):
    table = dynamodb.create_table(
        TableName=table_name,
        # keys to create table
        KeySchema=[
            {"AttributeName": "key", "KeyType": "HASH"},  # Partition key
        ],
        AttributeDefinitions=[
            {
                "AttributeName": "key",
                "AttributeType": "S",  # String type for the partition key
            },
        ],
        # setting to max Provision in free tier
        ProvisionedThroughput={"ReadCapacityUnits": 25, "WriteCapacityUnits": 25},
    )
    table.wait_until_exists()
    print("DynamoDB table created successfully.")


# calling the main
if __name__ == "__main__":
    bucket_name1 = "sampledata-b00946176"
    lambda1 = "arn:aws:lambda:ca-central-1:569029980056:function:extractFeatures"
    file_path = "/Users/alagappanveerappan/Downloads/Serverless/Assignment3/tech/"

    bucket_name2 = "tagsb00946176"
    lambda2 = "arn:aws:lambda:ca-central-1:569029980056:function:accessDB"

    table_name = "A3-DB"

    # checking before creation
    if not check_bucket_exists(bucket_name1):
        create_bucket(bucket_name1, lambda1)
    else:
        print("bucket existed")

    if not check_bucket_exists(bucket_name2):
        create_bucket(bucket_name2, lambda2)
    else:
        print("Bucket 2 already exists")

    if not check_table_exists(table_name):
        create_dynamodb_table(table_name)
    else:
        print("DynamoDB table already exists.")

    upload_files_to_s3(bucket_name1, file_path)

    # empty_bucket(bucket_name1)
    # delete_bucket(bucket_name1)

#reference:

# [1] AWS, "Amazon S3 examples using SDK for Python (Boto3)" 2023. [Online]. Available:
#https://docs.aws.amazon.com/code-library/latest/ug/python_3_s3_code_examples.html. [Accessed 19 07
#2023].