# AWS-Event-Driven-Serverless
Event-Driven Serverless Application with AWS Lambda: Develop and deploy a serverless application using AWS Lambda, S3, and DynamoDB. Automate file processing, named entity extraction, and database updates. Efficiently utilize AWS services for scalable and efficient data processing.
---

## Overview

Welcome to the Event-Driven Serverless Application repository. This professional project showcases the development of an event-driven serverless application using AWS Lambda, S3, and DynamoDB. The application automates file processing, named entity extraction, and database updates, resulting in efficient data processing and management.

### Features

- **Automated File Processing:** Upload files to an S3 bucket one at a time with a delay of 100 milliseconds, ensuring controlled and efficient processing.
- **Named Entity Extraction:** Utilize the `extractFeatures` Lambda function to extract named entities from files and create a JSON array of named entities for each file.
- **Dynamic Tagging:** Generated JSON arrays are stored in a new bucket `TagsB00xxxxxx` with appropriate tagging.
- **Database Updates:** The `accessDB` Lambda function automatically updates a DynamoDB database table by reading each named entity JSON file and populating the database table with key-value pairs.

### Workflow

1. Upload files to the `SampleDataB00xxxxxx` S3 bucket.
2. The `extractFeatures` Lambda function extracts named entities and generates JSON arrays.
3. JSON arrays are stored as files in the `TagsB00xxxxxx` bucket.
4. The `accessDB` Lambda function reads JSON files and updates a DynamoDB database table with key-value pairs.

### Testing

Comprehensive functional testing has been conducted to ensure the application's accuracy and efficiency. Test cases have been crafted to validate each stage of the application, from file uploads to database updates.

---

## How to Use

1. Clone this repository
2. Explore and modify Lambda function code as necessary.
3. Configure AWS services (S3, DynamoDB, Lambda) based on the provided steps.
4. Test the entire application by uploading files, triggering Lambda functions, and monitoring database updates.
5. Document proper test cases and record screenshots to validate the application's functionality.


