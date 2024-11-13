# AWS-Boto-Utils
A series of functions to interface with AWS services through the boto3 SDK. Currently supports:
- S3
- Cognito (user only, not admin)
- CloudWatch (see disclaimer below)

## Disclaimer and known issues
This package is a work in progress, and the author provides no guarantees regarding its use.

In particular, the following issues are known and will be addressed in further releases:
- `initiate_session()` in `session.py` assumes that the AWS user has the ability to create and use clients for all the supported services
- the functions in `cloudwatch.py` use a dictionary (`log_groups`) with a specific structure that is yet to be documented
- the methods in the `CognitoUser()` class can only be used with users already created in a Cognito user pool, no admin functions are supported at this stage
- documentation and commenting could be improved.

Feel free to fork, or clone this repo - response time to pull requests and questions is not guaranteed :)

## Dependencies
This project requires the following Python packages:
- `boto3`: For AWS S3 interactions
- `pandas`: For data manipulation
- `json`: For JSON file manipulation
- `io`: For data strcuture buffering

## Overview of functionality
### Session
Use `session.py` to:
- Initialize a Boto3 session 
- Return AWS service clients

The `initiate_session()` should be used before calling the functions from the other modules, with the exception of `CognitoUser()`, which is a standalone class.

### S3
Use `s3.py` to:
- get the keys and sizes of objects in an S3 bucket
- read JSON files from S3 into pandas DataFrames
- read CSV files from S3 into pandas DataFrames
- upload JSON structures and CSV to S3

### CloudWatch
Use `cloudwatch.py` to:
- list all streams related to a log group
- list all events from a stream

### Cognito
Use `cognito.py` to:
- get an ID token for a given application
- perform an initial login and password reset
- request a password change