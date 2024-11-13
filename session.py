import boto3

# Global variables
REGION_NAME = 'ap-southeast-2'

def initiate_session(access_key='', secret_key='', region_name=REGION_NAME):
    """
    Initialize a Boto3 session and set up AWS service clients.

    This function initializes an AWS session using explicit credentials and returns 
    clients for commonly used AWS services, including Cognito, S3, and CloudWatch logs.

    Args:
        access_key (str, optional): The AWS access key ID. If not provided, the function prompts 
            for input. Defaults to an empty string.
        secret_key (str, optional): The AWS secret access key. If not provided, the function prompts 
            for input. Defaults to an empty string.
        region_name (str): The AWS region for the session and clients. Defaults to the value 
            of `REGION_NAME`.

    Returns:
        dict: A dictionary with initialized Boto3 clients for the following services:
            - 'cognito' (boto3.client): Client for AWS Cognito Identity Provider.
            - 's3' (boto3.client): Client for Amazon S3.
            - 'cloudwatch' (boto3.client): Client for Amazon CloudWatch Logs.

    Raises:
        botocore.exceptions.NoCredentialsError: If credentials are missing or invalid.

    Example:
        >>> clients = initiate_session(region_name='us-west-2')
        >>> cognito_client = clients['cognito']
        >>> s3_client = clients['s3']
    """

    # Manually enter credentials if not provided
    if access_key == '':
        access_key = input("AWS Access Key: ")
    
    if secret_key == '':
        secret_key = input("AWS Secret Access Key: ")

    # Set up a boto3 session with explicit credentials
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name
    )

    # Initialise boto3 client for cognito
    clients = {
        'cognito': session.client('cognito-idp', region_name=region_name),
        's3': session.client('s3', region_name=region_name),
        'cloudwatch': session.client('logs', region_name=region_name)
    }

    return clients
    