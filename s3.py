import pandas as pd
import json
from io import BytesIO, StringIO
from typing import Optional
from botocore.exceptions import ClientError

def s3_list_objects(s3_client, bucket_name, folder_name, list_dirs=False) -> list:
    """
    List the keys of objects within a specified folder in an S3 bucket.

    This function retrieves a list of object keys within a given folder in an S3 bucket,
    optionally including directory keys. It filters out directory keys (those ending in `/`)
    unless `list_dirs` is set to `True`.

    Args:
        s3_client (boto3.client): A Boto3 S3 client instance used to interact with the S3 service.
        bucket_name (str): The name of the S3 bucket containing the folder.
        folder_name (str): The path to the folder within the S3 bucket to list objects from.
        list_dirs (bool, optional): If `True`, include keys that represent directories 
            (i.e., those ending in `/`). Defaults to `False`, excluding directories.

    Returns:
        list: A list of object keys (str) within the specified folder. Directory keys are
        included only if `list_dirs` is set to `True`. Returns an empty list if no objects are found.

    Raises:
        botocore.exceptions.ClientError: If there is an issue accessing the objects in S3.

    Example:
        >>> s3_client = boto3.client('s3')
        >>> objects = s3_list_objects(s3_client, 'my-bucket', 'path/to/folder', list_dirs=True)
        >>> for obj_key in objects:
        >>>     print(obj_key)
    """

    # List objects within the specified folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    # Create an empty list for the object keys
    keys = []

    # Check if there are contents and print them
    if 'Contents' in response:
        for obj in response['Contents']:
            # Add any object with an end key different from /
            if obj['Key'][-1] != '/':
                keys.append(obj['Key'])
            
            # Otherwise only include the object of the directories must be listed
            elif list_dirs:
                keys.append(obj['Key'])
                
    else:
        print("No objects found in the folder.")

    return keys

def s3_object_size(s3_client, bucket_name, object_name, units='MB')  -> Optional[float]:
    """
    Retrieve the size of an object in an S3 bucket, with optional unit conversion.

    This function uses an S3 client to retrieve metadata for an object specified by 
    its bucket and key (folder or file name) and returns the object's size in the specified units.
    
    Args:
        s3_client (boto3.client): A Boto3 S3 client instance used to interact with the S3 service.
        bucket_name (str): The name of the S3 bucket containing the object.
        object_name (str): The key (path) to the object within the S3 bucket.
        units (str, optional): The units to return the object size in. Supported values are:
            - 'MB' (megabytes) - default
            - 'kB' (kilobytes)
            If an unsupported unit is specified, the size will be returned in bytes.

    Returns:
        float or None: The size of the S3 object in the specified units, or `None` if the size
        information is unavailable.

    Raises:
        botocore.exceptions.ClientError: If there is an issue accessing the object in S3.

    Example:
        >>> s3_client = boto3.client('s3')
        >>> size_in_mb = s3_object_size(s3_client, 'my-bucket', 'path/to/object', units='MB')
        >>> print(f"Size: {size_in_mb} MB")
    """

    # Get object information
    response = s3_client.head_object(Bucket=bucket_name, Key=object_name)

    if 'ContentLength' in response:
        object_size = response['ContentLength']
    else:
        object_size = None

    # Convert to the appropriate unit
    # If the unit is not supported, bytes are used
    if units == 'MB':
        object_size = object_size/(1024*1024)
    elif units == 'kB':
        object_size = object_size/(1024)

    return object_size

def s3_object_sizes_df(s3_client, bucket_name, object_list) -> pd.DataFrame:
    """
    Create a DataFrame containing the sizes of specified objects in an S3 bucket.

    This function takes a list of object keys from an S3 bucket and returns a Pandas DataFrame
    with each object's key and its size in megabytes (MB), rounded to two decimal places.

    Args:
        s3_client (boto3.client): A Boto3 S3 client instance used to interact with the S3 service.
        bucket_name (str): The name of the S3 bucket containing the objects.
        object_list (list): A list of object keys (str) for which to retrieve sizes.

    Returns:
        pandas.DataFrame: A DataFrame with two columns:
            - 'key' (str): The S3 object key.
            - 'size_MB' (float): The size of the object in megabytes, rounded to two decimal places.

    Raises:
        botocore.exceptions.ClientError: If there is an issue accessing any of the objects in S3.

    Example:
        >>> s3_client = boto3.client('s3')
        >>> objects = ['path/to/object1', 'path/to/object2']
        >>> df = s3_object_sizes_df(s3_client, 'my-bucket', objects)
        >>> print(df)
    """

    # Create a DataFrame of objects with their size
    object_sizes = []
    for obj in object_list:
        object_sizes.append({
            'key': obj,
            'size_MB': round(s3_object_size(s3_client, bucket_name, obj), 2)
        })

    return pd.DataFrame(object_sizes)

def s3_read_json(s3_client, bucket_name, object_key) -> dict:
    """
    Read a JSON object from an S3 bucket and return its contents as a dictionary.

    This function retrieves a JSON file from an S3 bucket, reads its content, and 
    returns it as a dictionary.

    Args:
        s3_client (boto3.client): A Boto3 S3 client instance used to interact with the S3 service.
        bucket_name (str): The name of the S3 bucket containing the JSON object.
        object_key (str): The key (path) to the JSON object within the S3 bucket.

    Returns:
        dict: The contents of the JSON file as a dictionary.

    Raises:
        botocore.exceptions.ClientError: If there is an issue accessing the JSON object in S3.
        json.JSONDecodeError: If the content of the file is not valid JSON.

    Example:
        >>> s3_client = boto3.client('s3')
        >>> data = s3_read_json(s3_client, 'my-bucket', 'path/to/object.json')
        >>> print(data)
    """

    # Get the object from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

    # Read the object content using BytesIO
    file_content = response['Body'].read()
    return json.load(BytesIO(file_content))

def s3_upload_json(s3_client, bucket_name, object_key, dict_data):
    """
    Upload a JSON object to an S3 bucket.

    This function takes a Python dictionary, converts it to JSON format, and uploads it to
    the specified location in an S3 bucket.

    Args:
        s3_client (boto3.client): A Boto3 S3 client instance used to interact with the S3 service.
        bucket_name (str): The name of the S3 bucket where the JSON file will be uploaded.
        object_key (str): The key (path) where the JSON object will be stored in the S3 bucket.
        dict_data (dict): The dictionary containing the data to be uploaded as JSON.

    Returns:
        None: This function does not return any value. It only uploads the file to S3.

    Raises:
        botocore.exceptions.ClientError: If there is an issue uploading the JSON data to S3.

    Example:
        >>> s3_client = boto3.client('s3')
        >>> data = {'key': 'value'}
        >>> s3_load_json(s3_client, 'my-bucket', 'path/to/file.json', data)
    """
    
    # Format the JSON data
    json_data = json.dumps(dict_data)
    
    # Add the file to S3
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=json_data, ContentType='application/json')

def s3_read_csv(s3_client, bucket_name, object_key):
    """
    Load a CSV file from an S3 bucket into a Pandas DataFrame.

    This function retrieves a CSV file from an S3 bucket and loads its contents
    into a Pandas DataFrame for easy data manipulation and analysis.

    Args:
        s3_client (boto3.client): A Boto3 S3 client instance used to interact with the S3 service.
        bucket_name (str): The name of the S3 bucket containing the CSV file.
        object_key (str): The key (path) of the CSV file within the S3 bucket.

    Returns:
        pandas.DataFrame: A DataFrame containing the data from the CSV file.

    Raises:
        botocore.exceptions.ClientError: If there is an issue accessing the CSV file in S3.
        pandas.errors.EmptyDataError: If the CSV file is empty or cannot be read.

    Example:
        >>> s3_client = boto3.client('s3')
        >>> df = s3_load_csv(s3_client, 'my-bucket', 'path/to/file.csv')
        >>> print(df.head())
    """

    # Get the CSV file from S3 as bytes
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    file_content = response['Body'].read()

    # Load the CSV file into a pandas DataFrame
    csv_buffer = BytesIO(file_content)
    df = pd.read_csv(csv_buffer)

    return df


def s3_upload_csv(s3_client, bucket_name, object_key, dataframe):
    """
    Upload a Pandas DataFrame as a CSV file to an S3 bucket.

    This function converts a Pandas DataFrame to CSV format and uploads it to 
    a specified location in an S3 bucket.

    Args:
        s3_client (boto3.client): A Boto3 S3 client instance used to interact with the S3 service.
        bucket_name (str): The name of the S3 bucket where the CSV file will be uploaded.
        object_key (str): The key (path) where the CSV file will be stored in the S3 bucket.
        dataframe (pandas.DataFrame): The DataFrame to upload as a CSV file.

    Returns:
        None: This function does not return any value. It only uploads the file to S3.

    Raises:
        botocore.exceptions.ClientError: If there is an issue uploading the CSV file to S3.

    Example:
        >>> s3_client = boto3.client('s3')
        >>> df = pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})
        >>> s3_upload_csv(s3_client, 'my-bucket', 'path/to/file.csv', df)
    """
    
    # Convert DataFrame to CSV format and store it in a buffer
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    
    # Upload CSV data to S3
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
    except ClientError as e:
        print(f"Error occurred: {e.response['Error']['Message']}")


def s3_read_json_to_df(s3_client, bucket_name, object_key):
    """
    Load a JSON file from an S3 bucket into a Pandas DataFrame.

    This function retrieves a JSON file from an S3 bucket using the `s3_read_json` function, 
    and loads its contents into a Pandas DataFrame. It is expected that the JSON file's 
    structure aligns with a table format that can be represented as a DataFrame.

    Args:
        s3_client (boto3.client): A Boto3 S3 client instance used to interact with the S3 service.
        bucket_name (str): The name of the S3 bucket containing the JSON file.
        object_key (str): The key (path) of the JSON file within the S3 bucket.

    Returns:
        pandas.DataFrame: A DataFrame containing the data from the JSON file.

    Raises:
        botocore.exceptions.ClientError: If there is an issue accessing the JSON file in S3.
        ValueError: If the JSON data cannot be converted into a DataFrame.

    Example:
        >>> s3_client = boto3.client('s3')
        >>> df = s3_read_json_to_df(s3_client, 'my-bucket', 'path/to/file.json')
        >>> print(df.head())
    """
    
    # Read JSON content from S3
    json_data = s3_read_json(s3_client, bucket_name, object_key)
    
    # Load JSON content into a DataFrame
    try:
        df = pd.DataFrame(json_data)
    except ValueError as e:
        print("Error converting JSON to DataFrame:", e)
        raise

    return df
