"""
This package provides utilities for working with AWS services including 
session initialization, S3, CloudWatch, and Cognito integrations.

Modules:
    - session: Contains functions for initiating an authenticated AWS session.
    - cloudwatch: Contains utilities for interacting with AWS CloudWatch.
    - s3: Provides functions for interacting with Amazon S3.
    - cognito: Defines the CognitoUser used by AWS Cognito users.

Example usage:
    from my_package import initiate_session, CognitoUser
    session_clients = initiate_session()
    cognito_user = CognitoUser(session_clients['cognito'])
"""

from .session import initiate_session
from .cloudwatch import *
from .s3 import *
from .cognito import CognitoUser