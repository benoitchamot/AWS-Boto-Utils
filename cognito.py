import boto3
from botocore.exceptions import ClientError

class CognitoUser():
    """
    A class to manage and authenticate AWS Cognito users.

    This class provides methods to authenticate users, initiate password resets, confirm new passwords,
    and handle other user-related actions in AWS Cognito.

    Attributes:
        user_pool (str): The Cognito user pool ID.
        client_id (str): The Cognito app client ID.
        region (str): The AWS region where the Cognito user pool is located.
    """

    def __init__(self, user_pool, client_id, region):
        """
        Initialize the CognitoUser with user pool, client ID, and region information.

        Args:
            user_pool (str): The Cognito user pool ID.
            client_id (str): The Cognito app client ID.
            region (str): The AWS region where the Cognito user pool is located.
        """

        self.user_pool = user_pool
        self.client_id = client_id
        self.region = region

    def print_attributes(self):
        """
        Print the attributes of the Cognito user, including user pool ID, client ID, and region.
        """

        print("user_pool", self.user_pool, sep=': ')
        print("client_id", self.client_id, sep=': ')
        print("region", self.region, sep=': ')

    def get_token(self, username, password):
        """
        Authenticate a user and retrieve their JWT access token.

        Args:
            username (str): The username of the Cognito user.
            password (str): The password of the Cognito user.

        Returns:
            str or None: The JWT access token (IdToken) if authentication is successful; 
                         otherwise, None if there is an error.

        Raises:
            ClientError: If authentication fails due to an error in the Cognito API.
        """

        # Create a Cognito session
        client = boto3.client('cognito-idp', region_name=self.region)
        
        try:
            # Authenticate the user with Cognito
            response = client.initiate_auth(
                ClientId=self.client_id,
                AuthFlow='USER_PASSWORD_AUTH',
                AuthParameters={
                    'USERNAME': username,
                    'PASSWORD': password
                }
            )
            # Extract and return the Access Token (Bearer Token)
            return response['AuthenticationResult']['IdToken']
        
        except ClientError as e:
            print(f"Error occurred: {e.response['Error']['Message']}")
            return None
        
    def test_login(self, username, password, new_password):
        """
        Attempt to authenticate a user and handle new password requirements if necessary.

        Args:
            username (str): The username of the Cognito user.
            password (str): The current password of the Cognito user.
            new_password (str): The new password if a password change is required.

        Returns:
            None

        Raises:
            ClientError: If there is an error in the Cognito authentication process.
        """

        client = boto3.client('cognito-idp', region_name=self.region)

        try:
            # Try to authenticate the user with USER_PASSWORD_AUTH flow
            response = client.initiate_auth(
                ClientId=self.client_id,
                AuthFlow='USER_PASSWORD_AUTH',
                AuthParameters={
                    'USERNAME': username,
                    'PASSWORD': password
                }
            )

            # Check if a password change is required
            if 'ChallengeName' in response and response['ChallengeName'] == 'NEW_PASSWORD_REQUIRED':
                print("Password change is required.")

                # Respond to the new password required challenge
                change_password_response = client.respond_to_auth_challenge(
                    ClientId=self.client_id,
                    ChallengeName='NEW_PASSWORD_REQUIRED',
                    Session=response['Session'],
                    ChallengeResponses={
                        'USERNAME': username,
                        'NEW_PASSWORD': new_password
                    }
                )

                # After successful password change, try to get the access token again
                if 'AuthenticationResult' in change_password_response:
                    print("Password changed successfully.")
                else:
                    print("Password change failed.")

            elif 'AuthenticationResult' in response:
                # If no password change is required, authentication was successful
                print("No password change required.")
            else:
                print("Unexpected challenge received.")
        
        except ClientError as e:
            print(f"Error occurred: {e.response['Error']['Message']}")

    def reset_password(self, username):
        """
        Initiate the password reset flow for a Cognito user.

        Args:
            username (str): The username of the Cognito user requesting a password reset.

        Returns:
            None

        Raises:
            ClientError: If there is an error initiating the password reset process.
        """

        client = boto3.client('cognito-idp', region_name=self.region)
        
        try:
            # Start the password reset flow
            response = client.forgot_password(
                ClientId=self.client_id,
                Username=username
            )
            print("Password reset initiated. Check your email for the confirmation code.")
        except ClientError as e:
            print(f"Error occurred: {e.response['Error']['Message']}")
        
    def confirm_password(self, username, verification_code, new_password):
        """
        Confirm a password reset with a verification code for a Cognito user.

        Args:
            username (str): The username of the Cognito user resetting the password.
            verification_code (str): The code sent to the user for password reset confirmation.
            new_password (str): The new password to set.

        Returns:
            None

        Raises:
            ClientError: If there is an error confirming the password reset.
        """

        client = boto3.client('cognito-idp', region_name=self.region)
        
        try:
            # Confirm the password reset with the confirmation code
            response = client.confirm_forgot_password(
                ClientId=self.client_id,
                Username=username,
                ConfirmationCode=verification_code,
                Password=new_password
            )
            print("Password has been reset successfully.")
        except ClientError as e:
            print(f"Error occurred: {e.response['Error']['Message']}")