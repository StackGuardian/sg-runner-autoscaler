from stackguardian_autoscaler import StackGuardianAutoscaler
from aws_service import AwsService


def lambda_handler(event, context):
    """
    AWS Lambda function handler.

    Args:
        event (dict): The event data passed to the Lambda function.
        context (LambdaContext): The context object provided by AWS Lambda.

    Returns:
        dict: A response object containing the status code and message.
    """
    # Log the incoming event for debugging
    print("Received event:", event)

    # Process the event (this is a placeholder for your actual logic)
    autoscaler = StackGuardianAutoscaler(cloud_service=AwsService())
    try:
        autoscaler.start()

        # Create a response object
        response = {"statusCode": 200, "body": "success"}
    except Exception as e:
        response = {"statusCode": 500, "body": str(e)}

    return response
