import json
import sys
import logging
from stackguardian_autoscaler import StackGuardianAutoscaler
from aws_service import AwsService

# Setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
    # print("Received event:", event)
    # logger.info(f"Received event: {event}")

    # Process the event (this is a placeholder for your actual logic)
    autoscaler = StackGuardianAutoscaler(cloud_service=AwsService())
    try:
        autoscaler.start()

        # Create a response object
        response = {"statusCode": 200, "body": "success"}
    except Exception as e:
        response = {"statusCode": 500, "body": str(e)}

    return response


if __name__ == "__main__":
    # Read event and context from stdin when executed as binary
    try:
        input_data = json.loads(sys.stdin.read())
        event = input_data.get("event", {})
        context = input_data.get("context", "")

        result = lambda_handler(event, context)
        sys.exit(0 if result["statusCode"] == 200 else 1)
    except Exception as e:
        print(f"Exception in main: {e}", file=sys.stderr)
        error_response = {"statusCode": 500, "body": str(e)}
        print(json.dumps(error_response))
        sys.exit(1)
