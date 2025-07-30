import subprocess
import json
import os
import logging
import traceback

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        # Determine binary path
        task_root = os.environ.get("LAMBDA_TASK_ROOT", "/var/task")
        binary_path = os.path.join(task_root, "lambda_handler")

        # Validate binary existence and executability
        if not os.path.exists(binary_path):
            logger.error(f"Binary not found at {binary_path}")
            return {
                "statusCode": 404,
                "body": json.dumps(
                    {"error": "Binary not found", "path": binary_path}
                ),
            }

        if not os.access(binary_path, os.X_OK):
            logger.error(f"Binary not executable at {binary_path}")
            return {
                "statusCode": 403,
                "body": json.dumps(
                    {"error": "Binary not executable", "path": binary_path}
                ),
            }

        # Prepare input for subprocess
        input_data = json.dumps({"event": event, "context": str(context)})

        # Run subprocess with enhanced error handling
        try:
            result = subprocess.run(
                [binary_path],
                input=input_data,
                text=True,
                capture_output=True,
                env=os.environ.copy(),
                timeout=30,  # Add timeout to prevent hanging
            )
        except subprocess.TimeoutExpired:
            logger.error("Binary execution timed out")
            return {
                "statusCode": 504,
                "body": json.dumps({"error": "Execution timed out"}),
            }

        # Log stderr if not empty
        if result.stderr:
            print(result.stderr)

        # Log stdout if not empty
        if result.stdout:
            print(result.stdout)

    except Exception as e:
        # Catch-all error handling
        logger.error(f"Unexpected error: {str(e)}")
        logger.error(traceback.format_exc())

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": "Unexpected error",
                    "details": str(e),
                    "trace": traceback.format_exc(),
                }
            ),
        }
