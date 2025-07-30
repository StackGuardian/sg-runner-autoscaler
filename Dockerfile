# Use AWS Lambda Python 3.13 runtime base image
FROM public.ecr.aws/lambda/python:3.13

# Copy requirements file
COPY aws_requirements.txt ${LAMBDA_TASK_ROOT}/

# Install dependencies
RUN pip install --no-cache-dir -r aws_requirements.txt

# Copy application code
COPY stackguardian_autoscaler.py ${LAMBDA_TASK_ROOT}/
COPY aws_service.py ${LAMBDA_TASK_ROOT}/
COPY lambda.py ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler
CMD ["lambda.lambda_handler"]