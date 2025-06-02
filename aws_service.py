from typing import Optional, List
from botocore.exceptions import ClientError
import boto3
import os
import logging
from datetime import datetime
from mypy_boto3_autoscaling import AutoScalingClient
from mypy_boto3_s3 import S3Client
from stackguardian_autoscaler import CloudService, SGRunner


class AwsService(CloudService):
    def __init__(self):
        self.asg_client: AutoScalingClient = boto3.client("autoscaling")
        self.s3_client: S3Client = boto3.client("s3")
        self.ec2_client = boto3.client("ec2")

        self.ASG_NAME = os.getenv("AWS_ASG_NAME")
        self.BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
        self.SCALE_IN_TIMESTAMP_OBJECT_NAME = os.getenv(
            "SCALE_IN_TIMESTAMP_BLOB_NAME"
        )
        self.SCALE_OUT_TIMESTAMP_OBJECT_NAME = os.getenv(
            "SCALE_OUT_TIMESTAMP_BLOB_NAME"
        )

        self.asg_vms = self._get_vms_in_asg()

    def _get_vms_in_asg(self) -> Optional[List[dict]]:
        # Describe the Auto Scaling Group and get the instances
        response = self.asg_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[self.ASG_NAME]
        )

        # Extracting instances
        if response["AutoScalingGroups"]:
            asg = response["AutoScalingGroups"][0]
            asg_instances = asg["Instances"]

            instance_ids = []
            for asg_instance in asg_instances:
                instance_ids.append(asg_instance["InstanceId"])

            if len(instance_ids) == 0:
                return None

            response = self.ec2_client.describe_instances(
                InstanceIds=instance_ids
            )

            if not response:
                return None

            instances = []
            for reservation in response["Reservations"]:
                for instance in reservation["Instances"]:
                    instances.append(instance)

            return instances
        else:
            print(f"Auto Scaling Group '{self.ASG_NAME}' not found.")
            return []

    def _fetch_s3_blob(self, bucket_name, object_name):
        try:
            response = self.s3_client.get_object(
                Bucket=bucket_name, Key=object_name
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
        except Exception as e:
            print(f"Error fetching S3 object: {e}")
            return None

    def get_last_scale_out_event(self) -> Optional[datetime]:
        logging.info("STACKGUARDIAN: get last scale out event")
        # Fetch the scale-out timestamp from S3
        blob_content = self._fetch_s3_blob(
            self.BUCKET_NAME,
            self.SCALE_OUT_TIMESTAMP_OBJECT_NAME,
        )

        if not blob_content:
            return None

        last_scale_out_timestamp = datetime.fromisoformat(
            blob_content.decode("utf-8")
        )
        return last_scale_out_timestamp

    def set_last_scale_out_event(self, timestamp) -> Optional[datetime]:
        logging.info("STACKGUARDIAN: set last scale out event")
        self.s3_client.put_object(
            Bucket=self.BUCKET_NAME,
            Key=self.SCALE_OUT_TIMESTAMP_OBJECT_NAME,
            Body=datetime.isoformat(timestamp),
        )

    def set_autoscale_vms(self, count_of_vms: int):
        _ = self.asg_client.set_desired_capacity(
            AutoScalingGroupName=self.ASG_NAME,
            DesiredCapacity=count_of_vms,
        )

    def get_last_scale_in_event(self) -> Optional[datetime]:
        logging.info("STACKGUARDIAN: get last scale in event")
        # Fetch the scale-in timestamp from S3
        blob_content = self._fetch_s3_blob(
            self.BUCKET_NAME,
            self.SCALE_IN_TIMESTAMP_OBJECT_NAME,
        )

        if not blob_content:
            return None

        last_scale_in_timestamp = datetime.fromisoformat(
            blob_content.decode("utf-8")
        )
        return last_scale_in_timestamp

    def set_last_scale_in_event(self, timestamp):
        logging.info("STACKGUARDIAN: set last scale in event")
        self.s3_client.put_object(
            Bucket=self.BUCKET_NAME,
            Key=self.SCALE_IN_TIMESTAMP_OBJECT_NAME,
            Body=datetime.isoformat(timestamp),
        )

    def _find_aws_vm(self, sg_runner: SGRunner) -> Optional[dict]:
        for instance in self.asg_vms:
            if sg_runner.computer_name == instance["PrivateDnsName"]:
                return instance

    def add_scale_in_protection(self, sg_runner):
        instance = self._find_aws_vm(sg_runner)

        _ = self.asg_client.set_instance_protection(
            AutoScalingGroupName=self.ASG_NAME,
            InstanceIds=[instance["InstanceId"]],
            ProtectedFromScaleIn=True,
        )

    def remove_scale_in_protection(self, sg_runner):
        instance = self._find_aws_vm(sg_runner)

        _ = self.asg_client.set_instance_protection(
            AutoScalingGroupName=self.ASG_NAME,
            InstanceIds=[instance["InstanceId"]],
            ProtectedFromScaleIn=False,
        )

    def count_of_existing_vms(self) -> Optional[int]:
        instances = self._get_vms_in_asg()
        return len(instances) if instances else 0
