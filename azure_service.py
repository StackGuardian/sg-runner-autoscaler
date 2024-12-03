import datetime
from operator import truediv
import os
import io
import logging
from typing import List, Dict

from azure.core.exceptions import ResourceNotFoundError

from azure.identity import DefaultAzureCredential
from azure.core.exceptions import AzureError
from azure.core.polling import LROPoller
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.v2023_09_01.models import (
    VirtualMachineScaleSetVM,
    VirtualMachineScaleSet,
    VirtualMachineScaleSetVMProtectionPolicy,
)
from azure.storage.blob import BlobServiceClient


class AzureService:
    def __init__(self):
        logging.debug("Initializing Azure Service")
        self.AZURE_API_VERSION = "2023-09-01"
        self.AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")
        self.AZURE_RESOURCE_GROUP_NAME = os.getenv("AZURE_RESOURCE_GROUP_NAME")
        self.AZURE_VMSS_NAME = os.getenv("AZURE_VMSS_NAME")
        self.AZURE_BLOB_STORAGE_CONN_STRING = os.getenv(
            "AZURE_BLOB_STORAGE_CONN_STRING"
        )
        self.AZURE_BLOB_CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER_NAME")
        self.SCALE_IN_TIMESTAMP_BLOB_NAME = os.getenv(
            "SCALE_IN_TIMESTAMP_BLOB_NAME"
        )
        self.SCALE_OUT_TIMESTAMP_BLOB_NAME = os.getenv(
            "SCALE_OUT_TIMESTAMP_BLOB_NAME"
        )

        self.vmss_vms = []

        self.cred = DefaultAzureCredential()
        self.compute_client = ComputeManagementClient(
            credential=self.cred,
            subscription_id=self.AZURE_SUBSCRIPTION_ID,
            api_version=self.AZURE_API_VERSION,
        )

        self.blob_service_client = BlobServiceClient.from_connection_string(
            conn_str=self.AZURE_BLOB_STORAGE_CONN_STRING
        )
        self.container_client = self.blob_service_client.get_container_client(
            self.AZURE_BLOB_CONTAINER_NAME
        )
        self.vmss = self._fetch_vmss()
        self._refresh_vmss_vms()

    def _refresh_vmss_vms(self) -> List[VirtualMachineScaleSetVM]:
        """Gives list of VM's in scale set"""
        logging.info("fetching vmss_vms")
        vmss_vms = []
        try:
            vmss_instances_iterator = (
                self.compute_client.virtual_machine_scale_set_vms.list(
                    self.AZURE_RESOURCE_GROUP_NAME, self.AZURE_VMSS_NAME
                )
            )
            for vm in vmss_instances_iterator:
                vmss_vms.append(vm)
            self.vmss_vms = vmss_vms
        except AzureError as e:
            print(f"Error retrieving VMSS instances: {str(e)}")
            raise e

    def _fetch_vmss(self) -> VirtualMachineScaleSet:
        logging.info("STACKGUARDIAN: fetch vmss")
        try:
            vmss = self.compute_client.virtual_machine_scale_sets.get(
                self.AZURE_RESOURCE_GROUP_NAME, self.AZURE_VMSS_NAME
            )
        except AzureError as e:
            print(f"Error retrieving VMSS: {self.AZURE_VMSS_NAME}")
            raise e

        return vmss

    def update_vmss_vm(
        self,
        vm: VirtualMachineScaleSetVM,
    ):
        try:
            # Update the VM instance with the new protection policy
            vm_updated: LROPoller[VirtualMachineScaleSetVM] = (
                self.compute_client.virtual_machine_scale_set_vms.begin_update(
                    self.AZURE_RESOURCE_GROUP_NAME,
                    self.AZURE_VMSS_NAME,
                    vm.instance_id,
                    vm,
                )
            )

            print(f"Scale-in protection has been enabled for VM: {vm.name}")

        except AzureError as e:
            print(
                f"An error occurred while modifying scale-in protection: {str(e)}"
            )
            raise e

    def fetch_blob_content(self, blob_name: str) -> str:
        logging.info("STACKGUARDIAN: fetch blob content {}".format(blob_name))
        try:
            # Get a reference to the blob
            blob_client = self.container_client.get_blob_client(blob_name)

            # Download the blob's content
            download_stream = blob_client.download_blob()

            # Read the blob content (assuming it's a text file for this example)
            content = download_stream.readall()

            decoded_content = content.decode("utf-8")

            # If it's binary data, you can save it to a file or handle it differently
            logging.info("Blob content fetched successfully!")

            return decoded_content
        except ResourceNotFoundError as e:
            logging.info("blob {} not found".format(blob_name))
            return None
        except AzureError as e:
            logging.info(f"STACKGUARDIAN: Error fetching blob {e}")
            raise e

    def upload_blob_content(self, blob_name: str, data: str):
        logging.info("STACKGUARDIAN: uploading blob")
        try:
            # Get a reference to the blob (file in the container)
            blob_client = self.container_client.get_blob_client(blob_name)

            # Open the local file and upload it to the blob storage
            blob_client.upload_blob(
                data, overwrite=True
            )  # `overwrite=True` will overwrite if the blob already exists

            logging.info(
                f"STACKGUARDIAN: Blob '{blob_name}' uploaded successfully to container."
            )

        except AzureError as e:
            logging.info(f"STACKGUARDIAN: An error occurred: {e}")
            raise e

    def update_vmss_vm(
        self,
        vm: VirtualMachineScaleSetVM,
    ):
        try:
            # Update the VM instance with the new protection policy
            vm_updated: LROPoller[VirtualMachineScaleSetVM] = (
                self.compute_client.virtual_machine_scale_set_vms.begin_update(
                    self.AZURE_RESOURCE_GROUP_NAME,
                    self.AZURE_VMSS_NAME,
                    vm.instance_id,
                    vm,
                )
            )

            logging.info(
                f"STACKGUARDIAN: Scale-in protection has been enabled for VM: {vm.name}"
            )

        except AzureError as e:
            logging.info(
                f"STACKGUARDIAN: An error occurred while modifying scale-in protection: {str(e)}"
            )
            raise e

    def set_autoscale_vms(self, count):
        """Reduce scale set sku capacity"""
        logging.info("STACKGUARDIAN: set number of VM's to {}".format(count))
        # Update the VMSS instance count
        self.vmss.sku.capacity = count

        # Update the VMSS with the new instance count
        async_vmss_update: LROPoller[VirtualMachineScaleSet] = (
            self.compute_client.virtual_machine_scale_sets.begin_update(
                self.AZURE_RESOURCE_GROUP_NAME, self.AZURE_VMSS_NAME, self.vmss
            )
        )

        print(f"VMSS instance count updated to {async_vmss_update}")

    def _is_vm_scale_in_protected(self, vm: VirtualMachineScaleSetVM) -> bool:
        if vm.protection_policy != None:
            return vm.protection_policy.protect_from_scale_in
        return False

    def add_scale_in_protection(self, vm: VirtualMachineScaleSetVM):
        logging.info(
            "STACKGUARDIAN: add scale in protection to {}".format(
                vm.instance_id
            )
        )
        if not self._is_vm_scale_in_protected(vm):
            vm.protection_policy = VirtualMachineScaleSetVMProtectionPolicy(
                protect_from_scale_in=True
            )
            self.update_vmss_vm(vm)

    def find_azure_vm(self, sg_runner: Dict):
        for vm in self.vmss_vms:
            if (
                sg_runner.get("instanceDetails")[0]
                .get("ComputerName")
                .startswith(vm.os_profile.computer_name)
            ):
                return vm

    def remove_scale_in_protection(self, sg_runner: Dict):
        logging.info(
            "STACKGUARDIAN: remove scale in protection from {}".format(
                sg_runner.get("instanceDetails")[0].get("ComputerName")
            )
        )
        vm: VirtualMachineScaleSetVM = self.find_azure_vm(sg_runner)
        if vm == None:
            print(
                f"Azure VM for the stackguardian runner {sg_runner} does not exist"
            )
            return

        if self._is_vm_scale_in_protected(vm):
            vm.protection_policy = VirtualMachineScaleSetVMProtectionPolicy(
                protect_from_scale_in=False
            )
            self.update_vmss_vm(vm)

    def set_last_scale_in_event(self, timestamp: datetime.datetime):
        logging.info("STACKGUARDIAN: set last scale in event")
        self.container_client.upload_blob(
            self.SCALE_IN_TIMESTAMP_BLOB_NAME,
            io.BytesIO(timestamp.isoformat().encode()),
            overwrite=True,
        )

    def get_last_scale_in_event(self) -> datetime.datetime:
        last_scale_in_timestamp = self.fetch_blob_content(
            self.SCALE_IN_TIMESTAMP_BLOB_NAME
        )
        if last_scale_in_timestamp != None:
            timestamp = datetime.datetime.fromisoformat(last_scale_in_timestamp)
            return timestamp

    def set_last_scale_out_event(self, timestamp: datetime.datetime):
        logging.info("STACKGUARDIAN: set last scale out event")
        self.container_client.upload_blob(
            self.SCALE_OUT_TIMESTAMP_BLOB_NAME,
            io.BytesIO(timestamp.isoformat().encode()),
            overwrite=True,
        )

    def get_last_scale_out_event(self) -> datetime.datetime:
        logging.info("STACKGUARDIAN: get last scale out event")
        last_scale_out_timestamp = self.fetch_blob_content(
            self.SCALE_OUT_TIMESTAMP_BLOB_NAME
        )
        if last_scale_out_timestamp != None:
            timestamp = datetime.datetime.fromisoformat(
                last_scale_out_timestamp
            )
            return timestamp

    def count_of_existing_vms(self) -> int:
        return self.vmss.sku.capacity
