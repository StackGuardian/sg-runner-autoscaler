from typing import List, Dict
import requests
import os
import datetime
import logging

from azure_service import AzureService

import azure.functions as func

app = func.FunctionApp()

# TODO: Configure what happens when VM's are registered but unhealthy. Set them to draining
# TODO: Configure VM's are not registered but are unhealthy.
# TODO: What happens when a VM is registered but not connected. Solution: Set it as draining
# TODO: What happens when there are VM's that are not registered but exist in the scale set.


@app.timer_trigger(
    schedule="* * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def timer_trigger(myTimer: func.TimerRequest) -> None:
    cloud_service = AzureService()
    sg_autoscaler = StackGuardianAutoscaler(cloud_service)

    sg_autoscaler.start()


class StackGuardianAutoscaler:
    def __init__(self, cloud_service: AzureService):
        self.SG_BASE_URI = os.getenv("SG_BASE_URI")
        self.SG_API_KEY = os.getenv("SG_API_KEY")

        self.SCALE_IN_THRESHOLD = int(os.getenv("SCALE_IN_THRESHOLD"))
        self.SCALE_IN_STEP = int(os.getenv("SCALE_IN_STEP"))

        self.SCALE_OUT_THRESHOLD = int(os.getenv("SCALE_OUT_THRESHOLD"))
        self.SCALE_OUT_STEP = int(os.getenv("SCALE_OUT_STEP"))

        self.MIN_VMSS_VMS = 0

        self.SG_ORG = os.getenv("SG_ORG")
        self.SG_RUNNER_GROUP = os.getenv("SG_RUNNER_GROUP")

        self.cloud_service = cloud_service

        self.scale_in_cooldown_duration = datetime.timedelta(
            minutes=int(os.getenv("SCALE_IN_COOLDOWN_DURATION"))
        )
        self.scale_out_cooldown_duration = datetime.timedelta(
            minutes=int(os.getenv("SCALE_OUT_COOLDOWN_DURATION"))
        )

        self.sg_runner_group = None
        self.queued_jobs = None
        self.refresh_sg_runner_group()
        self.refresh_queued_jobs()

    def start(self):
        logging.info("starting the autoscale script")
        if (
            self.queued_jobs >= self.SCALE_OUT_THRESHOLD
            or len(self._fetch_sg_runners()) == 0
        ):
            self.scale_out()
            # incase there are any draining VM's left to delete even after scaling out depending on the scale_out_step and scale_in_step.
            self.terminate_vms()
        elif self.queued_jobs <= self.SCALE_IN_THRESHOLD:
            self.scale_in(self.SCALE_IN_STEP)
            # delete draining VM's
            self.terminate_vms()
        else:
            self.terminate_vms()

    def refresh_sg_runner_group(self):
        uri = (
            "{}/api/v1/orgs/{}/runnergroups/{}/?getActiveWorkflows=true".format(
                self.SG_BASE_URI, self.SG_ORG, self.SG_RUNNER_GROUP
            )
        )

        headers = {"Authorization": "apikey {}".format(self.SG_API_KEY)}

        res = requests.get(uri, headers=headers)
        res.raise_for_status()

        self.sg_runner_group = res.json()

    def _fetch_sg_runners(self):
        return self.sg_runner_group.get("msg").get("ContainerInstances")

    def refresh_queued_jobs(self) -> int:
        queued_jobs = self.sg_runner_group.get("msg").get(
            "QueuedWorkflowsCount"
        )

        if queued_jobs == None:
            raise Exception("Failed to fetch queued jobs")

        self.queued_jobs = queued_jobs

    def scale_out(self):
        logging.info("scaling out")

        # cooldown
        last_scale_out_timestamp = self.cloud_service.get_last_scale_out_event()
        timestamp_now = datetime.datetime.now()
        if last_scale_out_timestamp != None and (
            timestamp_now - last_scale_out_timestamp
            < self.scale_in_cooldown_duration
        ):
            return

        # Check if there are VM's in draining state
        draining_virtual_machines = self.fetch_vms_in_draining_state()

        has_scaled_out = False
        # if yes remove vm equal to scale_out_step from draining state
        if len(draining_virtual_machines) >= self.SCALE_OUT_STEP:
            for sg_runner in draining_virtual_machines[0 : self.SCALE_OUT_STEP]:
                self.update_sg_runner_status(sg_runner, "ACTIVE")
            has_scaled_out = True
        # remove all from draining state and add the number of VM's after
        # deducting the number of draining VM's from scale_out_step
        elif len(draining_virtual_machines) < self.SCALE_OUT_STEP:
            for sg_runner in draining_virtual_machines:
                self.update_sg_runner_status(sg_runner, "ACTIVE")
            self.cloud_service.set_autoscale_vms(
                self.SCALE_OUT_STEP - len(draining_virtual_machines),
            )
            has_scaled_out = True

        if has_scaled_out:
            self.cloud_service.set_last_scale_out_event(datetime.datetime.now())

        self.refresh_sg_runner_group()

    def update_sg_runner_status(self, sg_runner: Dict, status: str):
        logging.info(
            "updating runner status {} to {}".format(
                sg_runner.get("instanceDetails")[0].get("ContainerName"), status
            )
        )
        """Adds scale in protection policy for vm"""
        payload = {"Status": status, "RunnerId": sg_runner.get("runnerId")}

        headers = {"Authorization": "apikey {}".format(self.SG_API_KEY)}

        uri = "{}/api/v1/orgs/{}/runnergroups/{}/runner_status/".format(
            self.SG_BASE_URI, self.SG_ORG, self.SG_RUNNER_GROUP
        )

        res = requests.post(uri, payload, headers=headers)
        res.raise_for_status()

    def fetch_vms_in_draining_state(self) -> List[Dict]:
        """API call to get if vm's are in draining state
        Returns VM's that are in draining state
        """
        vms_draining = []
        for sg_runner in self._fetch_sg_runners():
            if sg_runner.get("status") == "DRAINING":
                vms_draining.append(sg_runner)

        return vms_draining

    def scale_in(self, scale_in_step):
        logging.info("scale in")

        # Cool down for scale in
        last_scale_in_timestamp = self.cloud_service.get_last_scale_in_event()
        timestamp_now = datetime.datetime.now()
        if last_scale_in_timestamp != None and (
            timestamp_now - last_scale_in_timestamp
            < self.scale_in_cooldown_duration
        ):
            logging.debug("skipping due to last scale in event")
            return

        sg_runners = self._fetch_sg_runners()

        if len(sg_runners) <= self.MIN_VMSS_VMS:
            return

        if len(sg_runners) < scale_in_step:
            scale_in_step = len(sg_runners)

        # add protection to newly spawned vm's
        for vm in self.cloud_service.vmss_vms:
            self.cloud_service.add_scale_in_protection(vm)

        vms_draining = self.fetch_vms_in_draining_state()

        active_drainable_vms = (
            len(self._fetch_sg_runners())
            - len(vms_draining)
            - self.MIN_VMSS_VMS
        )
        has_scaled_in = False
        if active_drainable_vms > 0:
            drain_count = scale_in_step
            for sg_runner in sg_runners:
                if drain_count == 0 or active_drainable_vms == 0:
                    break

                if sg_runner.get("status") != "DRAINING":
                    drain_count -= 1
                    active_drainable_vms -= 1
                    has_scaled_in = True
                    self.update_sg_runner_status(sg_runner, "DRAINING")

            # if there was a runner set to draining or scaled in
            if has_scaled_in:
                self.cloud_service.set_last_scale_in_event(
                    datetime.datetime.now()
                )

            self.refresh_sg_runner_group()

    def terminate_vms(self):
        logging.info("terminating VM's")
        sg_runner_draining: Dict = self.fetch_vms_in_draining_state()
        count = 0
        for sg_runner in sg_runner_draining:
            if (
                sg_runner["runningTasksCount"] == 0
                and sg_runner["pendingTasksCount"] == 0
            ):
                count += 1
                # TODO: replace this check with computer name
                # send the computer name to azure service for the protection to be removed
                self.cloud_service.remove_scale_in_protection(sg_runner)
                self.deregister_sg_runner(sg_runner)
        if count > 0:
            self.cloud_service.set_autoscale_vms(
                self.cloud_service.vmss.sku.capacity - count
            )

    def deregister_sg_runner(self, sg_runner: Dict):
        logging.info(
            "deregistering sg runner {}",
        )
        payload = {"RunnerId": sg_runner.get("runnerId")}

        uri = "{}/api/v1/orgs/{}/runnergroups/{}/deregister/".format(
            self.SG_BASE_URI, self.SG_ORG, self.SG_RUNNER_GROUP
        )

        headers = {"Authorization": "apikey {}".format(self.SG_API_KEY)}

        res = requests.post(uri, payload, headers=headers)
        res.raise_for_status()
