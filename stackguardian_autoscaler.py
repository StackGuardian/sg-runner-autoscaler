from abc import ABC, abstractmethod
import time
import requests
import os
from datetime import datetime, timedelta
import logging
from typing import List, Dict


class SGRunner:
    def __init__(self, sg_runner: Dict):
        self.ip_address = sg_runner.get("instanceDetails")[0].get("IPAddress")
        self.computer_name = sg_runner.get("instanceDetails")[0].get(
            "ComputerName"
        )
        self.instance_arn = sg_runner.get("containerInstanceArn")
        self.connection_status = sg_runner.get("agentConnected")
        self.status = sg_runner.get("status")
        self.runnerID = sg_runner.get("runnerId")
        self.sg_runner = sg_runner


class CloudService(ABC):
    @abstractmethod
    def get_last_scale_out_event(self) -> datetime:
        """Get when did the last scale out event occurred"""
        pass

    @abstractmethod
    def set_last_scale_out_event(self, timestamp: datetime):
        """
        Saves the timestamp of when the last scale out event occurred
        in the some kind of storage.
        """

    @abstractmethod
    def get_last_scale_in_event(self) -> datetime:
        """Get when did the last scale in event occurred"""
        pass

    @abstractmethod
    def set_last_scale_in_event(self, timestamp: datetime):
        """
        Saves the timestamp of when the last scale in event occurred
        in the some kind of storage.
        """

    @abstractmethod
    def set_autoscale_vms(self):
        """
        Set the number of desired number of VM's in the autoscale service
        provided by the cloud service
        """
        pass

    @abstractmethod
    def count_of_existing_vms(self):
        """
        Get the existing number of VM's or runners in the cloud
        """
        pass

    @abstractmethod
    def add_scale_in_protection(self, sg_runner: SGRunner):
        """
        Add protection so that a scale in event or reduction in the capapcity of
        a autoscale group does not terminate this VM
        """
        pass

    @abstractmethod
    def remove_scale_in_protection(self, sg_runner: SGRunner):
        """
        Remove scale in protection for the VM
        """
        pass


class StackGuardianAutoscaler:
    def __init__(self, cloud_service: CloudService):
        self.SG_BASE_URI = os.getenv("SG_BASE_URI")
        self.SG_API_KEY = os.getenv("SG_API_KEY")

        self.SCALE_IN_THRESHOLD = int(os.getenv("SCALE_IN_THRESHOLD"))
        self.SCALE_IN_STEP = int(os.getenv("SCALE_IN_STEP"))

        self.SCALE_OUT_THRESHOLD = int(os.getenv("SCALE_OUT_THRESHOLD"))
        self.SCALE_OUT_STEP = int(os.getenv("SCALE_OUT_STEP"))

        self.MIN_RUNNERS = 0

        self.SG_ORG = os.getenv("SG_ORG")
        self.SG_RUNNER_GROUP = os.getenv("SG_RUNNER_GROUP")

        self.cloud_service = cloud_service

        self.scale_in_cooldown_duration = timedelta(
            minutes=int(os.getenv("SCALE_IN_COOLDOWN_DURATION"))
        )
        self.scale_out_cooldown_duration = timedelta(
            minutes=int(os.getenv("SCALE_OUT_COOLDOWN_DURATION"))
        )

        self.sg_runner_group = None
        self.queued_jobs = None
        self.sg_runners: List[SGRunner] = None
        self._refresh_sg_runner_group()
        self._refresh_queued_jobs()

    def start(self):
        logging.info("STACKGUARDIAN: starting the autoscale script")
        sg_runners = self.sg_runners
        if (
            self.queued_jobs >= self.SCALE_OUT_THRESHOLD
            or len(sg_runners) < self.MIN_RUNNERS
            or (self.queued_jobs > 0 and len(sg_runners) == 0)
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

    def scale_out(self):
        logging.info(
            "STACKGUARDIAN: scale out: queued jobs {}, number of sg runners {}, min runners {}, scale out threshold {}".format(
                self.queued_jobs,
                len(self.sg_runners),
                self.MIN_RUNNERS,
                self.SCALE_OUT_THRESHOLD,
            )
        )

        # cooldown
        last_scale_out_timestamp = self.cloud_service.get_last_scale_out_event()
        timestamp_now = datetime.now()
        if last_scale_out_timestamp != None and (
            timestamp_now - last_scale_out_timestamp
            < self.scale_in_cooldown_duration
        ):
            logging.info(
                "STACKGUARDIAN: waiting for cooldown last scale out event {}".format(
                    last_scale_out_timestamp.isoformat()
                )
            )
            return

        # Check if there are VM's in draining state
        draining_virtual_machines = self._fetch_vms_in_draining_state()

        has_scaled_out = False
        # if yes remove vm equal to scale_out_step from draining state
        if len(draining_virtual_machines) >= self.SCALE_OUT_STEP:
            for sg_runner in draining_virtual_machines[0 : self.SCALE_OUT_STEP]:
                self._update_sg_runner_status(sg_runner, "ACTIVE")
            has_scaled_out = True
        # remove all from draining state and add the number of VM's after
        # deducting the number of draining VM's from scale_out_step
        elif len(draining_virtual_machines) < self.SCALE_OUT_STEP:
            for sg_runner in draining_virtual_machines:
                self._update_sg_runner_status(sg_runner, "ACTIVE")
            self.cloud_service.set_autoscale_vms(
                self.cloud_service.count_of_existing_vms()
                + self.SCALE_OUT_STEP
                - len(draining_virtual_machines),
            )
            has_scaled_out = True

        if has_scaled_out:
            self.cloud_service.set_last_scale_out_event(datetime.now())

        self._refresh_sg_runner_group()

    def scale_in(self, scale_in_step):
        if len(self.sg_runners) == 0:
            logging.info("STACKGUARDIAN: no runners exist to scale in")
            return

        logging.info(
            "STACKGUARDIAN scale in: queued jobs {}, number of sg runners {}, min runners {}, scale in threshold {}".format(
                self.queued_jobs,
                len(self.sg_runners),
                self.MIN_RUNNERS,
                self.SCALE_IN_THRESHOLD,
            )
        )

        # Cool down for scale in
        last_scale_in_timestamp = self.cloud_service.get_last_scale_in_event()
        timestamp_now = datetime.now()
        timestamp_now.isocalendar()
        if last_scale_in_timestamp != None and (
            timestamp_now - last_scale_in_timestamp
            < self.scale_in_cooldown_duration
        ):
            logging.info(
                "STACKGUARDIAN: waiting for cooldown last scale in event {}".format(
                    last_scale_in_timestamp.isoformat()
                )
            )
            return

        # add protection to newly spawned vm's
        for sg_runner in self.sg_runners:
            self.cloud_service.add_scale_in_protection(sg_runner)

        vms_draining = self._fetch_vms_in_draining_state()

        active_drainable_vms = (
            len(self.sg_runners) - len(vms_draining) - self.MIN_RUNNERS
        )

        if active_drainable_vms < scale_in_step:
            scale_in_step = active_drainable_vms

        has_scaled_in = False
        if active_drainable_vms > 0:
            drain_count = min(scale_in_step, active_drainable_vms)
            for sg_runner in self.sg_runners:
                if drain_count == 0:
                    break

                if sg_runner.status != "DRAINING":
                    self._update_sg_runner_status(sg_runner, "DRAINING")
                    drain_count -= 1
                    has_scaled_in = True

            # if there was a runner set to draining
            if has_scaled_in:
                logging.info(
                    "STACKGUARDIAN: scaled in {}".format(
                        scale_in_step - drain_count
                    )
                )
                self.cloud_service.set_last_scale_in_event(datetime.now())

            self._refresh_sg_runner_group()

    def terminate_vms(self):
        logging.info("STACKGUARDIAN: terminating VM's")

        sg_runner_draining: Dict = self._fetch_vms_in_draining_state()
        if len(sg_runner_draining) == 0:
            return

        count = 0
        for sg_runner in sg_runner_draining:
            if (
                sg_runner["runningTasksCount"] == 0
                and sg_runner["pendingTasksCount"] == 0
            ):
                count += 1
                self.cloud_service.remove_scale_in_protection(sg_runner)
                self._deregister_sg_runner(sg_runner)
        if count > 0:
            self.cloud_service.set_autoscale_vms(
                self.cloud_service.count_of_existing_vms() - count
            )

    def _deregister_sg_runner(self, sg_runner: Dict):
        logging.info(
            "STACKGUARDIAN: deregistering sg runner {}".format(
                sg_runner.get("instanceDetails")[0].get("ComputerName")
            )
        )
        payload = {"RunnerId": sg_runner.get("runnerId")}

        uri = "{}/api/v1/orgs/{}/runnergroups/{}/deregister/".format(
            self.SG_BASE_URI, self.SG_ORG, self.SG_RUNNER_GROUP
        )

        headers = {"Authorization": "apikey {}".format(self.SG_API_KEY)}

        res = requests.post(uri, payload, headers=headers)
        res.raise_for_status()

    def _refresh_sg_runner_group(self):
        uri = (
            "{}/api/v1/orgs/{}/runnergroups/{}/?getActiveWorkflows=true".format(
                self.SG_BASE_URI, self.SG_ORG, self.SG_RUNNER_GROUP
            )
        )

        headers = {"Authorization": "apikey {}".format(self.SG_API_KEY)}

        res = requests.get(uri, headers=headers)
        res.raise_for_status()

        self.sg_runner_group = res.json()
        sg_runners = []
        for runner in self.sg_runner_group.get("msg").get("ContainerInstances"):
            sg_runners.append(SGRunner(runner))

        self.sg_runners = sg_runners

    def _refresh_queued_jobs(self) -> int:
        queued_jobs = self.sg_runner_group.get("msg").get(
            "QueuedWorkflowsCount"
        )

        if queued_jobs == None:
            raise Exception("Failed to fetch queued jobs")

        self.queued_jobs = queued_jobs

    def _update_sg_runner_status(self, sg_runner: SGRunner, status: str):
        logging.info(
            "STACKGUARDIAN: updating runner VM status {} to {}".format(
                sg_runner.computer_name, status
            )
        )
        payload = {"Status": status, "RunnerId": sg_runner.runnerID}

        headers = {"Authorization": "apikey {}".format(self.SG_API_KEY)}

        uri = "{}/api/v1/orgs/{}/runnergroups/{}/runner_status/".format(
            self.SG_BASE_URI, self.SG_ORG, self.SG_RUNNER_GROUP
        )

        res = requests.post(uri, payload, headers=headers)
        res.raise_for_status()

    def _fetch_vms_in_draining_state(self) -> List[Dict]:
        """API call to get if vm's are in draining state
        Returns VM's that are in draining state
        """
        vms_draining = []
        for sg_runner in self.sg_runners:
            if sg_runner.status == "DRAINING":
                vms_draining.append(sg_runner)

        return vms_draining
