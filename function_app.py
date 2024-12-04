from azure_service import AzureService
from stackguardian_autoscaler import StackGuardianAutoscaler

import azure.functions as func

app = func.FunctionApp()

# TODO: Set VM's are registered but unhealthy to draining for termination
# TODO: Delete VM's are not registered but are unhealthy.
# TODO: VM is registered but not connected. Solution: Set it as draining
# TODO: VM's that are not registered but exist in the scale set. Terminate them


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
