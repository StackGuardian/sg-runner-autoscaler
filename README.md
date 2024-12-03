# Stackguardian Autoscaler

The **Stackguardian Autoscaler** module provides a template to autoscale stackguardian private runners.

## Overview

`stackguardian_autoscaler.py` contains an abstract base class, `CloudService`, that describes the methods required to interact with the cloud provider. To use this module, you'll need to implement the `CloudService` class for your chosen cloud provider.

An example implementation for **Azure** can be found in the `azure_service.py` file.

## Usage

1. **Implement the Cloud Service**:  
   To autoscale on your chosen cloud platform, extend the `CloudService` class and implement the required methods.

   ```python
   class XYZCloudService(CloudService):
       def method(self):
           # Implement the logic for method
           pass

   ```

   Refer the example for Azure (in `azure_service.py`):

2. **Initialize the Autoscaler**:
   Once your cloud service is implemented, create an instance of the StackguardianAutoscaler class, passing your cloud service class to it.

   Example:

   ```python
   from stackguardian_autoscaler import StackguardianAutoscaler
   from azure_service import AzureCloudService

   autoscaler = StackguardianAutoscaler(cloud_service=AzureCloudService)
   ```

3. **Start Autoscaling**:
   Call the start() method to initiate autoscaling.

   Example:

   ```python
     autoscaler.start()
   ```
