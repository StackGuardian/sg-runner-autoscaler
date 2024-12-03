# Stackguardian Autoscaler

A module which can be used to autoscale private runners.

## Usage

`stackguradian_autoscaler.py` contains an abstract class `CloudService` which
needs to be implemented for the cloud service of your choice. An example can be
found for Azure Cloud in `azure_service.py`

With the required code implemented initialize the StackguardianAutoscaler class
with the cloud service class and invoke the `start()` method.

# Stackguardian Autoscaler

The **Stackguardian Autoscaler** module provides a template to autoscale stackguardian private runners.

## Overview

`stackguardian_autoscaler.py` contains an abstract base class, `CloudService`, that defines the interface for autoscaling functionality. To use this module, you'll need to implement the `CloudService` class for your chosen cloud provider.

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
