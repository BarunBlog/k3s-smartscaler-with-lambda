import boto3
import os

class SmartScaler:
    def __init__(self):
        self.asg_client = boto3.client('autoscaling')
        self.asg_name = os.environ['ASG_NAME']
        self.min_nodes = 2
        self.max_nodes = 10

    def get_current_capacity(self):
        response = self.asg_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[self.asg_name]
        )
        return response['AutoScalingGroups'][0]['DesiredCapacity']

    def make_decision(self, cpu_utilization, pending_pods_count):
        current = self.get_current_capacity()

        # Scale Up (High CPU or Pending Pods)
        if cpu_utilization > 70 or pending_pods_count > 0:
            if current < self.max_nodes:
                return current + 1

        # Scale Down (Low CPU - Note: 10m window handled by Lambda trigger)
        elif cpu_utilization < 30:
            if current > self.min_nodes:
                return current - 1

        return current  # No change

    def apply_scaling(self, new_capacity):
        print(f"Updating ASG {self.asg_name} to capacity: {new_capacity}")
        self.asg_client.set_desired_capacity(
            AutoScalingGroupName=self.asg_name,
            DesiredCapacity=new_capacity,
            HonorCooldown=True
        )
