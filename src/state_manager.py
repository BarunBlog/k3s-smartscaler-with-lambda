import boto3
from botocore.exceptions import ClientError
import time

class StateManager:
    def __init__(self, table_name):
        self.table = boto3.resource('dynamodb').Table(table_name)
        self.lock_id = "scaling_lock"

    def acquire_lock(self):
        try:
            # Only update if 'is_locked' is False or doesn't exist
            self.table.put_item(
                Item={'id': self.lock_id, 'is_locked': True, 'timestamp': int(time.time())},
                ConditionExpression="attribute_not_exists(is_locked) OR is_locked = :f",
                ExpressionAttributeValues={":f": False}
            )
            return True
        except ClientError:
            return False

    def release_lock(self):
        self.table.update_item(
            Key={'id': self.lock_id},
            UpdateExpression="SET is_locked = :f",
            ExpressionAttributeValues={":f": False}
        )
