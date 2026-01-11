import os
from scaler import SmartScaler
from state_manager import StateManager
from metrics import PrometheusClient  # Assuming you have a client to query PromQL


def handler(event, context):
    state = StateManager(os.environ['DYNAMO_TABLE'])

    if not state.acquire_lock():
        print("Scaling already in progress. Skipping.")
        return

    try:
        scaler = SmartScaler()
        # Mocking metrics call for this example
        cpu = 85.0
        pending = 0

        new_cap = scaler.make_decision(cpu, pending)
        if new_cap != scaler.get_current_capacity():
            scaler.apply_scaling(new_cap)
    finally:
        state.release_lock()