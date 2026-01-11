import requests
import os

class PrometheusClient:
    def __init__(self):
        self.url = os.environ['PROMETHEUS_URL']

    def query_metric(self, promql_query):
        response = requests.get(f"{self.url}/api/v1/query", params={'query': promql_query})
        response.raise_for_status()
        results = response.json()['data']['result']

        if not results:
            return 0
        return float(results[0]['value'][1])

    def get_avg_cpu(self):
        # Query: Average CPU usage across all nodes over the last 10 minutes
        # Requirements: "Scale DOWN when: Average CPU < 30% for > 10 minutes"
        query = '100 - (avg by (instance) (irate(node_cpu_seconds_total{mode="idle"}[10m])) * 100)'
        return self.query_metric(query)

    def get_pending_pods(self):
        # Requirements: "Scale UP when: pending pods exist"
        query = 'count(kube_pod_status_phase{phase="Pending"})'
        return self.query_metric(query)