import os
import argparse
import ijson
import requests
import metric_emitter


class SasquatchBenchmarkProcessor:
    def __init__(self):
        self.benchmarks_file = "benchmarks/output.json"
        self.sasquatch_rest_proxy_url = os.environ["KAFKA_API_URL"]
        self.module_metric_pairs = []  # (module_name: string, metric_value: float)
        self.topic_name = "lsst.lf.tape.bench.test"  # TODO: Change to project name

    def start(self):
        metric = self.parse_cmd_args()
        self.collect_metric_values(metric)
        try:
            cluster_id = self.get_kafka_cluster_id()
            self.create_kafka_topic(cluster_id)
            self.post_results_to_sasquatch()
        except Exception as e:
            print(e)

    def parse_cmd_args(self):
        # Parse benchmark metric to compute
        parser = argparse.ArgumentParser()
        parser.add_argument("--metric", help="Which benchmark metric to compute")
        return parser.parse_args().metric

    def collect_metric_values(self, metric):
        # Collect metric values for each test
        with open(self.benchmarks_file, "rb") as input_file:
            modules = list(
                filter(
                    lambda module: metric in module["stats"].keys(),
                    ijson.items(input_file, "benchmarks.item"),
                )
            )
            self.module_metric_pairs = [
                (module["name"], float(module["stats"][metric])) for module in modules
            ]
        print("Found %d values for metric '%s'" % (len(self.module_metric_pairs), metric))

    def get_kafka_cluster_id(self):
        # Obtain Kafka cluster id
        headers = {"content-type": "application/json"}
        r = requests.get(f"{self.sasquatch_rest_proxy_url}/v3/clusters", headers=headers)
        cluster_id = r.json()["data"][0]["cluster_id"]
        print(f"Kafka cluster ID: {cluster_id}")
        return cluster_id

    def check_if_topic_exists(self):
        # Check if topic exists
        return (
            requests.get(
                f"{self.sasquatch_rest_proxy_url}/topics/{self.topic_name}",
                headers={"content-type": "application/vnd.kafka.v2+json"},
            ).status_code
            == 200
        )

    def create_kafka_topic(self, cluster_id):
        # Create Kafka topic
        if self.check_if_topic_exists():
            return

        topic_config = {
            "topic_name": self.topic_name,
            "partitions_count": 1,
            "replication_factor": 3,
        }

        headers = {"content-type": "application/json"}

        response = requests.post(
            f"{self.sasquatch_rest_proxy_url}/v3/clusters/{cluster_id}/topics",
            json=topic_config,
            headers=headers,
        )

        if response.status_code not in [200, 201]:
            raise Exception("Could not create Kafka topic!")

    def post_results_to_sasquatch(self):
        # Post data to Sasquatch
        for module_name, metric_val in self.module_metric_pairs:
            emitter = metric_emitter.Emitter(
                namespace="lsst.lf",
                name="tape.bench.test",
                module=module_name,
                benchmark_type="runtime",
                benchmark_unit="s",
            )
            emitter.set_value(metric_val)
            emitter.emit()


SasquatchBenchmarkProcessor().start()
