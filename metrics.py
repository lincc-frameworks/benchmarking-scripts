import os
import argparse
import ijson
import requests
import metric_emitter


class MetricsProcessor:
    """Extracts metrics from the benchmarking JSON files and submits them to Sasquatch"""

    def __init__(self):
        self.benchmarks_filepath = "../benchmarks/output.json"

        # These are passed as environment variables from the GitHub action
        self.sasquatch_rest_proxy_url = os.environ["KAFKA_API_URL"]
        self.sasquatch_namespace = "lsst.lf"

        self.project_name = os.environ["PROJECT_NAME"]
        self.topic_name = f"{self.sasquatch_namespace}.{self.project_name}"

        self.module_metric_pairs = []  # (module_name: string, metric_value: float)

    def start(self):
        metric = self.parse_cmdline_args()
        self.extract_metric_values(metric)
        try:
            cluster_id = self.get_kafka_cluster_id()
            self.create_kafka_topic(cluster_id)
            self.submit_results_to_sasquatch()
        except Exception as e:
            print(e)

    def parse_cmdline_args(self):
        """Parses the name of the metric to compute from the command line arguments.

        Returns
        ----------
        The name of the metric to compute.
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--metric", required=True, help="The name of the metric to compute")
        return parser.parse_args().metric

    def extract_metric_values(self, metric):
        """Extracts the metric values for each module from the benchmarking JSON file.

        Parameters
        ----------
        metric : str, required
            This is the name of the metric to extract.
        """
        with open(self.benchmarks_filepath, "rb") as input_file:
            modules = list(
                filter(
                    lambda module: metric in module["stats"].keys(),
                    ijson.items(input_file, "benchmarks.item"),
                )
            )
            self.module_metric_pairs = [
                (module["name"], float(module["stats"][metric])) for module in modules
            ]
        print(f"Found {len(self.module_metric_pairs)} values for metric {metric}")

    def get_kafka_cluster_id(self):
        """Performs request to the Sasquatch REST API to obtain the Kafka Cluster ID."""
        headers = {"content-type": "application/json"}
        r = requests.get(f"{self.sasquatch_rest_proxy_url}/v3/clusters", headers=headers)
        cluster_id = r.json()["data"][0]["cluster_id"]
        print(f"Kafka cluster ID: {cluster_id}")
        return cluster_id

    def check_if_kafka_topic_exists(self):
        """Performs request to the Sasquatch REST API to check if the Kafka topic for the current project exists."""
        return (
            requests.get(
                f"{self.sasquatch_rest_proxy_url}/topics/{self.topic_name}",
                headers={"content-type": "application/vnd.kafka.v2+json"},
            ).status_code
            == 200
        )

    def create_kafka_topic(self, cluster_id):
        """Creates a Kafka topic if it does not yet exist for the current project.

        It raises an exception if the topic could not be created.

        Parameters
        ----------
        cluster_id : str, required
            This is the Kafka cluster ID.
        """
        if self.check_if_kafka_topic_exists():
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

    def submit_results_to_sasquatch(self):
        """Posts metrics to Sasquatch using the metric-emitter package."""
        for module_name, metric_val in self.module_metric_pairs:
            emitter = metric_emitter.Emitter(
                namespace=self.sasquatch_namespace,
                name=f"{self.project_name}.bench.test",
                module=module_name,
                benchmark_type="runtime",
                benchmark_unit="s",
            )
            emitter.set_value(metric_val)
            emitter.emit()


MetricsProcessor().start()
