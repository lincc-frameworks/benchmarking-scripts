import argparse
import ijson
import metric_emitter
import os
import requests


class MetricsProcessor:
    """Extracts metrics from the benchmarking JSON files and submits them to Sasquatch"""

    BENCHMARKS_FILEPATH = "benchmarks/output.json"
    SASQUATCH_NAMESPACE = "lsst.lf"

    # Passed as environment variables from the GitHub action
    SASQUATCH_REST_PROXY_URL = os.environ["SASQUATCH_REST_PROXY_URL"]
    BENCH_NAME = f"{os.environ['PROJECT_NAME']}.bench"

    def __init__(self):
        # Name of the Kafka topic to send the metrics to
        self.topic_name = f"{self.SASQUATCH_NAMESPACE}.{self.BENCH_NAME}"
        # Dictionary of metric values by module
        self.module_metrics = {}  # { module_name: metric_value, ... }

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
        with open(self.BENCHMARKS_FILEPATH, "rb") as input_file:
            # Filter out modules missing the computed metric
            modules = list(
                filter(
                    lambda module: metric in module["stats"].keys(),
                    ijson.items(input_file, "benchmarks.item"),
                )
            )
            # Create dictionary with the modules and respective metric
            self.module_metrics = {module["name"]: float(module["stats"][metric]) for module in modules}
            print(f"Found {len(self.module_metrics)} values for metric '{metric}'")

    def get_kafka_cluster_id(self):
        """Performs request to the Sasquatch REST API to obtain the Kafka Cluster ID."""
        headers = {"content-type": "application/json"}
        r = requests.get(f"{self.SASQUATCH_REST_PROXY_URL}/v3/clusters", headers=headers)
        cluster_id = r.json()["data"][0]["cluster_id"]
        print(f"Kafka cluster ID: {cluster_id}")
        return cluster_id

    def check_if_kafka_topic_exists(self):
        """Performs request to the Sasquatch REST API to check if the Kafka topic for the current project exists."""
        headers = {"content-type": "application/vnd.kafka.v2+json"}
        response = requests.get(f"{self.SASQUATCH_REST_PROXY_URL}/topics/{self.topic_name}", headers=headers)
        return response.status_code == 200

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
            f"{self.SASQUATCH_REST_PROXY_URL}/v3/clusters/{cluster_id}/topics",
            json=topic_config,
            headers=headers,
        )

        if response.status_code not in [200, 201]:
            raise Exception("Could not create topic!")
        else:
            print(f"Topic '{self.topic_name}' created successfully!")

    def submit_results_to_sasquatch(self):
        """Posts metrics to Sasquatch using the metric-emitter package."""
        for module_name, metric_val in self.module_metrics.items():
            emitter = metric_emitter.Emitter(
                namespace=self.SASQUATCH_NAMESPACE,
                name=self.BENCH_NAME,
                module=module_name,
                benchmark_type="runtime",
                benchmark_unit="s",
            )
            emitter.set_value(metric_val)
            emitter.emit()


MetricsProcessor().start()
