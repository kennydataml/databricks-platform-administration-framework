import time
import multiprocessing

from databricks_cli.sdk import ApiClient
from databricks_cli.libraries.api import LibrariesApi
from databricks_cli.clusters.api import ClusterApi

from databricks_api.utils import render_yaml, parse_cmdline, CustomLogger, dir_path, logging
# , dump_yaml


class ClusterManagement:
    def __init__(self, logger, **kwargs):
        """
        :param **kwargs:
            reserved python word for unlimited parameters
            keys should only include: token, host
        :type **kwargs: dict
        """
        self.api_client = ApiClient(**kwargs)
        self.cluster_client = ClusterApi(self.api_client)
        self.libraries_client = LibrariesApi(self.api_client)
        self.logger = logger

    def create_cluster(self, cluster_specs):
        """function to build/edit cluster and start

        :param cluster_specs: cluster specs in clusterconf.yaml
        :type cluster_specs: dict
        """
        # self.cluster_client.get_cluster_by_name("unknown")

        try:
            cluster = self.cluster_client.get_cluster_by_name(
                cluster_specs["cluster_name"])

            self.logger.info(
                f"cluster {cluster['cluster_name']} exists "
                f"with id {cluster['cluster_id']}")
            self.logger.debug(cluster_specs)
            self.logger.debug(cluster)

            if not cluster_specs.items() <= cluster.items():
                self.logger.warning(
                    "cluster spec doesn't match existing cluster")

                cluster_specs['cluster_id'] = cluster['cluster_id']
                self.cluster_client.edit_cluster(cluster_specs)
            else:
                self.logger.info("cluster spec matches")
        except Exception:
            cluster = self.cluster_client.create_cluster(cluster_specs)
            self.logger.info(
                f"the cluster {cluster} is being created")
            time.sleep(30)

        cluster_id = cluster['cluster_id']
        status = self._cluster_status(cluster_id)

        while status['state'] in ["RESTARTING", "RESIZING", "TERMINATING"]:
            self.logger.info(
                f"waiting for the cluster. status {status['state']}")
            time.sleep(10)
            status = self._cluster_status(cluster_id)

        while status['state'] in ["TERMINATED", "PENDING"]:
            self.logger.info(f"cluster status {status['state']}")
            if status['state'] == "TERMINATED":
                self.logger.info(f"starting cluster, status {status['state']}")
                self.cluster_client.start_cluster(cluster_id)

            time.sleep(10)
            status = self._cluster_status(cluster_id)

        self.logger.info(
            f"cluster is up. final status: {status['state']}")

        return cluster_id

    def install_cluster_library(self, cluster_id, cluster_libraries):
        """function to install libraries on cluster

        :param cluster_id: id of cluster in Databricks to install libs on
        :type cluster_id: str
        :param cluster_libraries: clusterlib.yaml
        :type cluster_libraries: list(dict)
        """
        try:
            if not isinstance(cluster_libraries, list):
                raise ValueError(
                    f"cluster_libraries is not a list: {cluster_libraries}")

            current_libs = self.libraries_client.cluster_status(cluster_id)

            # parse the libs to match the yaml
            parsed_currentlibs = []
            if current_libs.get("library_statuses"):
                for lib in current_libs["library_statuses"]:
                    parsed_currentlibs.append(lib["library"])

            install_libs = [
                x for x in cluster_libraries if x not in parsed_currentlibs
            ]
            self.logger.info(f"install libraries: {install_libs}")
            self.libraries_client.install_libraries(
                cluster_id, install_libs)

            uninstall_libs = [
                x for x in parsed_currentlibs if x not in cluster_libraries
            ]
            self.logger.warning(f"uninstall libraries: {uninstall_libs}")
            self.libraries_client.uninstall_libraries(
                cluster_id, uninstall_libs)

        except Exception as error:
            self.logger.error(f"install_cluster_library error: {repr(error)}")

    def _cluster_status(self, cluster_id):
        """internal method to get cluster status

        :param cluster_id: id of databricks cluster
        :type cluster_id: str
        """
        try:
            status = self.cluster_client.get_cluster(cluster_id)
            return status
        except Exception as error:
            self.logger.error(f"cluster status error: {error}")

    def delete_unmanaged_clusters(self, cluster_config):
        """function to delete clusters that are not in clusterconf.yaml

        :param cluster_config: clusterconf.yaml
        :type cluster_config: list(dict)
        """
        existing_clusters = self.cluster_client.list_clusters()
        if existing_clusters.get("clusters"):
            existing_clusters = [c for c in existing_clusters.get(
                "clusters") if c["cluster_source"].upper() != "JOB"]
        self.logger.debug(existing_clusters)

        cluster_list = [c["cluster_name"] for c in cluster_config]
        remove_cluster = [
            (c["cluster_name"], c["cluster_id"]) for c in existing_clusters if c["cluster_name"] not in cluster_list
        ]

        self.logger.warning("removing unmanaged clusters:")
        self.logger.warning(remove_cluster)

        for c in remove_cluster:
            self.logger.debug(f"deleting {c[1]}")
            self.cluster_client.permanent_delete(c[1])

        return

    def main(self, cluster_specs, cluster_libraries):
        """main method to build/edit clusters and install libs

        :cluster_spec: cluster spec in clusterconf.yaml
        :type cluster_spec: dict
        :param cluster_libraries: clusterlib.yaml
        :type cluster_libraries: list(dict)
        """
        # self.logger.info("=======================================================")
        self.logger.info(
            f"create/update cluster: {cluster_specs['cluster_name']}")
        cluster_id = self.create_cluster(cluster_specs)

        self.logger.info("installing libraries")
        self.install_cluster_library(cluster_id, cluster_libraries)

        # self.logger.info("terminating cluster")
        # https://docs.databricks.com/dev-tools/api/latest/clusters.html#delete-terminate
        # self.cluster_client.delete_cluster(cluster_id)


if __name__ == "__main__":
    mplogger = multiprocessing.log_to_stderr()

    args = parse_cmdline(cmd_type="CLUSTER")
    if args.debug:
       mplogger.setLevel(logging.DEBUG)
    else:
        mplogger.setLevel(logging.INFO)

    logger = CustomLogger(mplogger)

    logger.info("""
++++++++++++++++++++++++++++++++++++++++
DATABRICKS CLUSTER MANAGEMENT
++++++++++++++++++++++++++++++++++++++++
    """)

    cluster_config = render_yaml(
        f"{dir_path}\\configuration\\{args.cluster_config_file}")
    cluster_libraries = render_yaml(
        f"{dir_path}\\configuration\\{args.cluster_library_file}")

    # how I feel everyday
    clusterfk = ClusterManagement(logger,
                                  token=args.personal_access_token,
                                  host=args.workspace_url)

    clusterfk.delete_unmanaged_clusters(cluster_config)
    # clusterfk.main(cluster_config[0], cluster_libraries)

    # pools
    if len(cluster_config) < 4:
        p = multiprocessing.Pool(processes=len(cluster_config))
    else:
        p = multiprocessing.Pool()

    args = (
        (cluster_specs, cluster_libraries)
        for cluster_specs in cluster_config
    )
    results = p.starmap_async(clusterfk.main, args)
    # cleanup
    p.close()
    p.join()

    logger.info(results.get())
    logger.info("all processes done")
