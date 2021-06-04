import argparse
import yaml
import logging
from pprint import pformat
from mako.template import Template
import os

dir_path = os.path.dirname(os.path.realpath(__file__))

logging.basicConfig(level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG)
LOGGER_NAME = "databricks_api"


def formatlog(func):
    """decorator to format log message
    """
    def wrapper(*args):
        if len(args) == 1:
            msg = args[0]
            return func(
                pformat(msg) if not isinstance(msg, str) else msg
            )
        elif len(args) == 2:
            _, msg = args
            return func(
                _,
                pformat(msg) if not isinstance(msg, str) else msg
            )
        else:
            raise ValueError(
                f"invalid number of args for formatlog: len(args) == {len(args)}")

    return wrapper


class CustomLogger:
    def __init__(self, logger):
        self.logger = logger

    @formatlog
    def info(self, msg):
        """info log

        :param msg: log message
        :type msg: str or any
        """
        self.logger.info(msg)

    @formatlog
    def warning(self, msg):
        """warning log

        :param msg: log message
        :type msg: str or any
        """
        self.logger.warning(msg)

    @formatlog
    def error(self, msg):
        """error log

        :param msg: log message
        :type msg: str or any
        """
        self.logger.error(msg)

    @formatlog
    def critical(self, msg):
        """critical log

        :param msg: log message
        :type msg: str or any
        """
        self.logger.critical(msg)

    @formatlog
    def debug(self, msg):
        """debug log

        :param msg: log message
        :type msg: str or any
        """
        self.logger.debug(msg)


logger = CustomLogger(logging.getLogger(LOGGER_NAME))


def render_yaml(filepath, kwargs={}):
    """function to render yaml using mako

    :param filepath: path to file
    :type filepath: str
    :param kwargs: parameters to substitute
    :type kwargs: dict

    :return: yaml contents
    :type return: dict
    """
    try:
        if kwargs:
            return yaml.safe_load(
                Template(filename=filepath).render(**kwargs)
            )
        else:
            with open(filepath, 'r') as stream:
                return yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        logger.error(exc)


def parse_cmdline(cmd_type=None):
    """function for command line args

    :return: command line arguments
    :type return: obj
    """
    parser = argparse.ArgumentParser(
        description="Databricks Workspace ACL Configuration")
    parser.add_argument('-pat', '--personal_access_token', type=str,
                        required=True,
                        help='Personal Access Token from Admin Console')
    parser.add_argument('-wu', '--workspace_url', type=str,
                        required=True, help='Workspace URL')
    parser.add_argument('--debug', action='store_true',
                        help='enable debug logging (default: False)')

    if cmd_type == "ACL":
        parser.add_argument('--remove', action='store_true',
                        help='remove unmanaged groups or users (default: False)')
        parser.add_argument('-af', '--acl_file', type=str,
                            default="ACL.yaml",
                            help="Default is ACL.yaml")
        # parser.add_argument('-d', '--domain', type=str, required=True,
        #                     help='FQDN of environment')

    if cmd_type == "CLUSTER":
        parser.add_argument('-ccf', '--cluster_config_file', type=str,
                            default="clusterconf.yaml",
                            help="Default is clusterconf.yaml")
        parser.add_argument('-clf', '--cluster_library_file', type=str,
                            default="clusterlib.yaml",
                            help="Default is clusterlib.yaml")

    return parser.parse_args()


def dump_yaml(data, filename="myfile.yaml"):
    with open(filename, "w") as f:
        yaml.dump(data, f, indent=2)


def trycatch(func):
    """decorator to catch exception
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error("Got error! ", repr(e))
            return ""

    return wrapper
