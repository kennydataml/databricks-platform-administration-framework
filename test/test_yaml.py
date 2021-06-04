"""used for quick testing of yaml rendering
"""
from databricks_api.utils import render_yaml, logger, dir_path
from pprint import PrettyPrinter

pp = PrettyPrinter(indent=4).pprint

configuration_path = f"{dir_path}\\configuration\\"

def test_yaml():
    mako_kwargs = {
        "domain": "domain.ca",
    }
    config_files = ["ACL.yaml"]
    for cf in config_files:
        contents = render_yaml(f"{configuration_path}\\{cf}", mako_kwargs)
        pp(contents)
        assert contents

# logger.info(contents)
