from databricks_api.utils import render_yaml, infolog

# attempt import to catch errors
from databricks_api.acl import main
from databricks_api.cluster import ClusterManagement

import os
from pprint import PrettyPrinter

pp = PrettyPrinter(indent=4).pprint
dir_path = os.path.dirname(os.path.realpath(__file__))


def test_render_yaml():
    mako_kwargs = {
        "domain": "domain.ca",
        "adf_appid": "abc123"
    }

    contents = render_yaml(f"{dir_path}\\ACL_template.yaml", mako_kwargs)
    expected = render_yaml(f"{dir_path}\\ACL_expected.yaml", {})
    pp(contents)
    pp(expected)
    assert isinstance(contents, dict)
    assert isinstance(expected, dict)
    assert contents == expected
