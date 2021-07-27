from databricks_api.api import SCIM
from pprint import pprint

token = ""
host = ""
scim = SCIM(token=token, host=host)

try:
    all_spns = [spn["applicationId"] for spn in scim.get_sp()["Resources"]]

    # for spn in all_spns:
        # scim.delete_sp(spn)
except Exception as err:
    pprint(err)
finally:
    pprint(scim.get_sp())
