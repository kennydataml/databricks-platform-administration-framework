from databricks_api.base import APIBase, PermissionsBase
from databricks_api.utils import logger
# , trycatch


class SCIM(APIBase):
    """https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/scim/#resource-url
    """

    def __init__(self, **kwargs):
        logger.info("""
++++++++++++++++++++++++++++++++++++++++
SCIM API
++++++++++++++++++++++++++++++++++++++++
        """)
        super().__init__(**kwargs)
        self.headers["Content-Type"] = "application/scim+json"
        self.headers["Accept"] = 'application/scim+json'

        self.scim_api = "preview/scim/v2"
        self.scim_url = f"{self.api_url}/{self.scim_api}"

        self.sp_url = f"{self.scim_url}/ServicePrincipals"
        self.groups_url = f"{self.scim_url}/Groups"
        self.users_url = f"{self.scim_url}/Users"

        self.patchop_schema = {
            "schemas": [
                "urn:ietf:params:scim:api:messages:2.0:PatchOp"
            ], }
        self.sp_schema = {
            "schemas": [
                "urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"
            ], }
        self.user_schema = {
            "schemas": [
                "urn:ietf:params:scim:schemas:core:2.0:User"
            ], }

    def get_sp(self, app_id=None):
        if app_id:
            url = f"{self.sp_url}?filter=applicationId+eq+{app_id}"
        else:
            url = self.sp_url

        r = self.request(url, request_type="get")
        return r

    @staticmethod
    def parse_group_vals(group_values):
        if isinstance(group_values[0], dict):
            group_values = [val["value"] for val in group_values]

        return [{"value": val} for val in group_values]

    def get_groups(self, groups):
        group_values = []
        for group in groups:
            r = self.request(f"{self.groups_url}?filter=displayName+eq+{group}",
                             request_type="get")
            group_values.append(r["Resources"][0]["id"])

        return group_values

    # @trycatch
    def add_sp(self, app_id, display_name, groups):
        body = {"applicationId": app_id,
                "displayName": display_name,
                "groups": self.parse_group_vals(
                    self.get_groups(groups)
                ),
                **self.sp_schema
                }
        r = self.request(self.sp_url, body, request_type="post")
        logger.info(f"ADDED spn {app_id} to groups {groups}")
        return r

    @staticmethod
    def filter_get_sp(r):
        sp = r["Resources"][0]
        sp_id = sp["id"]
        sp_groups = [group["value"] for group in sp["groups"]]
        return sp_id, sp_groups

    def remove_sp_group(self, sp_id=None, sp_groups=None, app_id=None, groups=None):
        if app_id:
            r = self.get_sp(app_id=app_id)
            logger.debug(r)
            sp_id, _ = self.filter_get_sp(r)

        body = {"Operations": [
            {
                "op": "remove",
                "path": f"members[value eq \"{sp_id}\"]",
            },
        ],
            **self.patchop_schema
        }

        if groups:
            sp_groups = self.get_groups(groups)

        for group in sp_groups:
            logger.debug(self.request(f"{self.groups_url}/{group}",
                                      body, request_type="patch"))

        logger.warning(f"Removed GROUPS {sp_groups} from SP {sp_id}")
        return

    # @trycatch
    def update_sp_group(self, app_id, groups, remove_current=False):
        r = self.get_sp(app_id=app_id)
        logger.debug(r)
        sp_id, sp_groups = self.filter_get_sp(r)
        logger.debug([sp_id, sp_groups])

        if remove_current:
            self.remove_sp_group(sp_id=sp_id, sp_groups=sp_groups)

        # if sp_groups:
        body = {"Operations": [
            {
                "op": "add",
                "path": "groups",
                "value": self.parse_group_vals(
                    self.get_groups(groups)
                ),
            },
        ],
            ** self.patchop_schema
        }

        try:
            r = self.request(f"{self.sp_url}/{sp_id}",
                             body,
                             request_type="patch")
            logger.info(f"UPDATED SP {app_id} to groups {sp_groups}")

            return r
        except Exception as err:
            logger.error(err)

    # @trycatch
    def delete_sp(self, app_id, groups):
        r = self.get_sp(app_id=app_id)
        logger.debug(r)
        sp_id, _ = self.filter_get_sp(r)

        r = self.request(f"{self.sp_url}/{sp_id}", request_type="delete")
        return r

    def add_user(self, user_name, display_name, groups):
        body = {"userName": user_name,
                "displayName": display_name,
                "groups": self.parse_group_vals(
                    self.get_groups(groups)
                ),
                **self.user_schema
                }
        r = self.request(self.users_url, body, request_type="post")
        logger.info(f"ADDED user {user_name} to groups {groups}")
        return r

    def get_user(self, user_name):
        r = self.request(f"{self.users_url}?filter=userName+eq+{user_name}",
                         request_type="get")
        user = r["Resources"][0]
        userid = user["id"]
        groups = user.get("groups")
        if groups:
            groups = self.parse_group_vals(groups)

        return userid, groups

    def get_multiple_users(self, user_filter):
        r = self.request(f"{self.users_url}?filter=userName+co+{user_filter}",
                         request_type="get")
        users = r["Resources"]
        # userid_list = [user["id"] for user in users]
        return users

    def update_user(self, user_name, display_name):
        userid, groups = self.get_user(user_name)
        id_url = f"{self.users_url}/{userid}"

        body = {"userName": user_name,
                "displayName": display_name,
                **self.user_schema
                }

        if groups:
            body["groups"] = groups

        r = self.request(id_url, body, request_type="put")
        logger.info(
            f"UPDATED user {user_name} with display_name {display_name}")
        return r

    def delete_user(self, user_name, userid=None):
        if not userid:
            userid, _ = self.get_user(user_name)

        r = self.request(f"{self.users_url}/{userid}",
                         request_type="delete")
        return r


class ClusterPermissions(PermissionsBase):
    """https://docs.databricks.com/dev-tools/api/latest/permissions.html#tag/Cluster-permissions
    There are four permission levels for a cluster:
    No Permissions
    Can Attach To (CAN_ATTACH_TO)
    Can Restart (CAN_RESTART)
    Can Manage (CAN_MANAGE)
    """

    def __init__(self, **kwargs):
        logger.info("""
++++++++++++++++++++++++++++++++++++++++
CLUSTER PERMISSONS
++++++++++++++++++++++++++++++++++++++++
 """)
        super().__init__(**kwargs)
        self.object_url = f"{self.permissions_url}/clusters"
        self.allowed_permissions = [
            "CAN_ATTACH_TO", "CAN_RESTART", "CAN_MANAGE"]


class DirectoryPermissions(PermissionsBase):
    """https://docs.databricks.com/dev-tools/api/latest/permissions.html#tag/Directory-permissions
    There are five permission levels for jobs:
    No Permissions
    Can Read (CAN_READ) — User can read items this directory
    Can Run (CAN_RUN) — Can run items in this directory.
    Can Edit (CAN_EDIT) — Can edit items in this directory.
    Can Manage (CAN_MANAGE) — Can manage this directory.
    """

    def __init__(self, **kwargs):
        logger.info("""
++++++++++++++++++++++++++++++++++++++++
DIRECTORY PERMISSONS
++++++++++++++++++++++++++++++++++++++++
        """)
        super().__init__(**kwargs)
        self.object_url = f"{self.permissions_url}/directories"
        self.allowed_permissions = ["CAN_READ",
                                    "CAN_RUN", "CAN_EDIT", "CAN_MANAGE"]
