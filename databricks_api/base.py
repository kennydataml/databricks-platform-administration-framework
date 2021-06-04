import requests
from databricks_api.utils import logger


class APIBase:
    def __init__(self, token, host):
        self.token = token
        self.headers = {'Authorization': f'Bearer {self.token}'}
        self.host = host
        self.api_url = f"{self.host}/api/2.0"

    def request(self, url, body=None, request_type="get"):
        kwargs = {
            "url": url,
            "headers": self.headers,
        }
        if body:
            kwargs["json"] = body

        request = getattr(requests, request_type)
        r = request(**kwargs)
        try:
            final_response = r.json()
        except Exception:
            final_response = r.text

        if r.status_code not in [200, 201, 204]:
            logger.debug(f"status code: {r.status_code}")
            logger.debug(final_response)
            # must throw error for acl.py try/except blocks
            raise ValueError(final_response)

        r.close()
        logger.debug(final_response)
        return final_response


class PermissionsBase(APIBase):
    """Permissions Super class
    Following attributes will be set in the child class:
    object_url
    allowed_permissions
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.permissions_api = "preview/permissions"
        self.permissions_url = f"{self.api_url}/{self.permissions_api}"

    def _check_permission(self, permission_level):
        permission_level = permission_level.upper()
        if permission_level not in self.allowed_permissions:
            raise ValueError(
                f"permission {permission_level} not in allowed permissions: {self.allowed_permissions}"
            )

        return permission_level

    def _parse_acl(self, access_control_list):
        """other objects include
        "user_name" for user objects
        "service_principal_name" for SPN objects
        """
        acl_list = []
        for acl in access_control_list:
            permission_level = self._check_permission(acl["permission"])
            if acl.get("group"):
                for group in acl["group"]:
                    acl_list.append(
                        {
                            "group_name": group,
                            "permission_level": permission_level
                        }
                    )
            elif acl.get("user"):
                for group in acl["user"]:
                    acl_list.append(
                        {
                            "user_name": group,
                            "permission_level": permission_level
                        }
                    )
            else:
                logger.error("acl does not contain group or user")

        return acl_list

    def get_permission_levels(self, object_id):
        return self.request(f"{self.object_url}/{object_id}/permissionLevels",
                            request_type="get")

    def get_permissions(self, object_id):
        return self.request(f"{self.object_url}/{object_id}",
                            request_type="get")

    def update_permissions(self, object_id, access_control_list):
        """updates existing permissions. not supporting user or SPN objects.

        any of the acl parameters, we are expecting:
        [
            {
                "permission": "abc",
                "group": ["a", "b", "c"]
            }
        ]
        """
        acl = self._parse_acl(access_control_list)
        return self.request(f"{self.object_url}/{object_id}",
                            body={"access_control_list": acl},
                            request_type="patch")

    def replace_permissions(self, object_id, access_control_list):
        """overwrites existing permissions. not supporting user or SPN objects.

        any of the acl parameters, we are expecting:
        [
            {
                "permission": "abc",
                "group": ["a", "b", "c"]
            }
        ]
        """
        acl = self._parse_acl(access_control_list)
        return self.request(f"{self.object_url}/{object_id}",
                            body={"access_control_list": acl},
                            request_type="put")
