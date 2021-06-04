from databricks_api.api import SCIM, ClusterPermissions, DirectoryPermissions

from databricks_cli.sdk import ApiClient
from databricks_cli.secrets.api import SecretApi
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.groups.api import GroupsApi

from databricks_api.utils import render_yaml, parse_cmdline, logger, dir_path, logging, LOGGER_NAME
# , dump_yaml
from timeit import default_timer as timer
import datetime


def deploy_groups(groups_client, scim, groups_config, remove_unmanaged=False):
    """function to deploy groups and corresponding users/spn

    :param groups_client: databricks Groups API
    :type groups_client: databricks_client.groups.api.GroupsApi
    :param scim: databricks SCIM API
    :type scim: api.SCIM
    :param groups_config: GROUPS in ACL.yaml
    :type groups_config: list(dict)
    """
    if remove_unmanaged:
        logger.warning("remove unmanaged groups and users is ENABLED")
    # delete groups that are not authorized
    existing_groups = [g for g in groups_client.list_all()["group_names"]
                       if g != "users"]
    group_list = [g["name"] for g in groups_config]

    logger.debug(existing_groups)
    logger.debug(group_list)

    remove_groups = [g for g in existing_groups
                     if g not in group_list and g != "admins"]
    logger.warning(f"unmanaged groups: {remove_groups}")
    if remove_unmanaged:
        for g in remove_groups:
            groups_client.delete(g)
            logger.debug(f"removed group: {g}")

    # group creation and user/spn create,add,remove
    for grp in groups_config:
        principal = grp["name"]
        logger.info("========================================")
        logger.info(f"Group: {principal}")

        try:
            groups_client.create(principal)
            logger.info(f"created group {principal}")
        except Exception as err:
            logger.error(err)

        # remove unauthorized members
        temp_members = groups_client.list_members(principal)
        if temp_members.get("members"):
            current_members = temp_members.get("members")
        else:
            current_members = []

        member_list = grp["members"]

        # add_members = []
        modified_member_list = []
        for member in member_list:
            user_name = member.get("user_name") if member.get(
                "user_name") else member.get("application_id")
            modified_member_list.append({"user_name": user_name})
            # if {"user_name": user_name} not in current_members:
            #     add_members.append(member)

        remove_members = []
        for cur_mem in current_members:
            member_key = "user_name" if grp["type"] == "user" else "application_id"
            if cur_mem not in modified_member_list:
                remove_members.append({member_key: cur_mem["user_name"]})

        logger.warning(f"unmanaged members: {remove_members}")
        # logger.info(f"adding members: {add_members}")
        logger.info(f"adding members to group")

        # create and add users/spn to their groups
        groups = [principal]

        if grp["type"] == "user":
            if remove_unmanaged:
                for user in remove_members:
                    user_name = user["user_name"]
                    groups_client.remove_member(
                        principal, user_name, None)
                    logger.warning(f"removed {user_name} from {principal}")

            for user in member_list:
                user_name = user["user_name"]
                display_name = user.get("display_name")
                try:
                    r = scim.add_user(user_name, display_name, groups)
                except Exception as err:
                    logger.debug(repr(err))
                    # if already existing user, update the groups
                    if display_name:
                        scim.update_user(user_name, display_name)

                    r = groups_client.add_member(
                        principal, user_name, None)

                logger.debug(r)
                # logger.info(f"successfully added {user_name}")
        elif grp["type"] == "spn":
            for spn in remove_members:
                scim.remove_sp_group(
                    app_id=spn["application_id"], groups=groups)

            for spn in member_list:
                app_id = spn["application_id"]
                display_name = spn.get("display_name")
                # scim.delete_sp(app_id)
                try:
                    r = scim.add_sp(app_id, display_name, groups)
                except Exception as err:
                    logger.debug(repr(err))
                    # if already existing SP, can update the groups
                    # remove_current=True,
                    r = scim.update_sp_group(app_id, groups,)

                logger.debug(r)


def deploy_secret_acl(secret_client, secret_config):
    """function to deploy secret scope permissions

    :param secret_client: databricks Secrets API
    :type secret_client: databricks_cli.secrets.api.SecretApi
    :param secret_config: SECRETS in ACL.yaml
    :type secret_config: list(dict)
    """
    logger.info("""
++++++++++++++++++++++++++++++++++++++++
SECRETS ACL
++++++++++++++++++++++++++++++++++++++++
        """)

    # remove ACL on *unmanaged* secret scopes
    current_scopes = [s["name"] for s in secret_client.list_scopes()["scopes"]]
    if not secret_config:
        logger.warning(f"removing ACL from UNMANAGED scopes: {current_scopes}")
        for s in current_scopes:
            current_acl = secret_client.list_acls(s)
            if current_acl.get("items"):
                for i in current_acl.get("items"):
                    secret_client.delete_acl(s, i["principal"])

        return

    scope_list = [s["scope"] for s in secret_config]
    remove_scopes = [s for s in current_scopes if s not in scope_list]
    logger.warning(f"removing ACL from UNMANAGED scopes: {remove_scopes}")
    for s in remove_scopes:
        current_acl = secret_client.list_acls(s)
        if current_acl.get("items"):
            for i in current_acl.get("items"):
                secret_client.delete_acl(s, i["principal"])
        # secret_client.delete_scope(s)

    # remove then add ACL on scope
    for secret in secret_config:
        # TODO try create scope?
        # complicated. needs AAD token which requires AAD application. also needs KV resource ID, KV DNS name:
        # https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#create-an-azure-key-vault-backed-secret-scope-using-the-databricks-cli
        # https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/app-aad-token#--use-an-azure-ad-access-token-to-access-the-databricks-rest-api
        # backend_azure_keyvault = {
        #     'resource_id': resource_id,
        #     'dns_name': dns_name
        # }
        # secret_client.create_scope(secret["scope"], "Creator", "AZURE_KEYVAULT", backend_azure_keyvault)

        scope = secret["scope"]
        acl_list = secret["acl"]
        logger.info("========================================")
        logger.info(scope)
        logger.debug(acl_list)

        # check and remove unauthorized ACL on scope
        current_acl = secret_client.list_acls(scope)
        parsed_acl = []
        if current_acl.get("items"):
            temp_read = {"permission": "READ"}
            temp_manage = {"permission": "MANAGE"}
            temp_write = {"permission": "WRITE"}
            for i in current_acl["items"]:
                if i["permission"] == "READ":
                    temp_read["group"] = temp_read.get(
                        "group", []) + [i["principal"]]
                elif i["permission"] == "MANAGE":
                    temp_manage["group"] = temp_manage.get(
                        "group", []) + [i["principal"]]
                elif i["permission"] == "WRITE":
                    temp_write["group"] = temp_write.get(
                        "group", []) + [i["principal"]]

            if temp_read.get("group"):
                parsed_acl.append(temp_read)
            if temp_manage.get("group"):
                parsed_acl.append(temp_manage)
            if temp_write.get("group"):
                parsed_acl.append(temp_write)
            logger.debug(f"existing parsed ACL: {parsed_acl}")
        else:
            logger.info(f"No ACL on scope {scope}")

        remove_acl = [d for d in parsed_acl if d not in acl_list]
        logger.warning(f"remove ACL: {remove_acl}")
        for d in remove_acl:
            for g in d["group"]:
                secret_client.delete_acl(scope, g)

        logger.info(f"applying ACL on scope {scope}")
        for acl in acl_list:
            for group in acl["group"]:
                secret_client.put_acl(scope, group, acl["permission"])
                logger.info(f'{acl["permission"]}: {group}')


def deploy_cluster_acl(cluster_client, cluster_perm, cluster_config):
    """function to deploy cluster permissions

    :param cluster_client: databricks Cluster API
    :type cluster_client: databricks_cli.clusters.api.ClusterApi
    :param cluster_perm:
        databricks Permissions API (cluster)
        https://docs.gcp.databricks.com/dev-tools/api/latest/permissions.html#tag/Cluster-permissions
    :type cluster_perm: api.ClusterPermissions
    :param cluster_config: CLUSTERS in ACL.yaml
    :type cluster_config: list(dict)
    """
    for cluster in cluster_config:
        cluster_name = cluster["name"]
        acl_list = cluster["acl"]
        logger.info("========================================")
        logger.info(cluster_name)
        logger.debug(acl_list)

        # get cluster id
        cluster_id = cluster_client.get_cluster_id_for_name(
            cluster["name"])

        # replace ACL on cluster
        logger.info(
            cluster_perm.replace_permissions(cluster_id, acl_list)
        )


def deploy_workspace_acl(workspace_client, dir_perm, workspace_config):
    """function to deploy permissions on workspace folders

    :param workspace_client: databricks Workspace API
    :type workspace_client: databricks_cli.workspace.api.WorkspaceApi
    :param dir_perm:
        databricks Permissions API (directory)
        https://docs.gcp.databricks.com/dev-tools/api/latest/permissions.html#tag/Directory-permissions
    :type dir_perm: api.DirectoryPermissions
    :param workspace_config: WORKSPACE in ACL.yaml
    :type workspace_config: list(dict)
    """
    # delete unmanaged folders
    folder_list = [f["folder"] for f in workspace_config]
    logger.debug(folder_list)
    ignore_folders = ["Shared", "Users"]

    current_items = ["/" + i.basename for i in workspace_client.list_objects("/")
                     if i.basename not in ignore_folders]

    remove_items = [i for i in current_items if i not in folder_list]
    logger.warning(f"removing UNMANAGED folders/files: {remove_items}")
    for ri in remove_items:
        workspace_client.delete(ri, True)

    # apply ACL to folders. create if not exist
    for wsdir in workspace_config:
        folder = wsdir["folder"]
        acl_list = wsdir["acl"]
        logger.info("========================================")
        logger.info(folder)
        logger.debug(acl_list)
        try:
            directory = workspace_client.get_status(folder)
        except Exception as error:
            logger.error(repr(error))
            logger.info(f"creating folder {folder}")
            logger.debug(workspace_client.mkdirs(folder))
            directory = workspace_client.get_status(folder)

        # https://github.com/databricks/databricks-cli/blob/master/databricks_cli/workspace/api.py#L39
        if not directory.is_dir:
            logger.error(f"path {folder} is not a directory")
        else:
            logger.info(
                dir_perm.replace_permissions(directory.object_id, acl_list)
            )


def main(config, token=None, host=None, cmdline_args=None):
    """main function for end to end Databricks workspace ACL configuraiton

    :param config: ACL configuration
    :type config: dict
    :param token: Databricks Personal Access Token
    :type token: str
    :param host: Databricks workspace  URL
    :type host: str
    :param cmdline_args: command line arguments
    :type cmdline_args: argparse
    """
    remove_unmanaged = False
    if cmdline_args:
        if cmdline_args.remove:
            remove_unmanaged = True

    logger.debug(config)
    kwargs = {"token": token,
              "host": host}

    # https://github.com/databricks/databricks-cli/blob/master/databricks_cli/sdk/api_client.py#L65
    api_client = ApiClient(**kwargs)
    # https://github.com/databricks/databricks-cli/blob/master/databricks_cli/groups/api.py#L27
    groups_client = GroupsApi(api_client)
    scim = SCIM(**kwargs)
    deploy_groups(groups_client, scim, config["GROUPS"], remove_unmanaged=remove_unmanaged)

    # https://github.com/databricks/databricks-cli/blob/master/databricks_cli/secrets/api.py#L27
    secret_client = SecretApi(api_client)
    if config.get("SECRETS"):
        deploy_secret_acl(secret_client, config["SECRETS"])
    else:
        deploy_secret_acl(secret_client, None)

    # https://github.com/databricks/databricks-cli/blob/master/databricks_cli/clusters/api.py
    cluster_client = ClusterApi(api_client)
    cluster_perm = ClusterPermissions(**kwargs)
    if config.get("CLUSTERS"):
        deploy_cluster_acl(cluster_client, cluster_perm, config["CLUSTERS"])

    # https://github.com/databricks/databricks-cli/blob/master/databricks_cli/workspace/api.py#L86
    # prepend all paths with /
    workspace_client = WorkspaceApi(api_client)
    dir_perm = DirectoryPermissions(**kwargs)
    deploy_workspace_acl(workspace_client, dir_perm, config["WORKSPACE"])


if __name__ == "__main__":
    start = timer()

    args = parse_cmdline(cmd_type="ACL")
    if args.debug:
        logging.getLogger(LOGGER_NAME).setLevel(logging.DEBUG)

    # mako_kwargs = {
        # "domain": args.domain,
    # }

    acl_config = render_yaml(
        f"{dir_path}\\configuration\\{args.acl_file}", 
        # mako_kwargs
        )

    main(acl_config,
         token=args.personal_access_token,
         host=args.workspace_url,
         cmdline_args=args)

    end = timer()
    runtime = str(datetime.timedelta(seconds=end-start))
    logger.info(f"EXECUTION TIME = {runtime}") # Time in seconds, e.g. 5.38091952400282
