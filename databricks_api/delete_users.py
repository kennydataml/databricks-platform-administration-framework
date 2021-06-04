"""script to delete users in case they were added wrongly or with wrong domain
"""

from databricks_api.api import SCIM
from databricks_api.utils import parse_cmdline, logger, logging, LOGGER_NAME
from timeit import default_timer as timer
import datetime


def main(token, host, user_list=[], domain=""):
    """main function to delete users. either list of users or domain to delete
    """
    kwargs = {"token": token,
              "host": host}
    scim = SCIM(**kwargs)
    if user_list and len(user_list > 0):
        for user in user_list:
            r = scim.delete_user(user)
            logger.debug(r)
            logger.info(f"deleted user {user}")
    elif domain:
        if not isinstance(domain, str):
            raise ValueError("domain provided but not a string.")

        user_list = scim.get_multiple_users(domain)
        for user in user_list:
            username = user["userName"]
            userid = user["id"]
            r = scim.delete_user(None, userid=userid)
            logger.debug(r)
            logger.info(f"deleted user {username}")


if __name__ == "__main__":
    start = timer()

    args = parse_cmdline()
    if args.debug:
        logging.getLogger(LOGGER_NAME).setLevel(logging.DEBUG)

    user_list = []
    # WARNING: include the @ symbol for domain. 
    # otherwise you will delete bocqa users in dev by accident
    domain = "@domain.ca"

    main(token=args.personal_access_token,
         host=args.workspace_url,
         user_list=user_list,
         domain=domain)

    end = timer()
    runtime = str(datetime.timedelta(seconds=end-start))
    logger.info(f"EXECUTION TIME = {runtime}") # Time in seconds, e.g. 5.38091952400282
