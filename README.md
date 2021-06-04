# Databricks DevOps: Platform Administration Framework
Framework for end to end platform administration using YAML configuration files. This includes cluster management and ACL permissions.  
The Python code is a wrapper for SCIM and Permissions REST API.
## Tech Debt
- SCIM API is still in-preview. Should __switch to AAD integration__ once it is GA.
- Permissions API is still in-preview. Update the end point in `api.py` once it is GA.
## High-Level End to End Deployment
1. Create Workspace
2. Create Secret Scopes for workspace (manual) 
    - programatically needs AAD token which requires AAD application. 
    - also needs KV resource ID, KV DNS name.
3. create feature branch and __configure ACL.yaml per environment__
    - requires Service Principal IDs, Secret Scope Names
4. commit and PR to deployment branch (develop is default)
    - azure pipeline yaml will run cluster.py, then acl.py in that order
    - if pipeline doesn't exist, `Set up build` from develop branch using azure pipeline yaml
## Python sys.path
__NOTE : remember to add the databricks folder to python's sys.path__
- local development: `conda develop .`
- azure pipelines: there are 2 options
  ```yaml
  # option 1 (recommended)
  variables:
    - name: PYTHONPATH
      value: "%PYTHONPATH%;$(extra_path)"
  # option 2
  - script: python script.py
    env:
      PYTHONPATH : "%PYTHONPATH%;$(extra_path)"
  ```

```bash
.
│   README.md
│   requirements.txt            # required python libs
│   __init__.py
│
├───databricks_api
│   │   acl.py                  # ACL main script. uses ACL*.yaml
│   │   api.py                  # custom API classes for SCIM and Permissions API
│   │   base.py                 # base super classes
│   │   cluster.py              # cluster management main script. uses clusterconf*.yaml and clusterlib*.yaml
│   │   utils.py                # common utilities
│   │   __init__.py
│   │
│   └───configuration           # can duplicate as necessary
│           ACL.yaml            # ACL configuration
│           clusterconf.yaml    # cluster configuration
│           clusterlib.yaml     # cluster library configuration for all clusters in workspace
│
└───test                        # pytest
        ACL_expected.yaml
        ACL_template.yaml
        test_utils.py
        __init__.py
```
## cluster.py usage
```
python databricks_api\cluster.py -h
usage: cluster.py [-h] -pat PERSONAL_ACCESS_TOKEN -wu WORKSPACE_URL
                  [-ccf CLUSTER_CONFIG_FILE] [-clf CLUSTER_LIBRARY_FILE]

Databricks Workspace ACL Configuration

optional arguments:
  -h, --help            show this help message and exit
  -pat PERSONAL_ACCESS_TOKEN, --personal_access_token PERSONAL_ACCESS_TOKEN
                        Personal Access Token from Admin Console
  -wu WORKSPACE_URL, --workspace_url WORKSPACE_URL
                        Workspace URL
  -ccf CLUSTER_CONFIG_FILE, --cluster_config_file CLUSTER_CONFIG_FILE
                        Default is clusterconf.yaml
  -clf CLUSTER_LIBRARY_FILE, --cluster_library_file CLUSTER_LIBRARY_FILE
                        Default is clusterlib.yaml
```
### clusterconf.yaml
for ML clusters, specify similar to `spark_version: 8.1.x-cpu-ml-scala2.12`.  
for GPU ML: `spark_version: 8.1.x-gpu-ml-scala2.12`. _Note that GPU spark version does not support credential passthrough._
## acl.py usage
```
python databricks_api\acl.py -h
usage: acl.py [-h] -pat PERSONAL_ACCESS_TOKEN -wu WORKSPACE_URL [-af ACL_FILE]

Databricks Workspace ACL Configuration

optional arguments:
  -h, --help            show this help message and exit
  -pat PERSONAL_ACCESS_TOKEN, --personal_access_token PERSONAL_ACCESS_TOKEN
                        Personal Access Token from Admin Console
  -wu WORKSPACE_URL, --workspace_url WORKSPACE_URL
                        Workspace URL
  -af ACL_FILE, --acl_file ACL_FILE
                        Default is ACL.yaml
```
---
# Resources
- Create high-concurrency with REST API: https://docs.databricks.com/dev-tools/api/latest/examples.html#create-a-high-concurrency-cluster
- [Delete commit history](https://stackoverflow.com/questions/13716658/how-to-delete-all-commit-history-in-github)
