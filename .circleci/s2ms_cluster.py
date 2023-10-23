import os
import singlestoredb as s2
import uuid
import sys
from typing import Dict, Optional

SQL_USER_PASSWORD = os.getenv("SQL_USER_PASSWORD")  # project UI env-var reference
S2MS_API_KEY = os.getenv("S2MS_API_KEY")  # project UI env-var reference

WORKSPACE_GROUP_BASE_NAME = "dbt-connector-ci-test-cluster"
WORKSPACE_NAME = "tests"
AWS_US_EAST_REGION = "3482219c-a389-4079-b18b-d50662524e8a"
AUTO_TERMINATE_MINUTES = 20
WORKSPACE_ENDPOINT_FILE = "WORKSPACE_ENDPOINT_FILE"
WORKSPACE_GROUP_ID_FILE = "WORKSPACE_GROUP_ID_FILE"

TOTAL_RETRIES = 5


def retry(func):
     for i in range(TOTAL_RETRIES):
        try:
            return func()
        except Exception as e:
            if i == TOTAL_RETRIES - 1:
                raise SystemExit(e)
            print(f"Attempt {i+1} failed with error: {e}. Retrying...")


def create_workspace(workspace_manager):
    w_group_name = WORKSPACE_GROUP_BASE_NAME + "-" + uuid.uuid4().hex
    def create_workspace_group():
        return workspace_manager.create_workspace_group(
            name=w_group_name,
            region=AWS_US_EAST_REGION,
            firewall_ranges=["0.0.0.0/0"],
            admin_password=SQL_USER_PASSWORD,
            expires_at="20m"
        )
    workspace_group = retry(create_workspace_group)

    with open(WORKSPACE_GROUP_ID_FILE, "w") as f:
        f.write(workspace_group.id)
    print("Created workspace group {}".format(w_group_name))

    print("Starting creation of a workspace")
    def create_workspace_within_group():
        return workspace_group.create_workspace(name=WORKSPACE_NAME, size="S-00", wait_on_active=True, wait_timeout=550)
    workspace = retry(create_workspace_within_group)

    with open(WORKSPACE_ENDPOINT_FILE, "w") as f:
        f.write(workspace.endpoint)
    print("Created workspace {}".format(WORKSPACE_NAME))
    return workspace


def terminate_workspace(workspace_manager) -> None:
    with open(WORKSPACE_GROUP_ID_FILE, "r") as f:
        workspace_group_id = f.read()
    workspace_group = workspace_manager.get_workspace_group(workspace_group_id)

    for workspace in workspace_group.workspaces:
        workspace.terminate(wait_on_terminated=True)
    workspace_group.terminate()


def check_and_update_connection(create_db: Optional[str] = None):
    with open(WORKSPACE_GROUP_ID_FILE, "r") as f:
        workspace_group_id = f.read()
    workspace_group = workspace_manager.get_workspace_group(workspace_group_id)
    workspace = workspace_group.workspaces[0]

    conn = workspace.connect(
        user="admin",
        password=SQL_USER_PASSWORD,
        port=3306)

    cur = conn.cursor()
    try:
        cur.execute("SELECT NOW():>TEXT")
        res = cur.fetchall()
        print(f"Successfully connected to {workspace.id} at {res[0][0]}")

        if create_db is not None:
            cur.execute(f"DROP DATABASE IF EXISTS {create_db}")
            cur.execute(f"CREATE DATABASE {create_db}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Not enough arguments to start/terminate cluster!")
        exit(1)
    command = sys.argv[1]
    db_name = None
    if len(sys.argv) > 2:
        db_name = sys.argv[2]

    workspace_manager = s2.manage_workspaces(access_token=S2MS_API_KEY)

    if command == "start":
        create_workspace(workspace_manager)
        check_and_update_connection(db_name)
        exit(0)

    if command == "terminate":
        terminate_workspace(workspace_manager)
        exit(0)

    if command == "update":
        check_and_update_connection(db_name)
        exit(0)
