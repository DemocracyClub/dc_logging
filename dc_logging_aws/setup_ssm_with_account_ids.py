"""
Run this script as an organisation admin to create the SSM entries in the 
monitoring accounts 
"""

import boto3
from mypy_boto3_organizations import OrganizationsClient
from mypy_boto3_ssm import SSMClient
from mypy_boto3_sts import STSClient

dev_and_stage_accounts = []
dev_monitoring_account = None
prod_accounts = []
prod_monitoring_account = None

org: OrganizationsClient = boto3.client("organizations")
groups = org.list_children(
    ParentId=org.list_roots()["Roots"][0]["Id"], ChildType="ORGANIZATIONAL_UNIT"
)
for group in groups["Children"]:
    group_info = org.describe_organizational_unit(
        OrganizationalUnitId=group["Id"]
    )
    group_name = group_info["OrganizationalUnit"]["Name"]
    if group_name == "Production accounts":
        account_list = prod_accounts
    else:
        account_list = dev_and_stage_accounts

    accounts_in_group = org.list_children(
        ParentId=group["Id"], ChildType="ACCOUNT"
    )
    for account in accounts_in_group["Children"]:
        account_id = account["Id"]
        account_list.append(account_id)
        account_name = org.describe_account(AccountId=account_id)["Account"][
            "Name"
        ]
        if account_name == "Dev - Monitoring - DC":
            dev_monitoring_account = account_id
        if account_name == "Production - Monitoring - DC":
            prod_monitoring_account = account_id


for account_id, account_list in (
    (prod_monitoring_account, prod_accounts),
    (dev_monitoring_account, dev_and_stage_accounts),
):
    role_arn = f"arn:aws:iam::{account_id}:role/OrganizationAccountAccessRole"

    sts: STSClient = boto3.client("sts")

    response = sts.assume_role(
        RoleArn=role_arn, RoleSessionName="monitoring_ssm_role"
    )
    creds = response["Credentials"]

    assumed_ssm: SSMClient = boto3.client(
        "ssm",
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name="eu-west-2",
    )
    assumed_ssm.put_parameter(
        Name="assume_role_aws_accounts",
        Type="StringList",
        Value=",".join(account_list),
        Overwrite=True,
    )
