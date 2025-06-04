import hashlib
from pathlib import Path
from string import Template

from aws_cdk import (
    aws_athena as athena,
)
from constructs import Construct
from models.models import BaseQuery


class AthenaNamedQueryFromModel(Construct):
    def __init__(
        self,
        scope: Construct,
        resource_id: str,
        query: BaseQuery,
        workgroup_name: str,
    ) -> None:
        super().__init__(scope, resource_id)

        query_directory = Path(__file__).resolve().parent.parent / "queries"
        query_file_path = query_directory / query.creation_context.get(
            "query_file_path"
        )

        with query_file_path.open("r") as file:
            query_raw = file.read()

        query_str = Template(query_raw).substitute(**query.creation_context)

        query_hash = hashlib.md5(query_str.encode("utf-8")).hexdigest()

        description = f"Version: {query_hash}"

        # Create the Athena named query
        self.named_query = athena.CfnNamedQuery(
            self,
            "AthenaNamedQuery",
            database=query.database.database_name,
            query_string=query_str,
            name=query.name,
            description=description,
            work_group=workgroup_name,
        )
