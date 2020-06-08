import awswrangler as wr
import os
import re
import singer

from typing import List, Dict
from genson import SchemaBuilder

logger = singer.get_logger()

ATHENA_TO_JSON = {
    "boolean": "boolean",
    "tinyint": "number",
    "smallint": "number",
    "int": "number",
    "integer": "number",
    "bigint": "number",
    "double": "number",
    "float": "number",
    "decimal": "number",
    "char": "string",
    "varchar": "string",
    "string": "string",
    "binary": None,
    "date": "string",
    "timestamp": "string",
    "array": "array",
    "map": None,
    "struct": "object",
    "null": "null",
}


class S3:
    def __init__(self, config: dict) -> None:
        self.config = config

    def get_schema_for_table(self, table: Dict) -> Dict:
        input_files = self._get_input_files_for_table(table)
        schema = self._get_schema_from_files(input_files)
        return schema

    def _get_input_files_for_table(self, table: Dict) -> List:
        search_pattern = table["search_pattern"]
        table_name = table["table_name"]
        prefix = table.get("search_prefix", "")
        bucket = self.config["bucket"]
        s3_path = os.path.join(bucket, prefix)

        try:
            matcher = re.compile(search_pattern)
        except re.error as e:
            raise ValueError(
                f"search_pattern for table `{table_name}` is not a valid regex",
                search_pattern,
            ) from e

        logger.info(f"Checking bucket {bucket} for keys matching {search_pattern}")
        # TODO:  Add paginator logic? What if these wr dicts get huge?
        all_objs = wr.s3.list_objects(s3_path)
        objs_sizes = wr.s3.size_objects(all_objs)
        not_empty_objs = [x for x in all_objs if objs_sizes[x] != 0]
        matched_objs = [
            x for x in not_empty_objs if matcher.search(os.path.basename(x))
        ]
        return matched_objs

    @staticmethod
    def _get_schema_from_files(input_files: List) -> Dict:
        # N.B. These are Athena data types:
        #   https://docs.aws.amazon.com/athena/latest/ug/data-types.html
        columns, _ = wr.s3.read_parquet_metadata(path=input_files, dataset=False)
        builder = SchemaBuilder()

        for key, value in columns.items():
            builder.add_schema(
                {"type": "object", "properties": {key: {"type": ATHENA_TO_JSON[value]}}}
            )

        return builder.to_schema()
