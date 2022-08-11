"""Handler for discovery/catalog generation"""
import singer
from singer import metadata
from .s3 import S3

from typing import Dict, List

logger = singer.get_logger()


class Discover:
    """Class with methods to generate the catalog"""

    def __init__(self, config: Dict) -> None:
        self.config = config
        self.s3 = S3(config)

    def get_catalog(self) -> Dict:
        logger.info("Starting tap discovery")
        streams = self._discover_streams()
        catalog = {"streams": streams}
        logger.info("Finished tap discovery")
        return catalog

    def _discover_streams(self) -> List:
        for table in self.config["tables"]:
            schema = self.s3.get_schema_for_table(table)
            streams = [
                {
                    "schema": schema,
                    "stream": table["table_name"],
                    "tap_stream_id": table["table_name"],
                    "metadata": self._load_metadata(
                        table, schema
                    ),
                }
        ]

        return streams

    @staticmethod
    def _load_metadata(table: Dict, schema: Dict) -> List:
        mdata = metadata.new()

        mdata = metadata.write(
            mdata, (), "table-key-properties", table["key_properties"]
        )

        for field_name in schema.get("properties", {}).keys():
            if table.get("key_properties", []) and field_name in table.get(
                "key_properties", []
            ):
                mdata = metadata.write(
                    mdata, ("properties", field_name), "inclusion", "automatic"
                )
            else:
                mdata = metadata.write(
                    mdata, ("properties", field_name), "inclusion", "available"
                )

        return metadata.to_list(mdata)
