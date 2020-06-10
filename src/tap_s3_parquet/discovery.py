"""Handler for discovery/catalog generation"""
from singer import metadata
from .s3 import S3

from typing import Dict, List


class Discover:
    """Class with methods to generate the catalog"""

    def __init__(self, config: Dict) -> None:
        """

        :param config:
        """
        self.config = config
        self.s3 = S3(config)

    def get_catalog(self) -> Dict:
        streams = self._discover_streams()
        catalog = {"streams": streams}
        return catalog

    def _discover_streams(self) -> List:
        streams = [
            {
                "stream": table["table_name"],
                "tap_stream_id": table["table_name"],
                "schema": self.s3.get_schema_for_table(table),
            }
            for table in self.config["tables"]
        ]

        # TODO use zip ya dummy
        for index, stream in enumerate(streams):
            stream.update(
                {
                    "metadata": self._load_metadata(
                        self.config["tables"][index], stream["schema"]
                    )
                }
            )
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
