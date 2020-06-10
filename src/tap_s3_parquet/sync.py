"""Handler for data syncing"""
import singer
from singer import metadata
from singer import utils
from singer import Transformer

from .s3 import S3

from typing import Dict

logger = singer.get_logger()


class Sync:
    def __init__(self, catalog: Dict, config: Dict, state: Dict) -> None:
        if state is None:
            state = {}
        self.catalog = catalog
        self.config = config
        self.state = state
        self.s3 = S3(config)

    @staticmethod
    def stream_is_selected(metadata_map) -> bool:
        # TODO log skipped
        return metadata_map.get((), {}).get("selected", False)

    def sync(self):
        logger.info("Syncing tap: tap-s3-parquet")
        # Select
        streams = self.catalog["streams"]
        metadata_maps = [metadata.to_map(x["metadata"]) for x in streams]
        tables = [
            next(
                y
                for y in self.config["tables"]
                if y["table_name"] == x["tap_stream_id"]
            )
            for x in streams
        ]
        key_properties = [
            metadata.get(x, (), "table-key-properties") for x in metadata_maps
        ]

        # Merge
        merged_sources = zip(streams, tables, key_properties, metadata_maps)

        # Filter
        selected_and_merged = [
            (s, t, k) for s, t, k, m in merged_sources if self.stream_is_selected(m)
        ]

        # Iterate and sync streams
        for stream, table, key_property in selected_and_merged:
            self._sync_stream(stream, table, key_property)

        logger.info("Finished Syncing tap: tap-s3-parquet")

    def _sync_stream(self, stream, table, key_property):
        stream_name = stream["tap_stream_id"]
        logger.info(f"Syncing stream: {stream_name}")

        singer.write_schema(stream_name, stream["schema"], key_property)
        self._sync_table(table, stream)

        logger.info(f"Finished syncing stream: {stream_name}")

    def _sync_table(self, table, stream) -> None:
        logger.info(f'Syncing table: {table["table_name"]}.')

        modified_since = utils.strptime_with_tz(
            singer.get_bookmark(self.state, table["table_name"], "modified_since")
            or self.config["start_date"]
        )
        # Select
        files = self.s3.get_input_files_for_table(table)
        file_descriptions = self.s3.get_descriptions(files)

        # Merge
        merged = zip(files, file_descriptions)

        # Filter
        filtered = [
            x
            for x in merged
            if x[1]["ContentLength"] > 0 and x[1]["LastModified"] > modified_since
        ]

        # Sort
        sorted_ = sorted(filtered, key=lambda x: x[1]["LastModified"])

        # Iterate and sync file
        for file, _ in sorted_:
            self._sync_file(stream, table, file)

        logger.info(f'Finished syncing table: {table["table_name"]}.')

    def _sync_file(self, stream, table, file):
        logger.info(f"Syncing file: ")
        dfs = self.s3.get_dfs_from_file(file)

        for df in dfs:
            dict_records = df.apply(lambda x: x.to_dict(), axis=1)

            with Transformer() as transformer:
                transformed_records = [
                    transformer.transform(
                        x, stream["schema"], metadata.to_map(stream["metadata"])
                    )
                    for x in dict_records
                ]
                for record in transformed_records:
                    singer.write_record(table["table_name"], record)

        logger.info(f"Finished syncing file: ")
