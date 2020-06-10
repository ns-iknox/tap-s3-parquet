"""CLI"""
import click
import json
import logging
import sys
import singer
from typing import Dict, Union

from .discovery import Discover
from .sync import Sync
from .utils import validate_and_load_file

logger = singer.get_logger()


@click.command()
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
    show_default=True,
)
@click.option("--discover", "discovery", is_flag=True, help="Run tap in discovery mode")
@click.option(
    "--config",
    required=True,
    callback=validate_and_load_file,
    help="Path to config file",
)
@click.option("--state", callback=validate_and_load_file, help="Path to state file")
@click.option("--catalog", callback=validate_and_load_file, help="Path to catalog file")
def cli(
    log_level: str,
    config: Dict,
    discovery: bool = False,
    state: Union[Dict, None] = None,
    catalog: Union[Dict, None] = None,
) -> None:
    """Singer tap to retrieve data from parquet files stored in S3"""

    logging.basicConfig(level=getattr(logging, log_level.upper()))

    if discovery:
        discoverer = Discover(config)
        json.dump(discoverer.get_catalog(), sys.stdout, indent=2)
    elif catalog:
        syncer = Sync(catalog, config, state)
        syncer.sync()


if __name__ == "__main__":
    cli()
