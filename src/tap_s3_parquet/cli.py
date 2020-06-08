"""CLI"""
import click
import json
import logging
import sys
import singer
from typing import Dict, Union

from .discovery import Discover
from .utils import file_option_loader

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
    "--config", required=True, callback=file_option_loader, help="Path to config file"
)
@click.option("--state", callback=file_option_loader, help="Path to state file")
@click.option("--catalog", callback=file_option_loader, help="Path to catalog file")
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
        logger.info("Starting discovery")
        discoverer = Discover(config)
        json.dump(discoverer.get_catalog(), sys.stdout, indent=2)
        logger.info("Finished discovery")
    else:
        logger.info("Starting sync")
        logger.info("Finished sync")


if __name__ == "__main__":
    cli()
