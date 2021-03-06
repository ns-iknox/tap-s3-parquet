"""Various utilities"""
from pathlib import Path
import click
import json
from typing import Tuple


def validate_and_load_file(_, __, file_path: str) -> Tuple:
    if file_path is not None:
        file = Path(file_path)
        if file.exists() is False:
            raise click.BadParameter(f'File "{file_path}" not found.')
        else:
            try:
                return json.loads(file.read_text())
            except json.decoder.JSONDecodeError:
                raise click.BadParameter(f'File "{file_path}" is not valid JSON.')
