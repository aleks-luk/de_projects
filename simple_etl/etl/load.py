import sqlite3
from pathlib import Path
from simple_etl.etl.extract import fetch_users_data
from simple_etl.utils import (
    execute_insert_script
)
from simple_etl.etl.transform import transform_batch
from loguru import logger
from collections.abc import Sequence, Mapping


def load_to_sqlite(
        carts_data: Sequence[Mapping[str, int]],
        products_data: Mapping[int, str],
        config: Mapping[str, Mapping[str, str | Path]],
        conn: sqlite3.Connection,
        batch_size: int = 30
) -> None:
    skip = 0
    while True:
        user_data = fetch_users_data(
            config["api_urls"]["users"],
            skip,
            batch_size
        )
        if not user_data:
            break
        data_to_insert = transform_batch(
            carts_data,
            products_data,
            user_data,
            config
        )
        try:
            execute_insert_script(
                conn,
                data_to_insert,
                config["paths"]["insert_row"]
            )
            skip += batch_size
        except KeyError as e:
            logger.error("Key Error", e)
            raise
