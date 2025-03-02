import random
import sqlite3
import csv
import json
import time

from loguru import logger
from pathlib import Path
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from typing import Any
from collections.abc import Sequence

logger.add(
    "logs.txt",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO"
)


def save_to_csv(
        data: Sequence[Sequence[Any, ...]],
        file_path: Path
) -> None:
    try:
        with file_path.open(mode="a", newline="", encoding="utf-8") as file:
            headers = [
                "user_id", "firstName", "lastName",
                "age", "gender", "email",
                "eyeColor", "latitude", "longitude",
                "country", "most_bought_cat"
            ]
            writer = csv.writer(file, delimiter=";")
            writer.writerow(headers)
            for entry in data:
                writer.writerow(entry)
    except FileNotFoundError:
        logger.error(f"Error: The file {file_path} was not found.")
        raise
    except PermissionError:
        logger.error(f"Error: Permission denied for file {file_path}.")
        raise


def get_country(
        latitude: float,
        longitude: float,
        max_retries: int = 3
) -> str | None:
    geolocator = Nominatim(user_agent="geopy_v2_simple_etl_task", timeout=5)
    retries = 0
    while retries < max_retries:
        try:
            location = geolocator.reverse(
                (latitude, longitude),
                exactly_one=True
            )
            if location and "address" in location.raw:
                return location.raw["address"].get("country")
            else:
                return None
        except GeocoderTimedOut:
            wait_time = min(
                0.7 ** retries + random.uniform(0.1, 0.5),
                1.1
            )
            time.sleep(wait_time)
            retries += 1
        except GeocoderServiceError as e:
            logger.error(f"GeoCoder service error {e}.")
            time.sleep(0.1)
            retries += 1
        except Exception as e:
            logger.error(f"Error {e}.")
            retries += 1


def create_connection(db_file: Path) -> sqlite3.Connection:
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        logger.error(
            f"SQL error: {e.sqlite_errorcode}"
            f"{e.sqlite_errorname}"
        )
        raise
    logger.info("Connected to the database")
    return conn


def execute_sql_script(
        conn: sqlite3.Connection,
        ddl_script_file_path: Path
) -> None:
    try:
        with ddl_script_file_path.open("r") as file:
            sql_script = file.read().strip()
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise
    except PermissionError:
        logger.error(
            f"Error: Permission denied for file {ddl_script_file_path}."
        )
        raise
    try:
        cursor = conn.cursor()
        cursor.execute(sql_script)
    except (
            sqlite3.OperationalError,
            sqlite3.DatabaseError,
            sqlite3.Error
    ) as e:
        logger.error(f"Database error {e}")
        raise
    logger.info("SQL script executed successfully")


def execute_insert_script(
        conn: sqlite3.Connection,
        data_to_insert: Sequence[tuple[Any, ...]],
        #list[tuple[Any, ...]],
        dml_script_file_path: Path
) -> None:
    try:
        with dml_script_file_path.open("r") as file:
            sql_script = file.read().strip()
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise
    try:
        cursor = conn.cursor()
        cursor.executemany(sql_script, data_to_insert)
    except (
            sqlite3.IntegrityError,
            sqlite3.DataError,
            sqlite3.DatabaseError,
            sqlite3.Error
    ) as e:
        conn.rollback()
        logger.error(f"Unexpected error during transaction {e}")
        raise
    conn.commit()
    logger.info("SQL script executed successfully")


def read_config(
        file_path: Path = Path("config.json")
) -> dict[str, dict[str, str | Path]]:
    try:
        with file_path.open("r") as f:
            d = json.load(f)
    except FileNotFoundError as e:
        logger.error(f"Config not found: {e}")
        raise
    except PermissionError:
        logger.info(f"Error: Permission denied for file {file_path}.")
        raise
    d["paths"]["create_table"] = Path(d["paths"]["create_table"])
    d["paths"]["insert_row"] = Path(d["paths"]["insert_row"])
    d["paths"]["save_path"] = Path(d["paths"]["save_path"])
    d["paths"]["db"] = Path(d["paths"]["db"])
    return d
