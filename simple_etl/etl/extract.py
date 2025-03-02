import requests

from typing import Any
from loguru import logger
from json import JSONDecodeError


def fetch_carts_data(
        url: str,
        skip: int = 0,
        limit: int = 30
) -> list[dict[str, int]]:
    try:
        r = requests.get(
            url,
            params={
                "skip": skip,
                "limit": limit
            }
        )
        r.raise_for_status()
    except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.RequestException
    ) as e:
        logger.error(f"GET response encountered an error, {e}")
        raise
    try:
        carts_data = r.json()
    except JSONDecodeError as e:
        logger.error(f"JSON decode error {e}")
        raise
    cleaned_carts = []
    for cart in carts_data["carts"]:
        cleaned_carts.append(
            {
                "c_cart_id": cart["id"],
                "c_product_id": cart["products"][0]["id"],
                "c_user_id": cart["userId"],
            }
        )
    return cleaned_carts


def fetch_products_data(
        url: str,
        skip: int = 0,
        limit: int = 0
) -> dict[int, str]:
    try:
        r = requests.get(
            url,
            params={
                "select": "id,"
                          "category",
                "skip": skip,
                "limit": limit
            }
        )
        r.raise_for_status()
    except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.RequestException
    ) as e:
        logger.error(f"GET response encountered an error, {e}")
        raise
    try:
        products_data = r.json()
    except JSONDecodeError as e:
        logger.error(f"JSON decode error {e}")
        raise
    cleaned_products = {}
    for product in products_data["products"]:
        cleaned_products[product["id"]] = product["category"]

    return cleaned_products


def fetch_users_data(
        url: str,
        skip: int,
        batch_size: int
) -> list[dict[str, Any]]:
    try:
        r = requests.get(
            url,
            params={
                "select": "firstName,"
                          "lastName,"
                          "age,"
                          "gender,"
                          "email,"
                          "eyeColor,"
                          "address,"
                          "id",
                "skip": skip,
                "limit": batch_size
            }
        )
        r.raise_for_status()
    except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.RequestException
    ) as e:
        logger.error(f"GET response encountered an error, {e}")
        raise
    try:
        users_data = r.json()
    except JSONDecodeError as e:
        logger.error(f"JSON decode error {e}")
        raise
    cleaned_users = []
    for user in users_data["users"]:
        cleaned_users.append(
            {
                "user_id": user["id"],
                "firstName": user["firstName"],
                "lastName": user["lastName"],
                "age": user["age"],
                "gender": user["gender"],
                "email": user["email"],
                "eyeColor": user["eyeColor"],
                "latitude": user["address"]["coordinates"]["lat"],
                "longitude": user["address"]["coordinates"]["lng"]
            }
        )
    return cleaned_users
