from collections import defaultdict, Counter
from simple_etl.utils import (
    save_to_csv,
    get_country
)
from collections.abc import Sequence, Mapping
from typing import Any
from pathlib import Path


def transform_batch(
        carts_data: Sequence[Mapping[str, int]],
        products_data: Mapping[int, str],
        users_data: Sequence[Mapping[str, Any]],
        config: Mapping[str, Mapping[str, str | Path]]
) -> list[tuple[Any, ...]]:
    user_purchases = defaultdict(list)
    for cart in carts_data:
        user_id = cart["c_user_id"]
        product_id = cart["c_product_id"]

        cat = products_data.get(product_id)
        user_purchases[user_id].append(cat)

    top_bought_cat = {}
    for user_id, category in user_purchases.items():
        if category:
            top_cat = Counter(category).most_common(1)[0][0]
            top_bought_cat[user_id] = top_cat

    final_results = []
    for user in users_data:
        user_id = user["user_id"]
        lat = user["latitude"]
        lng = user["longitude"]
        most_bought_cat = top_bought_cat.get(user_id, None)
        country = get_country(lat, lng)
        final_results.append(
            (
                user_id, user["firstName"], user["lastName"],
                user["age"], user["gender"], user["email"],
                user["eyeColor"], lat, lng, country,
                most_bought_cat
            )
        )
    save_to_csv(final_results, config["paths"]["save_path"])

    return final_results

# def transform_batch(carts_data: list,
#                     products_data: dict,
#                     users_data: list
#                     ) -> list:
#     user_purchases = defaultdict(list)
#
#     for cart in carts_data:
#         user_purchases[cart["c_user_id"]].append(
#             products_data.get(cart["c_product_id"])
#         )
#     top_bought_cat = {
#         user_id: Counter(category).most_common(1)[0[0]]
#         for user_id, category in user_purchases.items() if category
#     }
#
#     final_result = [
#         (
#             user["user_id"], user["firstName"], user["lastName"],
#             user["age"], user["gender"], user["email"],
#             get_country(user["latitude"], user["longitude"]),
#             top_bought_cat.get(user["user_id"])
#         )
#         for user in users_data
#     ]
#     save_to_csv(final_result)
#     return final_result
