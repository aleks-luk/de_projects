from etl.extract import (
    fetch_products_data,
    fetch_carts_data
)
from etl.load import load_to_sqlite
from simple_etl.utils import (
    read_config,
    create_connection,
    execute_sql_script
)


def main():
    config = read_config()
    carts_data = fetch_carts_data(
        config["api_urls"]["carts"]
    )
    products_data = fetch_products_data(
        config["api_urls"]["products"]
    )
    conn = create_connection(
        config["paths"]["db"]
    )
    try:
        execute_sql_script(
            conn,
            config["paths"]["create_table"]
        )
        load_to_sqlite(
            carts_data,
            products_data,
            config,
            conn
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
