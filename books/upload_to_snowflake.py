from snowflake.snowpark.session import Session

if __name__ == '__main__':
    snowflake = Session.builder.configs({
        "connection_name" : "dev"
    }).create()
    snowflake.sql("""
        CREATE schema if not exists raw.aleks
        """).show()
    snowflake.sql("""
    CREATE OR REPLACE STAGE raw.aleks.books_stage
    """).show()
    snowflake.sql("""
        CREATE OR REPLACE TEMP TABLE raw.aleks.tmp_books(
            Title varchar,
            Author varchar,
            ISBN_ISSN varchar,
            DOI varchar,
            Wydawnictwo varchar,
            Rok_wydania varchar,
            Liczba_stron varchar,
            XML_ISBN_ISSN varchar
        )
        """).show()
    snowflake.sql("""
        CREATE TRANSIENT TABLE if not exists raw.aleks.books(
            Title varchar,
            Author varchar,
            ISBN_ISSN varchar,
            DOI varchar,
            Wydawnictwo varchar,
            Rok_wydania varchar,
            Liczba_stron varchar,
            XML_ISBN_ISSN varchar,
            _upload_date_time timestamp_ntz(9)
        )
        """).show()
    snowflake.sql("""
    PUT file:///Users/alex/git_repos/de_projects/_data/books/raw/scraped_books_file.csv @raw.aleks.books_stage
    """).show()
    snowflake.sql("""
    COPY INTO raw.aleks.tmp_books from 
    @raw.aleks.books_stage
    FILE_FORMAT = (type= csv, field_delimiter= ';' skip_header= 1 field_optionally_enclosed_by= '"')
    """).show()
    snowflake.sql("""
    INSERT INTO raw.aleks.books
    select *, current_timestamp() as _upload_date_time  from raw.aleks.tmp_books
    """).show()