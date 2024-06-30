import pandas as pd
import json
import os


os.environ['LOAD_PATH'] = 'files_to_load/'
os.environ['RAW_DATA_PATH'] = './raw_data/'

load_path = os.environ['LOAD_PATH']
raw_data_path = os.environ['RAW_DATA_PATH']


def transform_books():
    books_frame = pd.read_csv(os.path.join(raw_data_path, 'books.csv'), delimiter=';',
                              usecols=['ISBN', 'Book-Title', 'Book-Author', 'Year-Of-Publication', 'Publisher', 'Image-URL-S'],
                              encoding='windows-1250', low_memory=False)

    new_books_column_names = {
        'ISBN': 'book_isbn',
        'Book-Title': 'book_title',
        'Book-Author': 'book_author',
        'Year-Of-Publication': 'year_published',
        'Publisher': 'publisher',
        'Image-URL-S': 'book_image_url'
    }
    books_frame.rename(columns=new_books_column_names, inplace=True)
    books_frame.to_csv(os.path.join(load_path, 'books_data_transformation_v1.csv'), sep=';', index=False, header=True)


def transform_users():
    users_frame = pd.read_csv(os.path.join(raw_data_path, 'users.csv'), usecols=['Location', 'Age'], delimiter=';', encoding='windows-1250', low_memory=False)
    df_location = users_frame['Location'].str.split(',', expand=True)
    df_location = df_location.drop(columns=[1, 3, 4, 5, 6, 7, 8])

    users_frame = users_frame.drop(columns=['Location'])
    users_frame = pd.concat([users_frame, df_location], axis=1)
    new_users_column_names = {
        'Age': 'user_age',
        0: 'user_city',
        2: 'user_country'
    }

    users_frame.rename(columns=new_users_column_names, inplace=True)
    users_frame['user_city'] = users_frame['user_city'].str.title()
    users_frame['user_country'] = users_frame['user_country'].str.title()
    users_frame['user_age'] = users_frame['user_age'].fillna(0).astype(int)

    users_frame.to_csv(os.path.join(load_path, 'users_data_transformation_v1.csv'), sep=';', index=False, header=True)


def transform_ratings():
    ratings_frame = pd.read_csv(os.path.join(raw_data_path, 'ratings.csv'), usecols=['ISBN', 'Book-Rating'], delimiter=';', encoding='windows-1250', low_memory=False)
    new_ratings_column_names = {
        'ISBN': 'r_book_isbn',
        'Book-Rating': 'r_book_rating',
    }
    ratings_frame.rename(columns=new_ratings_column_names, inplace=True)
    ratings_frame.to_csv(os.path.join(load_path, 'ratings_data_transformation_v1.csv'), sep=';', index=False, header=True)


def transform_user_api_data():
    with open(os.path.join(raw_data_path, 'users_api_data.json'), 'r', encoding='utf-8') as f:
        users_api_data = json.load(f)
    users = users_api_data.get('results', [])
    df_users = pd.json_normalize(users)
    df_users.reset_index(inplace=True)
    df_users = df_users.drop(columns=[
        'index', 'gender', 'email', 'phone', 'cell', 'nat', 'name.title',
        'name.first', 'name.last', 'location.street.number',
        'location.street.name', 'location.state', 'location.postcode',
        'location.coordinates.latitude', 'location.coordinates.longitude',
        'location.timezone.offset', 'location.timezone.description',
        'login.uuid', 'login.username', 'login.password', 'login.salt',
        'login.md5', 'login.sha1', 'login.sha256', 'dob.date', 'dob.age',
        'registered.date', 'id.name', 'id.value',
        'picture.large', 'picture.medium', 'picture.thumbnail'
    ])

    new_api_users_column_names = {
        'location.city': 'user_city',
        'location.country': 'user_country',
        'registered.age': 'user_age'
    }
    df_users.rename(columns=new_api_users_column_names, inplace=True)
    df_users['user_age'] = df_users['user_age'].fillna(0).astype(int)
    df_users.to_csv(os.path.join(load_path, 'user_api_data_transformation_v1.csv'), sep=';', index=False, header=True)


def transform_scrapped_books_data():
    scrapped_data = pd.read_csv(os.path.join(raw_data_path, 'scrapped_book_data.csv'), delimiter=';', usecols=['Title', 'Author', 'ISBN/ISSN:', 'Wydawnictwo:', 'Rok wydania:'], low_memory=False)
    new_scraped_data_column_names = {
        'ISBN/ISSN:': 'book_isbn',
        'Title': 'book_title',
        'Author': 'book_author',
        'Rok wydania:': 'year_published',
        'Wydawnictwo:': 'publisher',
    }
    scrapped_data.rename(columns=new_scraped_data_column_names, inplace=True)
    scrapped_data['year_published'] = scrapped_data['year_published'].fillna(0).astype(int)
    scrapped_data.to_csv(os.path.join(load_path, 'scrapped_books_data_transformation_v1.csv'), sep=';', index=False, header=True)


if __name__ == '__main__':
    transform_books()
    transform_users()
    transform_ratings()
    transform_user_api_data()
    transform_scrapped_books_data()
