import pandas as pd
import json
import os
import logging
from argparse import ArgumentParser
from datetime import datetime


logging.basicConfig(filename='api.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


class DataTransformation:

    def __init__(self):
        self.data_frames = {}
        logging.info("DataTransformation initialized")

    def transform_books(self, source_path):
        books_frame = pd.read_csv(os.path.join(source_path, 'books.csv'), delimiter=';',
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
        self.data_frames['books'] = books_frame

    def transform_users(self, source_path):
        users_frame = pd.read_csv(os.path.join(source_path, 'users.csv'), usecols=['Location', 'Age'], delimiter=';', encoding='windows-1250', low_memory=False)
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
        self.data_frames['users'] = users_frame

    def transform_ratings(self, source_path):
        ratings_frame = pd.read_csv(os.path.join(source_path, 'ratings.csv'), usecols=['ISBN', 'Book-Rating'], delimiter=';', encoding='windows-1250', low_memory=False)
        new_ratings_column_names = {
            'ISBN': 'r_book_isbn',
            'Book-Rating': 'r_book_rating',
        }
        ratings_frame.rename(columns=new_ratings_column_names, inplace=True)
        self.data_frames['ratings'] = ratings_frame

    def transform_user_api_data(self, source_path):
        with open(os.path.join(source_path, 'users_api_data.json'), 'r', encoding='utf-8') as f:
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
        self.data_frames['api_users'] = df_users

    def transform_scrapped_books_data(self, source_path):
        scrapped_data = pd.read_csv(os.path.join(source_path, 'scrapped_book_data.csv'), delimiter=';', usecols=['Title', 'Author', 'ISBN/ISSN:', 'Wydawnictwo:', 'Rok wydania:'], low_memory=False)
        new_scraped_data_column_names = {
            'ISBN/ISSN:': 'book_isbn',
            'Title': 'book_title',
            'Author': 'book_author',
            'Rok wydania:': 'year_published',
            'Wydawnictwo:': 'publisher',
        }
        scrapped_data.rename(columns=new_scraped_data_column_names, inplace=True)
        scrapped_data['year_published'] = scrapped_data['year_published'].fillna(0).astype(int)
        self.data_frames['scrapped_data'] = scrapped_data

    def save_to_csv(self, data_type, target_path):
        df = self.data_frames.get(data_type)
        if df is not None:
            file_name = os.path.join(target_path, f"transformed_{data_type}_file_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.csv")
            df.to_csv(file_name, index=False)
            logging.info(f"{data_type.capitalize()} data saved to {file_name}")
        else:
            logging.error(f"No data found for type: {data_type}")


if __name__ == '__main__':
    parser = ArgumentParser(prog='Data Transformation')
    parser.add_argument('-t', '--target_path', default='./_data/books/transformed', type=str)
    parser.add_argument('-s', '--source_path', default='./_data/books/raw', type=str)
    args = parser.parse_args()
    transformation = DataTransformation()
    transformation.transform_books(source_path=args.source_path)
    transformation.transform_users(source_path=args.source_path)
    transformation.transform_ratings(source_path=args.source_path)
    # transformation.transform_user_api_data(source_path=args.source_path)
    # transformation.transform_scrapped_books_data(source_path=args.source_path) >I think we should delete fun save to csv from scrapper_books.py in order to have transformation and saving files in one place
    transformation.save_to_csv('books', target_path=args.target_path)
    transformation.save_to_csv('users', target_path=args.target_path)
    transformation.save_to_csv('ratings', target_path=args.target_path)
    # transformation.save_to_csv('api_users', target_path=args.target_path)
    # python books/data_transformation.py -t './_data/books/transformed' -s './_data/books/raw'
