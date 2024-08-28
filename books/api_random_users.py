import requests
import logging
import json
import os
from argparse import ArgumentParser

logging.basicConfig(filename='api.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')
load_path = 'raw_data/'


class UserAPI:
    def __init__(self, n_users=10, v_nationalities=['us']):
        self.users_url = 'https://randomuser.me/api/'
        self.params = {'nat': v_nationalities, 'seed': 'foobar', 'results': n_users, 'format': 'json'}

    def fetch_users_from_api(self):
        logging.info(f'Fetching users _data from API: {self.users_url}')
        try:
            r = requests.get(self.users_url, params=self.params)
            if r.status_code == 200:
                users_json_data = r.json()
                logging.info('Successfully fetched users _data from API')
                return users_json_data
        except requests.exceptions.RequestException as e:
            logging.error(f'Failed to fetch users _data from API: {e}')
            return None

    def save_to_file(self, data, target_path):
        if data is not None:
            file_name = os.path.join(target_path, 'api_random_users.json')
            with open(file_name, 'w') as f:
                json.dump(data, f)
            logging.info(f'Saved _data to {file_name}')
        else:
            logging.warning(f'No _data to save for {target_path}')


if __name__ == '__main__':
    parser = ArgumentParser(prog='Get api random users data')
    parser.add_argument('-t', '--target_path')
    args = parser.parse_args()
    user_api = UserAPI()
    users = user_api.fetch_users_from_api()
    user_api.save_to_file(users, target_path=args.target_path)
