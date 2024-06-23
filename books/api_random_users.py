import requests
import logging
import json

logging.basicConfig(filename='api.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')
load_path = 'raw_data/'

class UserAPI:
    def __init__(self, n_users=10, v_nationalities=['us']):
        self.users_url = 'https://randomuser.me/api/'
        self.params = {'nat': v_nationalities, 'seed': 'foobar', 'results': n_users, 'format': 'json'}

    def fetch_users_from_api(self):
        logging.info(f'Fetching users data from API: {self.users_url}')
        try:
            r = requests.get(self.users_url, params=self.params)
            if r.status_code == 200:
                users_json_data = r.json()
                logging.info('Successfully fetched users data from API')
                return users_json_data
        except requests.exceptions.RequestException as e:
            logging.error(f'Failed to fetch users data from API: {e}')
            return None

    def save_to_file(self, data, filename):
        if data is not None:
            file_path = f'./{filename}'
            with open(file_path, 'w') as f:
                json.dump(data, f)
            logging.info(f'Saved data to {file_path}')
        else:
            logging.warning(f'No data to save for {filename}')

if __name__ == '__main__':
    user_api = UserAPI()
    users = user_api.fetch_users_from_api()
    user_api.save_to_file(users, load_path +'users_api_data.json')
