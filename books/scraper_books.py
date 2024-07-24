import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import logging
from argparse import ArgumentParser
from datetime import datetime
import os


# Configure logging
logging.basicConfig(filename='api.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


class BookScraper:
    BASE_URL = 'https://libra.ibuk.pl'

    def __init__(self):
        self.results = []
        self.file_name = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')+'.csv'
        logging.info("BookScraper initialized")

    def get_total_pages(self):
        logging.info("Fetching total number of pages")
        r = requests.get(self.BASE_URL+'/ksiazki')
        r.raise_for_status()
        soup = bs(r.text, 'html.parser')
        pagination = soup.find('ul', class_='pagination')
        if pagination:
            pages = pagination.find_all('li')
            if pages:
                last_page = pages[-2].get_text(strip=True)
                logging.info(f"Found {last_page} pages")
                return int(last_page)
        return 1

    def scrape(self, max_pages=1):
        logging.info(f"Starting to scrape up to {max_pages} pages")
        total_pages = min(self.get_total_pages(), max_pages)
        for page in range(1, total_pages + 1):
            url = f'{self.BASE_URL}/ksiazki?_page={page}'
            logging.info(f"Scraping page {page} of {total_pages}: {url}")
            r = requests.get(url)
            r.raise_for_status()
            soup = bs(r.text, 'html.parser')
            links = soup.find_all('a', class_='no-text-decoration il-textcolor-extradark il-fontweight-600')
            for link in links:
                url = link.get('href')
                if url:
                    self.scrape_book_details(url)

    def scrape_book_details(self, link):
        book_url = self.BASE_URL + link
        logging.info(f"Scraping book details: {book_url}")
        r_book_url = requests.get(book_url)
        r_book_url.raise_for_status()
        web2 = bs(r_book_url.text, 'html.parser')

        title = web2.find('h1', class_='header-size my-3')
        author = web2.find('span', class_='il-font-size il-textcolor-light-secondary')

        table = {
            'Title': title.get_text(strip=True),
            'Author': author.get_text(strip=True)
        }

        tbody = web2.find('tbody')
        if tbody:
            trs = tbody.find_all('tr')
            for tr in trs:
                tds = tr.find_all('td')
                if len(tds) == 2:
                    name, attribute = tds
                    table[name.get_text(strip=True)] = attribute.get_text(strip=True)

        self.results.append(table)
        logging.info(f"Added book _data: {table}")

    def get_data_frame(self):
        return pd.DataFrame(self.results)

    def save_to_csv(self, target_path):
        file_name = os.path.join(target_path, self.file_name)
        df = self.get_data_frame()
        logging.info(f"Saving _data to CSV file: {file_name}")
        df.to_csv(file_name, sep=';', index=False, header=True)
        logging.info("_data saved successfully")

if __name__ == '__main__':
    # python books/scraper_books.py -t './_data/books/raw' -l 2
    parser = ArgumentParser(prog='Books Scraper')
    parser.add_argument('-t', '--target_path')
    parser.add_argument('-l', '--limit_pages', default=1, type=int)
    args = parser.parse_args()
    scraper = BookScraper()
    scraper.scrape(max_pages=args.limit_pages)
    scraper.save_to_csv(target_path=args.target_path)
    print(scraper.get_data_frame().to_markdown())

