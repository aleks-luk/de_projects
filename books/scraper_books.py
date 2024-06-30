import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import logging

# Configure logging
logging.basicConfig(filename='api.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


class BookScraper:
    def __init__(self, base_url, landing_page):
        self.base_url = base_url
        self.landing_page = landing_page
        self.results = []
        logging.info("BookScraper initialized")

    def get_total_pages(self):
        logging.info("Fetching total number of pages")
        try:
            r = requests.get(self.base_url)
            r.raise_for_status()
            soup = bs(r.text, 'html.parser')
            pagination = soup.find('ul', class_='pagination')
            if pagination:
                pages = pagination.find_all('li')
                if pages:
                    last_page = pages[-2].get_text(strip=True)
                    logging.info(f"Found {last_page} pages")
                    return int(last_page)
        except requests.RequestException as e:
            logging.error(f"Error fetching total pages: {e}")
        return 1

    def scrape(self, max_pages=1):
        logging.info(f"Starting to scrape up to {max_pages} pages")
        total_pages = min(self.get_total_pages(), max_pages)
        for page in range(1, total_pages + 1):
            url = f'{self.base_url}?_page={page}'
            logging.info(f"Scraping page {page} of {total_pages}: {url}")
            try:
                r = requests.get(url)
                r.raise_for_status()
                soup = bs(r.text, 'html.parser')
                links = soup.find_all('a', class_='no-text-decoration il-textcolor-extradark il-fontweight-600')
                for link in links:
                    self.scrape_book_details(link)
            except requests.RequestException as e:
                logging.error(f"Error scraping page {page}: {e}")

    def scrape_book_details(self, link):
        book_url = self.landing_page + link.get('href')
        logging.info(f"Scraping book details: {book_url}")
        try:
            r_book_url = requests.get(book_url)
            r_book_url.raise_for_status()
            web2 = bs(r_book_url.text, 'html.parser')

            title = web2.find('h1', class_='header-size my-3')
            author = web2.find('span', class_='il-font-size il-textcolor-light-secondary')

            table = {
                'Title': title.get_text(strip=True) if title else 'N/A',
                'Author': author.get_text(strip=True) if author else 'N/A'
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
        except requests.RequestException as e:
            logging.error(f"Error scraping book details: {e}")

    def get_data_frame(self):
        return pd.DataFrame(self.results)

    def save_to_csv(self, filename):
        df = self.get_data_frame()
        logging.info(f"Saving _data to CSV file: {filename}")
        df.to_csv(filename, sep=';', index=False, header=True)
        logging.info("_data saved successfully")

# Example usage
base_url = 'https://libra.ibuk.pl/ksiazki'
landing_page = 'https://libra.ibuk.pl'

scraper = BookScraper(base_url, landing_page)
scraper.scrape()
scraper.save_to_csv('raw_data/scrapped_book_data.csv')