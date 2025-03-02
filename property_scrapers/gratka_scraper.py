import requests
import json
from bs4 import BeautifulSoup as bs

PROPERTY_TYPES = {
'mieszkania': 'apartments','domy': 'houses', 'dzialki-grunty': 'lands', 'lokale-uzytkowe': 'commercial space',
    'inwestycje-deweloperskie': 'development investments', 'zagraniczne mieszkania': 'foreign apartments'
}
# PROPERTY_TYPES = ['mieszkania', 'domy', 'dzialki-grunty', 'lokale-uzytkowe', ]
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
}
PROPERTY_DETAILS = {}
COUNTER = 1

def get_total_pages():
    r = requests.get(base_url, headers=HEADERS)
    r.raise_for_status()
    soup = bs(r.text, 'html.parser')
    pagination = soup.find('div', class_='UM-euJ Mg95cr')
    if pagination:
        pages = pagination.find_all('span')
        print(pages)
        print(1)
        if pages:
            print(2)
            last_page = pages[-1].get_text(strip=True)
            print(last_page)
            return int(last_page)
    return 1

def scrape(property_type, max_pages=1):
    total_pages = min(get_total_pages(), max_pages)
    for page in range(1, total_pages + 1):
        url = f'{base_url}/?page={page}'
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        soup = bs(r.text, 'html.parser')
        links = soup.find_all('a', class_='ROzmJ2')
        for link in links:
            url = link.get('href')
            if url:
                print(property_type)
                print(f'https://gratka.pl'+url)
                scrape_details(f'https://gratka.pl'+url, property_type)

def scrape_details(link, property_type):
    global COUNTER
    r_property_url = requests.get(link, headers=HEADERS)
    r_property_url.raise_for_status()
    property_soup = bs(r_property_url.text, 'html.parser')

    property_price_soup = property_soup.find('div', class_='_6qd6U8').get_text(strip=True)

    property_basic_info_soup = property_soup.find_all('span', class_='_1Aukq8')
    basic_details = [basic_detail.get_text(strip=True)[1:] for basic_detail in property_basic_info_soup]

    property_advantages_soup = property_soup.find_all('li', class_='Y-8-7T')
    advantages_info = [advantage.get_text(strip=True) for advantage in property_advantages_soup]

    property_description_soup = property_soup.find('div', class_='Kyc-uW').get_text(strip=True).replace("✅", "")

    property_details_soup = property_soup.find_all('div', class_='iT04N1')
    details = [detail.get_text(separator=' ', strip=True) for detail in property_details_soup]

    ##TODO write the rest of this scraper


if __name__ == '__main__':
    for key, value in PROPERTY_TYPES.items():
        base_url = f"https://gratka.pl/nieruchomosci/{key}"
        scrape(key)
