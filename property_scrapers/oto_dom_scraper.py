import requests
import json
from bs4 import BeautifulSoup as bs

PROPERTY_TYPES = {'mieszkanie': 'apartments', 'dom': 'houses', 'dzialka': 'lands', 'lokal': 'commercial space'}
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
}
PROPERTY_DETAILS = {}
COUNTER = 1

def get_total_pages():
    r = requests.get(base_url, headers=HEADERS)
    r.raise_for_status()
    soup = bs(r.text, 'html.parser')
    pagination = soup.find('ul', class_='e1h66krm4 css-iiviho')
    if pagination:
        pages = pagination.find_all('li')
        if pages:
            last_page = pages[-2].get_text(strip=True)
            return int(last_page)
    return 1


def scrape(property_type, max_pages=1):
    total_pages = min(get_total_pages(), max_pages)
    for page in range(1, total_pages + 1):
        url = f'{base_url}/?page={page}'
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        soup = bs(r.text, 'html.parser')
        links = soup.find_all('a', class_='css-16vl3c1 e17g0c820')
        for link in links:
            url = link.get('href')
            if url:
                scrape_details(f'https://otodom.pl'+url, property_type)


def scrape_details(link, property_type):
    global COUNTER
    r_property_url = requests.get(link, headers=HEADERS)
    r_property_url.raise_for_status()
    property_soup = bs(r_property_url.text, 'html.parser')

    offer_details_soup = property_soup.find_all('p', class_='e2md81j2 css-htq2ld')
    offer_upd_date, offer_ins_date, offer_id = [detail.get_text(strip=True).split(":")[1].strip() for detail in offer_details_soup]
    property_price_soup = property_soup.find('strong', class_='css-1o51x5a e1k1vyr21').get_text(strip=True)
    property_descripton_soup = property_soup.find('div', attrs={'data-cy': 'adPageAdDescription'}).get_text(strip=True)
    property_adress_soup = property_soup.find('a', class_='css-1jjm9oe e42rcgs1').get_text(strip=True)
    property_general_info_soup = property_soup.find_all('button', class_='eezlw8k1 css-7kiatb')
    property_details_soup = property_soup.find_all('div', class_='css-t7cajz e15n0fyo1')

    general_info = [info.get_text(strip=True) for info in property_general_info_soup]

    scraped_data = {
        'property_type': property_type,
        'link': link,
        'price': property_price_soup,
        'address': property_adress_soup,
        'general_info': general_info,
        'details': [detail.get_text(separator=' ', strip=True) for detail in property_details_soup],
        'description': property_descripton_soup,
        'offer_insert_date': offer_ins_date,
        'offer_update_date': offer_upd_date,
        'offer_id': offer_id,
        'source': 'otodom.pl'
    }
    PROPERTY_DETAILS[f"item_{COUNTER}"] = scraped_data
    COUNTER += 1


if __name__ == '__main__':
    for key, value in PROPERTY_TYPES.items():
        base_url = f"https://www.otodom.pl/pl/wyniki/sprzedaz/{key}/cala-polska"
        scrape(key)
    with open('otodom_property_details.json', 'w') as json_file:
        json.dump(PROPERTY_DETAILS, json_file, ensure_ascii=False, indent=4)
