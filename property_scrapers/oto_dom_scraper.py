import requests
from bs4 import BeautifulSoup as bs

property_types = {'mieszkanie': 'apartments', 'dom': 'houses', 'dzialka': 'lands', 'lokal': 'commercial space'}


def get_total_pages():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
    }
    r = requests.get(base_url, headers=headers)
    r.raise_for_status()
    soup = bs(r.text, 'html.parser')
    pagination = soup.find('ul', class_='e1h66krm4 css-iiviho')
    if pagination:
        pages = pagination.find_all('li')
        if pages:
            last_page = pages[-2].get_text(strip=True)
            return int(last_page)
    return 1


def scrape_urls(max_pages=1):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
    }
    total_pages = min(get_total_pages(), max_pages)
    for page in range(1, total_pages + 30):
        url = f'{base_url}/?page={page}'
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        soup = bs(r.text, 'html.parser')
        links = soup.find_all('a', class_='css-16vl3c1 e17g0c820')
        for link in links:
            url = link.get('href')
            print('otodom.pl'+url)


if __name__ == '__main__':
    for key, value in property_types.items():
        base_url = f"https://www.otodom.pl/pl/wyniki/sprzedaz/{key}/cala-polska"
        print(f'Scraping {value} links')
        get_total_pages()
        scrape_urls()
