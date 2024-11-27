from bs4 import BeautifulSoup
from utils.fetcher import Fetcher
import urllib.request


class Xbox(Fetcher):
    def __init__(self):
        super().__init__()
        self.url = 'https://www.exophase.com'

    @staticmethod
    def _request(url: str) -> str:
        request = urllib.request.Request(url)
        request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0')
        request.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8')
        request.add_header('Accept-Language', 'en-US,en;q=0.5')
        return urllib.request.urlopen(request).read().decode('utf-8')

    def last_page(self, url: str) -> int:
        try:
            html_content = self._request(url)
            soup = BeautifulSoup(html_content, 'html.parser')
            pagination = soup.find('ul', class_='p-4 pagination justify-content-center')
            return int(pagination.find_all('li')[-2].text.strip())
        except Exception as e:
            print(f'Failed to retrieve the last page number. Error: {str(e).strip()}')
            return 0
