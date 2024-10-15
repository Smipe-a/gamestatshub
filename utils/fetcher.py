from typing import Optional, Any
from time import sleep
import requests


class Fetcher:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'
        }

    def fetch_data(self, url: str, retries: int = 3, delay: int = 1) -> Optional[Any]:
        attempt = 0
        while attempt < retries:
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                return response.content
            except requests.RequestException as e:
                if isinstance(e, requests.exceptions.ConnectionError) and '104' in str(e):
                    attempt += 1
                    sleep(delay)
                else:
                    raise e
        error_message = f'Failed to complete request after {retries} attempts.'
        raise requests.RequestException(error_message)
