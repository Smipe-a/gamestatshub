from typing import Optional, Any
from time import sleep
import requests


class TooManyRequestsError(Exception):
    pass

class Fetcher:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'
        }

    def fetch_data(self, url: str, content_type: str = 'html', retries: int = 3, delay: int = 1) -> Optional[Any]:
        attempt = 0
        while attempt < retries:
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                if content_type == 'json':
                    return response.json()
                elif content_type == 'html':
                    return response.content
                else:
                    raise TypeError(f'Unsupported content type: {content_type}.')
            except requests.RequestException as e:
                if response.status_code == 429:
                    print('Too many requests. Waiting for 5 minute...')
                    raise TooManyRequestsError
                elif isinstance(e, requests.exceptions.ConnectionError) and '104' in str(e):
                    attempt += 1
                    sleep(delay)
                else:
                    raise e
        error_message = f'Failed to complete request after {retries} attempts.'
        raise requests.RequestException(error_message)
