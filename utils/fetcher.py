from typing import Optional, Any
import requests


class TooManyRequestsError(Exception):
    pass

class ForbiddenError(Exception):
    pass

class Fetcher:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'
        }

    def fetch_data(self, url: str, content_type: str = 'html') -> Optional[Any]:
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            if content_type == 'json':
                return response.json()   
            elif content_type == 'html':
                return response.content
            else:
                raise TypeError(f'Unsupported content type: {content_type}.')
        except UnboundLocalError:
            raise UnboundLocalError('Unexpected connection loss. Attempting to reconnect')
        except requests.exceptions.JSONDecodeError:
            raise requests.exceptions.JSONDecodeError("Invalid JSON response", "Not a valid json", 0)
        except requests.RequestException as e:
            if response.status_code == 429:
                print('Too many requests. Waiting for 5 minute...')
                raise TooManyRequestsError
            elif response.status_code in {401, 403, 502}:
                # It is unclear why a 401, 403, 502 is being caught
                raise ForbiddenError
            else:
                raise e
