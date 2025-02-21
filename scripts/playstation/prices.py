from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Tuple
from psycopg2 import extensions, Error
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
from utils.constants import (PLAYSTATION_SCHEMA, DATABASE_TABLES,
                             PLAYSTATION_LOGS, CURRENCY)
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from scripts import ExophaseAPI

LOGGER = configure_logger(Path(__file__).name, PLAYSTATION_LOGS)


class PlayStationPrices(ExophaseAPI):
    def __init__(self, process: str):
        super().__init__()
        self.process = process

        self.prices = 'https://psprices.com/{currency}/games/?q={query}&platform={platform}&show=games'

        # Number of records added to the 'prices' table
        self.added = 0

    @staticmethod
    def _current_data() -> str:
        current_date = datetime.now()
        return current_date.strftime('%Y-%m-%d')

    def _get_appids(self, connection: extensions.connection) -> List[Optional[Tuple[int, str]]]:
        try:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT gameid, title, platform
                    FROM playstation.games
                    WHERE gameid NOT IN (SELECT gameid
                                         FROM playstation.prices
                                         WHERE date_acquired = '{self._current_data()}');
                """
                cursor.execute(query)
                # app[0] - appid; app[1] - title; app[2] - platform
                return [(app[0], app[1], app[2]) for app in cursor.fetchall()]
        except Exception as e:
            LOGGER.error('Failed to retrieve the list of appids from the database. ' \
                         'Error: %s', str(e).strip())
            return []

    def get_prices(self, connection: extensions.connection,
                   appid: int, title: str, platform: str):
        if platform == 'PS Vita':
            platform = 'PSVita'
        
        prices = []
        for currency in CURRENCY['playstation']:
            html_content = self._request(self.prices.format(
                currency=currency, query=self._construct_query(title), platform=platform))
            soup = BeautifulSoup(html_content, 'html.parser')
            
            games = soup.find('div', class_='grid grid-cols-12 gap-3').find_all('div',
                class_='col-span-6 sm:col-span-4 md:col-span-3 lg:col-span-2')
            
            candidates = {}
            for candidate in games:
                candidate_title = candidate.find('span',
                    class_='line-clamp-2 h-10 underline-offset-2 group-hover:underline text-gray-900 ' + \
                        'dark:text-gray-50 group-hover:text-primary-600 ' + \
                        'dark:group-hover:text-primary-400 transition-colors').text.strip()
                
                try:
                    candidate_price = candidate.find('span',
                        class_='inline-flex items-center space-x-0.5').text.strip().replace(',', '.')
                    
                    for element in {'$', '£', '€', '₽', '￥', '\xa0'}:
                        candidate_price = candidate_price.replace(element, '')
                        
                    if currency == 'region-jp':
                        candidate_price = candidate_price.replace('.', '')

                    if candidate_price == 'Free':
                        candidate_price = 0
                    else:
                        candidates[candidate_title] = float(candidate_price)
                except AttributeError:
                    # Price not found for one of the candidates
                    pass
            
            best_match = self._find_best_match(title, candidates.keys())
            if best_match:
                prices.append(candidates[best_match])
            else:
                prices.append(None)

        try:
            # DATABASE_TABLES[5] = 'prices'
            insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[5],
                        [[appid] + prices + [self._current_data()]])
            self.added += 1
        except (IndexError, Error) as e:
            LOGGER.warning('Failed to insert data into the database. Error: %s', e)

    def start(self):
        with connect_to_database() as connection:
            with ThreadPoolExecutor() as executor:
                executor.map(lambda appid, title, platform:
                             self.get_prices(connection, appid, title, platform),
                             *zip(*self._get_appids(connection)))
        
            LOGGER.info(f'Added "{self.added}" new data to the table "playstation.{self.process}"')

def main():
    try:
        process = 'prices'
        LOGGER.info(f'Process started')

        playstation_prices = PlayStationPrices(process)
        playstation_prices.start()
    except (Exception, KeyboardInterrupt) as e:
        if str(e) == '':
            e = 'Forced termination'
        LOGGER.error(f'An unhandled exception occurred with error: {str(e).strip()}')
        LOGGER.info(f'Added "{playstation_prices.added}" new data to the table "playstation.{process}"')
        
        raise Exception(e)
