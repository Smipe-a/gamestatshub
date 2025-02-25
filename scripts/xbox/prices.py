from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Tuple
from psycopg2 import extensions, Error
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
from utils.constants import (XBOX_SCHEMA, DATABASE_TABLES,
                             XBOX_LOGS, CURRENCY)
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from scripts import ExophaseAPI

LOGGER = configure_logger(Path(__file__).name, XBOX_LOGS)


class XboxPrices(ExophaseAPI):
    def __init__(self, process: str):
        super().__init__()
        self.process = process

        self.prices = 'https://psprices.com/{currency}/games/?q={query}&platform=XOne&show=games'

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
                    SELECT gameid, title
                    FROM xbox.games
                    WHERE gameid NOT IN (SELECT gameid
                                         FROM xbox.prices
                                         WHERE date_acquired = '{self._current_data()}');
                """
                cursor.execute(query)
                # app[0] - appid; app[1] - title
                return [(app[0], app[1]) for app in cursor.fetchall()]
        except Exception as e:
            LOGGER.error('Failed to retrieve the list of appids from the database. ' \
                         'Error: %s', str(e).strip())
            return []

    def get_prices(self, connection: extensions.connection, appid: int, title: str):
        prices = []
        for currency in CURRENCY['xbox']:
            # The price data for the JP region is unavailable on the website
            if currency == 'region-jp':
                prices.append(None)
                continue

            html_content = self._request(self.prices.format(
                currency=currency, query=self._construct_query(title)))
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
                    for element in {'$', '£', '€', '₽', '\xa0'}:
                        candidate_price = candidate_price.replace(element, '')
                    
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
            insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[5],
                        [[appid] + prices + [self._current_data()]])
            self.added += 1
        except (IndexError, Error) as e:
            LOGGER.warning('Failed to insert data into the database. Error: %s', e)

    def start(self):
        with connect_to_database() as connection:
            with ThreadPoolExecutor() as executor:
                executor.map(lambda appid, title:
                             self.get_prices(connection, appid, title),
                             *zip(*self._get_appids(connection)))
        
            LOGGER.info(f'Added "{self.added}" new data to the table "xbox.{self.process}"')

def main():
    try:
        process = 'prices'
        LOGGER.info(f'Process started')

        xbox_prices = XboxPrices(process)
        xbox_prices.start()
    except (Exception, KeyboardInterrupt) as e:
        if str(e) == '':
            e = 'Forced termination'
        LOGGER.error(f'An unhandled exception occurred with error: {str(e).strip()}')
        LOGGER.info(f'Added "{xbox_prices.added}" new data to the table "xbox.{process}"')
        
        raise Exception(e)
