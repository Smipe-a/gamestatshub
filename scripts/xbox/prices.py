from psycopg2 import extensions, Error
from datetime import datetime
from bs4 import BeautifulSoup
from typing import Dict
from utils.constants import (XBOX_SCHEMA, DATABASE_TABLES,
                             X_PRICES_FILE_LOG, CURRENCY, MATCH_XBOX_PRICES)
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from scripts import ExophaseAPI

LOGGER = configure_logger(__name__, X_PRICES_FILE_LOG)


class XboxPrices(ExophaseAPI):
    def __init__(self):
        super().__init__()
        self.prices = 'https://psprices.com/{currency}/games/?q={query}&platform=XOne&show=games'

    @staticmethod
    def _current_data() -> str:
        current_date = datetime.now()
        return current_date.strftime('%Y-%m-%d')

    def _get_apps(self, connection: extensions.connection) -> Dict[int, str]:
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
                return {app[0]: app[1] for app in cursor.fetchall()}
        except Exception as e:
            LOGGER.warning(f'Failed to retrieve data from the database. ' \
                           f'Error: {str(e).strip()}')
            return {}

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
            games = soup.find_all('div', class_='grid grid-cols-12 gap-3')[1].find_all('div',
                class_='col-span-6 sm:col-span-4 md:col-span-3 lg:col-span-2')
            candidates = {}
            for candidate in games:
                candidate_title = candidate.find('span',
                    class_='line-clamp-2 h-10 underline-offset-2 group-hover:underline').text.strip()
                try:
                    candidate_price = float(candidate.find('span',
                        class_='text-lg font-bold').text.strip().replace(',', '.').replace('\xa0', ''))
                    candidates[candidate_title] = candidate_price
                except AttributeError:
                    # Price not found for one of the candidates
                    pass
            best_match = self._find_best_match(title, candidates.keys(), MATCH_XBOX_PRICES)
            if best_match:
                prices.append(candidates[best_match])
            else:
                prices.append(None)
        try:
            # DATABASE_TABLES[5] = 'prices'
            insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[5],
                        [[appid] + prices + [self._current_data()]])
        except (IndexError, Error) as e:
            LOGGER.warning('Failed to insert data into the database. Error: %s', e)

    def start(self):
        with connect_to_database() as connection:
            for appid, title in self._get_apps(connection).items():
                self.get_prices(connection, appid, title)

def main():
    XboxPrices().start()
