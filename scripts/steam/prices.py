from typing import Optional, Generator, List
from psycopg2 import extensions, Error
from collections import defaultdict
from datetime import datetime
from time import sleep
from utils.constants import (STEAM_SCHEMA, DATABASE_TABLES,
                             S_PRICES_FILE_LOG, CURRENCY)
from utils.database.connector import connect_to_database, insert_data
from utils.fetcher import Fetcher, TooManyRequestsError
from utils.logger import configure_logger

LOGGER = configure_logger(__name__, S_PRICES_FILE_LOG)


class SteamPrices(Fetcher):
    def __init__(self):
        super().__init__()
        self.url = 'https://store.steampowered.com'
        self.prices = self.url + '/api/appdetails/?appids={appids}&cc={currency}&filters=price_overview'
        self.added = 0
    
    @staticmethod
    def _create_batches(appids: List[int], batch_size: int = 100) -> Generator[List[int], None, None]:
        for i in range(0, len(appids), batch_size):
            yield appids[i:i + batch_size]

    def _get_appids(self, connection: extensions.connection) -> List[Optional[int]]:
        try:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT game_id
                    FROM steam.games
                    WHERE game_id NOT IN (SELECT game_id
                                          FROM steam.prices
                                          WHERE date_acquired = '{self._current_data()}');
                """
                cursor.execute(query)
                return [appid[0] for appid in cursor.fetchall()]
        except Exception as e:
            LOGGER.warning('Failed to retrieve the list of appids from the database. ' \
                           'Error: %s', str(e).strip())
            return []

    @staticmethod
    def _current_data() -> str:
        current_date = datetime.now()
        return current_date.strftime('%Y-%m-%d')

    def get_prices(self, connection: extensions.connection, appids: List[int]):
        prices = defaultdict(list)
        # Constructing a string of 100 appids in a single request
        query = ','.join(map(str, appids))
        for currency in CURRENCY['steam']:
            try:
                json_content = self.fetch_data(
                    self.prices.format(appids=query, currency=currency), 'json')
            except TooManyRequestsError:
                # 429 is returned by SteamWebAPI due 
                # to the rate limit of requests within a 5-minute window
                sleep(301)
                json_content = self.fetch_data(
                    self.prices.format(appids=query, currency=currency), 'json')
            for appid in appids:
                try:
                    # The value in JSON is without symbols. For example, 1400 = 14 USD
                    price = json_content.get(str(appid), {}).get(
                        'data', {}).get('price_overview', {}).get('final', None) / 100
                except (TypeError, AttributeError):
                    price = None
                prices[appid].append(price)
        prices = [
            [appid] + currency + [self._current_data()]
            for appid, currency in prices.items()
        ]
        try:
            # DATABASE_TABLES[5] = 'prices'
            insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[5], prices)
            self.added += len(prices)
        except (Error, IndexError) as e:
            LOGGER.warning(f'Data for {self._current_data()} was not successfully inserted. ' \
                           f'Failed appids = {appids}')
            LOGGER.warning(e)

    def start(self):
        with connect_to_database() as connection:
            for appids in self._create_batches(self._get_appids(connection)):
                self.get_prices(connection, appids)
            LOGGER.info(f'Successful inserted "{100 * self.added}" values')

def main():
    SteamPrices().start()