from bs4 import BeautifulSoup
from typing import Optional
from time import sleep
import psycopg2
from utils.constants import DATABASE_TABLES, PLAYSTATION_SCHEMA, P_PLAYERS_FILE_LOG
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from utils.fetcher import Fetcher

LOGGER = configure_logger(__name__, P_PLAYERS_FILE_LOG)


class PSNPlayers(Fetcher):
    def __init__(self):
        super().__init__()
        self.url = 'https://psnprofiles.com'
    
    def _get_last_page(self) -> Optional[int]:
        try:
            html_content = self.fetch_data(f'{self.url}/leaderboard')
            soup = BeautifulSoup(html_content, 'html.parser')
            return int(soup.find('ul', class_='pagination').find_all(
                'li')[-2].text.strip().replace(',', ''))
        except Exception as e:
            LOGGER.error('Failed to retrieve the number of pages ' \
                         'for data collection. The process was ' \
                         'interrupted with error:', str(e).strip())
            return None
    
    def get_players(self):
        with connect_to_database() as connection:
            last_page = self._get_last_page()
            if last_page:
                for page in range(1, last_page + 1):
                    players = []
                    try:
                        html_content = self.fetch_data(
                            f'{self.url}/leaderboard/all?page={page}')
                        soup = BeautifulSoup(html_content, 'html.parser')
                    except Exception as e:
                        LOGGER.warning(f'Failed to retrieve the code ' \
                                       f'of page "{page}". Error:', str(e).strip())
                        continue
                    # Get all players on page
                    table = soup.find('table', class_='leaderboard zebra')
                    # table.tr 0 position is empty
                    for row in table.find_all('tr')[1:]:
                        nickname = row.find('td',
                            style='text-align:left;').find('a').text.strip()
                        country = row.find('td',
                            style='padding-left: 10px;').find('img').get('title')
                        players.append([nickname, country])
                    try:
                        # DATABASE_TABLES[2] = 'players'
                        insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[2], players)
                        # Increasing the delay for data retrieval as 
                        # the site is returning a 429 error
                        sleep(0.5)
                    except psycopg2.Error as e:
                        LOGGER.warning(f'Error on page "{page}":', str(e).strip())

def main():
    PSNPlayers().get_players()
