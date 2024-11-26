from bs4 import BeautifulSoup
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
        self.leaderboard = 'https://psnprofiles.com/leaderboard{}'
    
    def _get_last_page(self) -> int:
        try:
            html_content = self.fetch_data(self.leaderboard.format(''))
            soup = BeautifulSoup(html_content, 'html.parser')
            return int(soup.find('ul', class_='pagination').find_all(
                'li')[-2].text.strip().replace(',', ''))
        except Exception as e:
            LOGGER.error('Failed to retrieve the number of pages ' \
                         'for data collection. The process was ' \
                         'interrupted with error: %s', str(e).strip())
            return 0
    
    def get_players(self):
        with connect_to_database() as connection:
            last_page = self._get_last_page()
            for page in range(1, last_page + 1):
                players = []
                try:
                    html_content = self.fetch_data(self.leaderboard.format(f'/all?page={page}'))
                    soup = BeautifulSoup(html_content, 'html.parser')
                except Exception as e:
                    LOGGER.warning(f'Failed to retrieve the code ' \
                                   f'of page "{page}". Error: {str(e).strip()}')
                    continue
                # Get all players on page
                players_on_page = soup.find('table', class_='leaderboard zebra').find_all('tr')[1:]
                for player in players_on_page:
                    nickname = player.find('td',
                        style='text-align:left;').find('a').text.strip()
                    country = player.find('td',
                        style='padding-left: 10px;').find('img').get('title')
                    players.append([nickname, country])
                try:
                    # DATABASE_TABLES[2] = 'players'
                    insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[2], players)
                    # Increasing the delay for data retrieval as 
                    # the site is returning a 429 error
                    sleep(0.5)
                except psycopg2.Error as e:
                    LOGGER.warning(f'Error on page "{page}": {str(e).strip()}')

def main():
    PSNPlayers().get_players()
