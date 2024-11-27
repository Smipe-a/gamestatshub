from concurrent.futures import ThreadPoolExecutor
from psycopg2 import extensions, Error
from bs4 import BeautifulSoup
from utils.constants import XBOX_SCHEMA, DATABASE_TABLES, X_PLAYERS_FILE_LOG
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from scripts.xbox import Xbox

LOGGER = configure_logger(__name__, X_PLAYERS_FILE_LOG)


class XboxPlayers(Xbox):
    def __init__(self):
        super().__init__()
        self.leaderboard = self.url + '/xbox/leaderboard/page/{page}/'
        self.profiles = 0

    def get_players(self, connection: extensions.connection, page: int):
        html_content = self._request(self.leaderboard.format(page=page))
        soup = BeautifulSoup(html_content, 'html.parser')
        try:
            players = soup.find('table', class_='table').find_all('tr', class_='player')
        except AttributeError:
            # Players data is missing on the page, but the page exists
            return
        data_players = []
        for player in players:
            profile = player.find('td', class_='username_inner').find('a').get('href')
            html_content = self._request(self.url + profile)
            soup = BeautifulSoup(html_content, 'html.parser')
            player = soup.find('section', class_='section-profile-header pb-3').find('div')
            nickname = player.get('data-username')
            playerid = player.get('data-playerid')
            data_players.append([playerid, nickname])
            self.profiles += 1
        try:
            # DATABASE_TABLES[2] = 'players'
            insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[2], data_players)
        except (Error, IndexError) as e:
            LOGGER.warning(e)
            self.profiles -= len(data_players)

    def start(self):
        with connect_to_database() as connection:
            with ThreadPoolExecutor() as executor:
                executor.map(lambda page: self.get_players(connection, page),
                             range(1, self.last_page(self.leaderboard.format(page=1)) + 1))
            LOGGER.info(f'Received "{self.profiles}" profiles during execution')

def main():
    XboxPlayers().start()
