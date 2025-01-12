from concurrent.futures import ThreadPoolExecutor
from psycopg2 import extensions, Error
from bs4 import BeautifulSoup
from pathlib import Path
from utils.database.connector import connect_to_database, insert_data
from utils.constants import XBOX_SCHEMA, DATABASE_TABLES, XBOX_LOGS
from utils.logger import configure_logger
from scripts import ExophaseAPI

LOGGER = configure_logger(Path(__file__).name, XBOX_LOGS)


class XboxPlayers(ExophaseAPI):
    def __init__(self, process: str):
        super().__init__()
        self.process = process

        self.leaderboard = self.url + '/xbox/leaderboard/page/{page}/'

        # Number of records added to the 'players' table
        self.added = 0

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
        
        try:
            # DATABASE_TABLES[2] = 'players'
            insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[2], data_players)
            self.added += len(data_players)
        except (Error, IndexError) as e:
            LOGGER.error(e)

    def start(self):
        with connect_to_database() as connection:
            with ThreadPoolExecutor() as executor:
                executor.map(lambda page: self.get_players(connection, page),
                             range(1, self.last_page(self.leaderboard.format(page=1)) + 1))
            
        LOGGER.info(f'Added "{self.added}" new data to the table "xbox.{self.process}"')

def main():
    process = 'players'
    LOGGER.info(f'Process started')

    xbox_players = XboxPlayers()

    try:
        xbox_players.start()
    except (Exception, KeyboardInterrupt) as e:
        if str(e) == '':
            e = 'Forced termination'
        LOGGER.error(f'An unhandled exception occurred with error: {str(e).strip()}')
        LOGGER.info(f'Added "{xbox_players.added}" new data to the table "xbox.{process}"')
        
        raise Exception(e)
