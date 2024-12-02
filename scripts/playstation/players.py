from concurrent.futures import ThreadPoolExecutor
from psycopg2 import extensions, Error
from bs4 import BeautifulSoup
from typing import Optional
import pycountry
from utils.constants import PLAYSTATION_SCHEMA, DATABASE_TABLES, P_PLAYERS_FILE_LOG
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from scripts import ExophaseAPI

LOGGER = configure_logger(__name__, P_PLAYERS_FILE_LOG)


class PlayStationPlayers(ExophaseAPI):
    def __init__(self):
        super().__init__()
        self.leaderboard = self.url + '/psn/leaderboard/page/{page}/'
        self.profiles = 0
    
    @staticmethod
    def _format_country(country_code: Optional[str]) -> Optional[str]:
        # ISO 3166-1 alpha-2
        return pycountry.countries.get(alpha_2=country_code).name

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
            country = player.find('td', class_='flag_inner').find(
                'img').get('src').split('/')[-1].replace('.png', '').upper()
            country = self._format_country(country)
            html_content = self._request(self.url + profile)
            soup = BeautifulSoup(html_content, 'html.parser')
            player = soup.find('section', class_='section-profile-header pb-3').find('div')
            nickname = player.get('data-username')
            playerid = player.get('data-playerid')
            data_players.append([playerid, nickname, country])
            self.profiles += 1
        try:
            # DATABASE_TABLES[2] = 'players'
            insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[2], data_players)
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
    PlayStationPlayers().start()
