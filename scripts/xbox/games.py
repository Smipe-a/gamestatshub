from concurrent.futures import ThreadPoolExecutor
from psycopg2 import extensions, Error
from typing import Tuple, Any, List
from bs4 import BeautifulSoup
import json
from utils.constants import (XBOX_SCHEMA, DATABASE_TABLES, X_GAMES_FILE_LOG)
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from scripts import ExophaseAPI

LOGGER = configure_logger(__name__, X_GAMES_FILE_LOG)


class XboxGames(ExophaseAPI):
    def __init__(self):
        super().__init__()
        self.games = '/public/archive/platform/xbox/page/{page}?q=&sort=added'
        self.added = 0

    def _get_details(self, gameid: int, gametitle: str, url: str) -> Tuple[List[Any], List[Any]]:
        html_content = self._request(url)
        soup = BeautifulSoup(html_content, 'html.parser')
        details = [gameid, gametitle] + self.get_details(soup)
        achievements = self.get_achievements(soup, gameid)
        return details, achievements

    def get_games(self, connection: extensions.connection, page: int):
        games = json.loads(self._request(
            self.api + self.games.format(page=page))).get('games', {}).get('list', [])
        batch_games, batch_achievements = [], []
        for game in games:
            gameid = game['master_id']
            gametitle = game['title']
            gameurl = game['endpoint_awards']
            details, achievements = self._get_details(gameid, gametitle, gameurl)
            batch_games.append(details)
            batch_achievements.extend(achievements)
            self.added += 1
        try:
            # DATABASE_TABLES[0] = 'games'
            # DATABASE_TABLES[1] = 'achievements'
            insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[0], batch_games)
            insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[1], batch_achievements)
        except (Error, IndexError) as e:
            LOGGER.warning(e)
            self.added -= len(batch_games)

    def start(self):
        with connect_to_database() as connection:
            try:
                last_page = json.loads(self._request(
                    self.api + self.games.format(page=1))).get('games', {}).get('pages', 0)
            except Exception as e:
                LOGGER.error(e)
            with ThreadPoolExecutor() as executor:
                executor.map(lambda page: self.get_games(connection, page),
                             range(1, last_page + 1))
            LOGGER.info(f'Received "{self.added}" games during execution')

def main():
    XboxGames().start()
