from typing import Optional, Tuple, Any, Dict, List
from concurrent.futures import ThreadPoolExecutor
from psycopg2 import extensions, Error
from bs4 import BeautifulSoup
import pickle
import json
from utils.constants import (PLAYSTATION_SCHEMA, DATABASE_TABLES,
                             P_GAMES_FILE_LOG, CASHE_PLAYSTATIONURLS)
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from scripts import ExophaseAPI

LOGGER = configure_logger(__name__, P_GAMES_FILE_LOG)


class PlayStationGames(ExophaseAPI):
    def __init__(self):
        super().__init__()
        self.games = '/public/archive/platform/psn/page/{page}?q=&sort=added'
        self.added = 0

    def _get_details(self, gameid: int, gametitle: str,
                     platform: str, url: str) -> Tuple[List[Any], List[Any]]:
        html_content = self._request(url)
        soup = BeautifulSoup(html_content, 'html.parser')
        details = [gameid, gametitle, platform] + self.get_details(soup)
        achievements = self.get_achievements(soup, gameid)
        return details, achievements

    def get_games(self, connection: extensions.connection, page: int,
                  dump_playstationurls: Dict[int, Optional[str]]):
        games = json.loads(self._request(
            self.api + self.games.format(page=page))).get('games', {}).get('list', [])
        batch_games, batch_achievements = [], []
        for game in games:
            gameid = game['master_id']
            gametitle = game['title']
            gameurl = game['endpoint_awards']
            platform = game['platforms'][0]['name']
            details, achievements = self._get_details(gameid, gametitle, platform, gameurl)
            batch_games.append(details)
            batch_achievements.extend(achievements)
            dump_playstationurls[gameid] = gameurl
            self.added += 1
        try:
            # DATABASE_TABLES[0] = 'games'
            # DATABASE_TABLES[1] = 'achievements'
            insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[0], batch_games)
            insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[1], batch_achievements)
        except (Error, IndexError) as e:
            LOGGER.warning(e)
            self.added -= len(batch_games)

    def start(self):
        # Retrieve a cache of data pairs in the form of (appid, href)
        try:
            with open('./resources/' + CASHE_PLAYSTATIONURLS, 'rb') as file:
                dump_playstationurls = pickle.load(file)
        except FileNotFoundError:
            dump_playstationurls = {}
        previous_len = len(dump_playstationurls)
        with connect_to_database() as connection:
            try:
                last_page = json.loads(self._request(
                    self.api + self.games.format(page=1))).get('games', {}).get('pages', 0)
            except Exception as e:
                LOGGER.error(e)
            with ThreadPoolExecutor() as executor:
                executor.map(lambda page: self.get_games(connection, page, dump_playstationurls),
                             range(1, last_page + 1))
            LOGGER.info(f'Received "{self.added}" games during execution')
        # Update it for further use in playstation/history.py
        current_len = len(dump_playstationurls)
        with open('./resources/' + CASHE_PLAYSTATIONURLS, 'wb') as file:
            pickle.dump(dump_playstationurls, file)
            LOGGER.info(f'The cache containing gameid-url pairs has been updated. ' \
                        f'It previously had "{previous_len}" values, ' \
                        f'and now it contains "{current_len}" values')

def main():
    PlayStationGames().start()
