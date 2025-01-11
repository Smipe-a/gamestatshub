from typing import Optional, Tuple, Any, Dict, List
from concurrent.futures import ThreadPoolExecutor
from psycopg2 import extensions, Error
from bs4 import BeautifulSoup
from pathlib import Path
import pickle
import json
from utils.constants import (PLAYSTATION_SCHEMA, DATABASE_TABLES,
                             PLAYSTATION_LOGS, CASHE_PLAYSTATIONURLS)
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from scripts import ExophaseAPI

LOGGER = configure_logger(Path(__file__).name, PLAYSTATION_LOGS)


class PlayStationGames(ExophaseAPI):
    def __init__(self, process_games: str, process_achievements: str):
        super().__init__()
        self.process_games = process_games
        self.process_achievements = process_achievements

        self.games = self.api + '/public/archive/platform/psn/page/{page}?q=&sort=added'

        # Number of records added to the 'games' table
        self.added_games = 0
        # Number of records added to the 'achievements' table
        self.added_achievements = 0

    def _get_details(self, gameid: int, gametitle: str,
                     platform: str, gameurl: str) -> Tuple[List[Any], List[Any]]:
        # Called from ExophaseAPI
        html_content = self._request(gameurl)
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Called from ExophaseAPI
        details = [gameid, gametitle, platform] + self.get_details(soup)
        achievements = self.get_achievements(soup, gameid)
        
        return details, achievements

    def get_games(self, connection: extensions.connection, page: int,
                  dump_playstationurls: Dict[int, Optional[str]]):
        json_content = json.loads(self._request(self.games.format(page=page)))
        
        batch_games, batch_achievements = [], []
        games = json_content.get('games', {}).get('list', [])
        for game in games:
            gameid = game['master_id']
            gametitle = game['title']
            gameurl = game['endpoint_awards']
            platform = game['platforms'][0]['name']
            
            details, achievements = self._get_details(gameid, gametitle, platform, gameurl)
            
            batch_games.append(details)
            batch_achievements.extend(achievements)
            
            dump_playstationurls[gameid] = gameurl
        
        try:
            # DATABASE_TABLES[0] = 'games'
            # DATABASE_TABLES[1] = 'achievements'
            insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[0], batch_games)
            self.added_games += len(batch_games)
            insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[1], batch_achievements)
            self.added_achievements += len(batch_achievements)
        except (Error, IndexError) as e:
            LOGGER.warning(e)

    def start(self):
        # Retrieve a cache of data pairs in the form of (appid, href)
        try:
            with open('./resources/' + CASHE_PLAYSTATIONURLS, 'rb') as file:
                dump_playstationurls = pickle.load(file)
            
            if not isinstance(dump_playstationurls, dict) or len(dump_playstationurls) == 0:
                raise FileNotFoundError
        except FileNotFoundError:
            dump_playstationurls = {}
        
        previous_len = len(dump_playstationurls)
        with connect_to_database() as connection:
            try:
                json_content = json.loads(self._request(self.games.format(page=1)))
                last_page = json_content.get('games', {}).get('pages', 0)
            except Exception as e:
                LOGGER.error(e)
            with ThreadPoolExecutor() as executor:
                executor.map(lambda page: self.get_games(connection, page, dump_playstationurls),
                             range(1, last_page + 1))
        
        LOGGER.info(f'Added "{self.added_games}" new data to the table "playstation.{self.process_games}"')
        LOGGER.info(f'Added "{self.added_achievements}" new data to the table "playstation.{self.process_achievements}"')
        
        # Update it for further use in playstation/history.py
        current_len = len(dump_playstationurls)
        with open('./resources/' + CASHE_PLAYSTATIONURLS, 'wb') as file:
            pickle.dump(dump_playstationurls, file)
        
        LOGGER.info(f'The cache containing gameid-url pairs has been updated. ' \
                    f'It previously had "{previous_len}" values, ' \
                    f'and now it contains "{current_len}" values')

def main():
    process_games, process_achievements = 'games', 'achievements'
    LOGGER.info(f'Process started')

    playstation_games = PlayStationGames(process_games, process_achievements)

    try:
        playstation_games.start()
    except (Exception, KeyboardInterrupt) as e:
        if str(e) == '':
            e = 'Forced termination'
        LOGGER.error(f'An unhandled exception occurred with error: {str(e).strip()}')

        LOGGER.info(f'Added "{playstation_games.added_games}" new data to the table "playstation.{process_games}"')
        LOGGER.info(f'Added "{playstation_games.added_achievements}" new data to the table "playstation.{process_achievements}"')
        
        raise Exception(e)
