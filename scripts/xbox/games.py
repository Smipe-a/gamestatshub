from typing import Optional, Tuple, Any, Dict, List
from concurrent.futures import ThreadPoolExecutor
from psycopg2 import extensions, Error
from bs4 import BeautifulSoup
from pathlib import Path
import pickle
import json
from utils.constants import (XBOX_SCHEMA, DATABASE_TABLES,
                             XBOX_LOGS, CASHE_XBOXURLS)
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from scripts import ExophaseAPI

LOGGER = configure_logger(Path(__file__).name, XBOX_LOGS)


class XboxGames(ExophaseAPI):
    def __init__(self, process_games: str, process_achievements: str):
        super().__init__()
        self.process_games = process_games
        self.process_achievements = process_achievements

        self.games = self.api + '/public/archive/platform/xbox/page/{page}?q=&sort=added'
        
        # Number of records added to the 'games' table
        self.added_games = 0
        # Number of records added to the 'achievements' table
        self.added_achievements = 0

    def _get_details(self, gameid: int, gametitle: str,
                     url: str) -> Tuple[List[Any], List[Any]]:
        # Called from ExophaseAPI
        html_content = self._request(url)
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Called from ExophaseAPI
        details = [gameid, gametitle] + self.get_details(soup)
        achievements = self.get_achievements(soup, gameid)
        
        return details, achievements

    def get_games(self, connection: extensions.connection, page: int,
                  dump_xboxurls: Dict[int, Optional[str]]):
        json_content = json.loads(self._request(self.games.format(page=page)))
        
        batch_games, batch_achievements = [], []
        games = json_content.get('games', {}).get('list', [])
        for game in games:
            gameid = game['master_id']
            gametitle = game['title']
            gameurl = game['endpoint_awards']
            
            details, achievements = self._get_details(gameid, gametitle, gameurl)
            
            batch_games.append(details)
            batch_achievements.extend(achievements)
            
            dump_xboxurls[gameid] = gameurl
        try:
            # DATABASE_TABLES[0] = 'games'
            # DATABASE_TABLES[1] = 'achievements'
            insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[0], batch_games)
            self.added_games += len(batch_games)
            insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[1], batch_achievements)
            self.added_achievements += len(batch_achievements)
        except (Error, IndexError) as e:
            LOGGER.error(e)

    def start(self):
        # Retrieve a cache of data pairs in the form of (appid, href)
        try:
            with open('./resources/' + CASHE_XBOXURLS, 'rb') as file:
                dump_xboxurls = pickle.load(file)
            
            if not isinstance(dump_xboxurls, dict) or len(dump_xboxurls) == 0:
                raise FileNotFoundError
        except FileNotFoundError:
            dump_xboxurls = {}
        
        previous_len = len(dump_xboxurls)
        with connect_to_database() as connection:
            try:
                json_content = json.loads(self._request(self.games.format(page=1)))
                last_page = json_content.get('games', {}).get('pages', 0)
            except Exception as e:
                LOGGER.error(e)
            with ThreadPoolExecutor() as executor:
                executor.map(lambda page: self.get_games(connection, page, dump_xboxurls),
                             range(1, last_page + 1))
        
        LOGGER.info(f'Added "{self.added_games}" new data to the table "xbox.{self.process_games}"')
        LOGGER.info(f'Added "{self.added_achievements}" new data to the table "xbox.{self.process_achievements}"')
        
        # Update it for further use in xbox/history.py
        current_len = len(dump_xboxurls)
        with open('./resources/' + CASHE_XBOXURLS, 'wb') as file:
            pickle.dump(dump_xboxurls, file)
        
        LOGGER.info(f'The cache containing gameid-url pairs has been updated. ' \
                    f'It previously had "{previous_len}" values, ' \
                    f'and now it contains "{current_len}" values')

def main():
    process_games, process_achievements = 'games', 'achievements'
    LOGGER.info(f'Process started')

    xbox_games = XboxGames(process_games, process_achievements)

    try:
        xbox_games.start()
    except (Exception, KeyboardInterrupt) as e:
        if str(e) == '':
            e = 'Forced termination'
        LOGGER.error(f'An unhandled exception occurred with error: {str(e).strip()}')

        LOGGER.info(f'Added "{xbox_games.added_games}" new data to the table "xbox.{process_games}"')
        LOGGER.info(f'Added "{xbox_games.added_achievements}" new data to the table "xbox.{process_achievements}"')
        
        raise Exception(e)
