from typing import Optional, Union, Dict, List, Set
from concurrent.futures import ThreadPoolExecutor
from psycopg2 import extensions, Error
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
import pickle
import json
from utils.constants import (PLAYSTATION_SCHEMA, DATABASE_TABLES,
                             PLAYSTATION_LOGS, CASHE_PLAYSTATIONURLS)
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from scripts import ExophaseAPI

LOGGER = configure_logger(Path(__file__).name, PLAYSTATION_LOGS)


class PlayStationHistory(ExophaseAPI):
    def __init__(self, process_purchased: str, process_history: str):
        super().__init__()
        self.process_purchased = process_purchased
        self.process_history = process_history

        self.purchased = self.api + '/public/player/{playerid}/games?' \
            + 'page={page}&environment=psn&sort=1'
        self.history = self.api + '/public/player/{playerid}/game/{gameid}/earned'

        # Number of records added to the 'purchased_games' table
        self.added_purchased = 0
        # Number of records added to the 'history' table
        self.added_history = 0
    
    @staticmethod
    def _get_appids_achievements(connection: extensions.connection,
                                 condition: str) -> Set[Union[int, str]]:
        try:
            with connection.cursor() as cursor:
                queries = {
                    'games': """
                        SELECT gameid
                        FROM playstation.games;
                    """,
                    'achievements': """
                        SELECT achievementid
                        FROM playstation.achievements;
                    """
                }
                cursor.execute(queries[condition])
                return {value[0] for value in cursor.fetchall()}
        except Exception as e:
            LOGGER.error(f'Failed to retrieve the game/achievement list. Error: {e}')
            return set()

    @staticmethod
    def _get_playerid(connection: extensions.connection) -> List[Optional[int]]:
        try:
            with connection.cursor() as cursor:
                # ORDER BY RANDOM() - for a representative sample
                query = """
                    SELECT playerid
                    FROM playstation.players
                    WHERE playerid NOT IN (SELECT playerid FROM playstation.purchased_games)
                    ORDER BY RANDOM();
                """
                cursor.execute(query)
                return [playerid[0] for playerid in cursor.fetchall()]
        except Exception as e:
            LOGGER.error(f'Failed to retrieve the user list. Error: {e}')
            return []

    @staticmethod
    def _format_timestamp(timestamp: int) -> str:
        # UNIX-timestamp
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

    def get_history(self, connection: extensions.connection,
                    playerid: int, gameid: int,
                    history: List[Optional[Union[int, str]]],
                    achievementids: Set[Union[int, str]],
                    dump_playstationurls: Dict[int, str]):
        json_content = json.loads(
            self._request(self.history.format(playerid=playerid, gameid=gameid)))
        
        for achievement in json_content.get('list', []):
            check = True
            achievementid = f'{gameid}_{achievement["awardid"]}'
            
            # At a certain point, the data in the database may not contain
            # any newly added achievements. This check helps to update our data
            if achievementid not in achievementids:
                gameurl = dump_playstationurls[gameid]
                
                html_content = self._request(gameurl)
                soup = BeautifulSoup(html_content, 'html.parser')
                
                new_achievements = self.get_achievements(soup, gameid)
                
                try:
                    # DATABASE_TABLES[1] = 'achievements'
                    insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[1], new_achievements)
                except Error as e:
                    LOGGER.error(f'The achievement data update for the game "{gameid}" was not successful. ' \
                                 f'Error: {e}')
                    check = False
            
            if check:
                history.append([playerid,
                                achievementid,
                                self._format_timestamp(achievement['timestamp'])])
    
    def get_purchased(self, connection: extensions.connection,
                      playerid: int, gameids: Set[Optional[int]]) -> List[Optional[int]]:
        purchased, page = [], 1
        
        json_content = json.loads(
            self._request(self.purchased.format(playerid=playerid, page=page)))
       
        while json_content.get('success', False):
            for game in json_content.get('games', []):
                gameid = game['master_id']
                title = game['meta']['title']
                platform = game['meta']['platforms'][0]['name']
                
                try:
                    url = game['meta']['endpoint_awards'].replace(f'/achievements/#{playerid}', '')
                except AttributeError:
                    # Data for the game is not available on the source website
                    LOGGER.warning(f'Data for the game "{gameid}" with the title' \
                                   f'"{title}" is not available on the source website')
                    continue
                
                if gameid not in gameids:
                    # Adding information about a game that is not in our database
                    # but was found in the player's profile JSON data
                    html_content = self._request(url)
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    details = [gameid, title, platform] + self.get_details(soup)
                    achievements = self.get_achievements(soup, gameid)
                    
                    try:
                        # DATABASE_TABLES[0] = 'games'
                        # DATABASE_TABLES[1] = 'achievements'
                        insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[0], [details])
                        insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[1], achievements)
                    except (Error, IndexError) as e:
                        LOGGER.warning(e)
                
                purchased.append(gameid)
                # Overwriting gameids with updated new games
                # If the next player encounters it again, we no longer consider it as new
                gameids.add(gameid)
            
            # Iterating to the next page
            page += 1
            json_content = json.loads(
                self._request(self.purchased.format(playerid=playerid, page=page)))
        return purchased

    def start(self):
        with connect_to_database() as connection:
            gameids = self._get_appids_achievements(connection, 'games')
            achievementids = self._get_appids_achievements(connection, 'achievements')
            
            try:
                with open('./resources/' + CASHE_PLAYSTATIONURLS, 'rb') as file:
                    dump_playstationurls = pickle.load(file)

                    if not isinstance(dump_playstationurls, dict) or len(dump_playstationurls) == 0:
                        raise FileNotFoundError
            except FileNotFoundError:
                dump_playstationurls = {}
            
            for playerid in self._get_playerid(connection):
                history = []
                
                purchased = self.get_purchased(connection, playerid, gameids)
                with ThreadPoolExecutor() as executor:
                    executor.map(lambda gameid: self.get_history(
                        connection, playerid, gameid, history, achievementids, dump_playstationurls), purchased)
                
                if not purchased:
                    purchased = None
                
                try:
                    # DATABASE_TABLES[3] = 'history'
                    insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[3], history)
                    self.added_history += len(history)
                    # DATABASE_TABLES[4] = 'purchased_games'
                    insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[4], [[playerid, purchased]])
                    self.added_purchased += 1
                except (IndexError, Error) as e:
                    LOGGER.error(e)
        
        LOGGER.info(f'Added "{self.added_purchased}" new data to the table "playstation.{self.process_purchased}"')
        LOGGER.info(f'Added "{self.added_history}" new data to the table "playstation.{self.process_history}"')

def main():
    process_purchased, process_history = 'purchased_games', 'history'

    playstation_history = PlayStationHistory(process_purchased, process_history)
    
    try:
        playstation_history.start()
    except (Exception, KeyboardInterrupt) as e:
        if str(e) == '':
            e = 'Forced termination'
        LOGGER.error(f'An unhandled exception occurred with error: {str(e).strip()}')
        
        LOGGER.info(f'Added "{playstation_history.added_purchased}" new data to the table "playstation.{process_purchased}"')
        LOGGER.info(f'Added "{playstation_history.added_history}" new data to the table "playstation.{process_history}"')
        
        raise Exception(e)
