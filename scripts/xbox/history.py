from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Union, List, Set
from psycopg2 import extensions, Error
from datetime import datetime
import json
from utils.constants import XBOX_SCHEMA, DATABASE_TABLES, X_HISTORY_FILE_LOG
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from scripts.xbox.games import XboxGames
from scripts.xbox import Xbox

LOGGER = configure_logger(__name__, X_HISTORY_FILE_LOG)


class XboxHistory(Xbox):
    def __init__(self):
        super().__init__()
        self.purchased = self.api + '/public/player/{playerid}/games?' \
            + 'page={page}&environment=xbox&sort=1'
        self.history = self.api + '/public/player/{playerid}/game/{gameid}/earned'
    
    @staticmethod
    def _get_gameid(connection: extensions.connection) -> Set[Optional[int]]:
        try:
            with connection.cursor() as cursor:
                query = """
                    SELECT gameid
                    FROM xbox.games;
                """
                cursor.execute(query)
                return {gameid[0] for gameid in cursor.fetchall()}
        except Exception as e:
            LOGGER.error(f'Failed to retrieve the game list. Error: {e}')
            return set()

    @staticmethod
    def _get_playerid(connection: extensions.connection) -> List[Optional[int]]:
        try:
            with connection.cursor() as cursor:
                query = """
                    SELECT playerid
                    FROM xbox.players
                    WHERE playerid NOT IN (SELECT playerid FROM xbox.purchased_games);
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

    def get_history(self, playerid: int, gameid: int,
                    history: List[Optional[Union[int, str]]], ):
        json_content = json.loads(
            self._request(self.history.format(playerid=playerid, gameid=gameid)))
        for achievement in json_content.get('list', []):
            history.append([playerid,
                            f"{gameid}_{achievement['awardid']}",
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
                    details, achievements = XboxGames().get_details(gameid, title, url)
                    try:
                        # DATABASE_TABLES[0] = 'games'
                        # DATABASE_TABLES[1] = 'achievements'
                        insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[0], [details])
                        insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[1], achievements)
                    except (Error, IndexError) as e:
                        LOGGER.warning(e)
                purchased.append(gameid)
                gameids.add(gameid)
            page += 1
            json_content = json.loads(
                self._request(self.purchased.format(playerid=playerid, page=page)))
        return purchased

    def start(self):
        with connect_to_database() as connection:
            gameids = self._get_gameid(connection)
            for playerid in self._get_playerid(connection):
                purchased = self.get_purchased(connection, playerid, gameids)
                history = []
                with ThreadPoolExecutor() as executor:
                    executor.map(lambda gameid: self.get_history(playerid, gameid, history), purchased)
                if not purchased:
                    purchased = None
                try:
                    # DATABASE_TABLES[3] = 'history'
                    insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[3], history)
                    # DATABASE_TABLES[4] = 'purchased_games'
                    insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[4], [[playerid, purchased]])
                except (IndexError, Error) as e:
                    LOGGER.warning(e)

def main():
    XboxHistory().start()
    