from typing import Generator, Optional, Union, List, Set
from concurrent.futures import ThreadPoolExecutor
from psycopg2 import Error, extensions
from datetime import datetime
from decouple import config
from utils.database.connector import connect_to_database, insert_data, delete_data
from utils.constants import STEAM_SCHEMA, DATABASE_TABLES, S_HISTORY_FILE_LOG
from utils.fetcher import Fetcher, ForbiddenError
from utils.logger import configure_logger

LOGGER = configure_logger(__name__, S_HISTORY_FILE_LOG)


class SteamHistory(Fetcher):
    def __init__(self):
        super().__init__()
        self.owned_games = 'https://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={}&steamid={}&format=json'
        self.achievements = 'https://api.steampowered.com/ISteamUserStats/GetPlayerAchievements/v0001/?appid={}&key={}&steamid={}'
    
    @staticmethod
    def _get_players_id(connection: extensions.connection) -> List[str]:
        # Retrieve data for players with no game history
        with connection.cursor() as cursor:
            query = """
                SELECT player_id
                FROM steam.players
                WHERE player_id NOT IN (SELECT player_id FROM steam.purchased_games);
            """
            cursor.execute(query)
            return [steamid[0] for steamid in cursor.fetchall()]
    
    @staticmethod
    def _get_appids_achievements(connection: extensions.connection,
                                 condition: str) -> Set[Union[int, str]]:
        with connection.cursor() as cursor:
            queries = {
                'games': """
                    SELECT DISTINCT game_id
                    FROM steam.achievements;
                """,
                'achievements': """
                    SELECT achievement_id
                    FROM steam.achievements;
                """
            }
            cursor.execute(queries[condition])
            return {value[0] for value in cursor.fetchall()}

    @staticmethod
    def _format_timestamp(timestamp: int) -> str:
        # UNIX-timestamp
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def _create_batches(appids: List[int], batch_size: int = 50) -> Generator[List[int], None, None]:
        for i in range(0, len(appids), batch_size):
            yield appids[i:i + batch_size]

    def get_data_from_steam(self,
                            steamid: str,
                            owned_games: List[Optional[int]],
                            library: List[Optional[int]],
                            game_achievements: List[Optional[int]],
                            appids: Set[int],
                            db_achievements: Set[str]):
        for game in owned_games:
            appid = game['appid']
            if appid in appids:
                try:
                    json_content = self.fetch_data(
                        self.achievements.format(appid, config('API_KEY'), steamid), 'json')
                except ForbiddenError:
                    # The player's profile is not private, but the game statistics are hidden
                    library.clear()
                    return
                achievements = json_content.get('playerstats', {}).get('achievements', [])
                for achievement in achievements:
                    achievement_id = f"{appid}_{achievement['apiname']}"
                    if achievement['achieved'] and achievement_id in db_achievements:
                        game_achievements.append([
                            steamid,
                            achievement_id,
                            self._format_timestamp(achievement['unlocktime'])
                        ])
            library.append(appid)

    def get_achievement_history(self, connection: extensions.connection,
                                steamid: str, appids: Set[int], achievementids: Set[str]):
        try:
            json_content = self.fetch_data(self.owned_games.format(config('API_KEY'), steamid), 'json')
        except ForbiddenError:
            # Private profiles
            # DATABASE_TABLE[4] = 'purchased_games'
            insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[4], [[steamid, None]])
            return
        owned_games = json_content.get('response', {}).get('games', [])
        library, game_achievements = [], []
        with ThreadPoolExecutor() as executor:
            executor.map(lambda batches: self.get_data_from_steam(
                steamid, batches, library, game_achievements, appids, achievementids
            ), self._create_batches(owned_games))
        if not library:
            library = None
        # DATABASE_TABLE[4] = 'purchased_games'
        insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[4], [[steamid, library]])
        try:
            # DATABASE_TABLES[3] = 'history'
            insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[3], game_achievements)
        except IndexError:
            # We reach this point if the player has games,
            # but achievements in all of them are either not earned or missing
            pass
        except Error as e:
            # If the data was not inserted due to an error,
            # remove the player_id from the processed list
            delete_data(connection, STEAM_SCHEMA, DATABASE_TABLES[4], 'player_id', [[steamid]])
            LOGGER.warning(e)

    def start(self):
        with connect_to_database() as connection:
            steamids = self._get_players_id(connection)
            appids = self._get_appids_achievements(connection, 'games')
            achievementids = self._get_appids_achievements(connection, 'achievements')
            for steamid in steamids:
                self.get_achievement_history(connection, steamid, appids, achievementids)

def main():
    SteamHistory().start()
