from typing import Generator, Optional, List, Set
from concurrent.futures import ThreadPoolExecutor
from psycopg2 import Error, extensions
from datetime import datetime
from decouple import config
from utils.constants import STEAM_SCHEMA, DATABASE_TABLES, S_ACHIEVEMENTS_HISTORY_FILE_LOG
from utils.database.connector import connect_to_database, insert_data, delete_data
from utils.logger import configure_logger
from utils.fetcher import Fetcher

LOGGER = configure_logger(__name__, S_ACHIEVEMENTS_HISTORY_FILE_LOG)


class SteamAchievementsHistory(Fetcher):
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
    def _get_appids_achievements(connection: extensions.connection) -> Set[int]:
        with connection.cursor() as cursor:
            query = """
                SELECT DISTINCT game_id
                FROM steam.achievements;
            """
            cursor.execute(query)
            return {appid[0] for appid in cursor.fetchall()}

    @staticmethod
    def _format_timestamp(timestamp: int) -> str:
        # UNIX-timestamp
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def _create_batches(appids: List[int], batch_size: int = 50) -> Generator[List[int], None, None]:
        for i in range(0, len(appids), batch_size):
            yield appids[i:i + batch_size]

    def get_data_from_steam(self, steamid: str,
                         owned_games: List[Optional[int]],
                         library: List[Optional[int]],
                         game_achievements: List[Optional[int]],
                         appids: Set[int]):
        for game in owned_games:
            appid = game['appid']
            if appid in appids:
                json_content = self.fetch_data(self.achievements.format(appid, config('API_KEY'), steamid), 'json')
                achievements = json_content.get('playerstats', {}).get('achievements', [])
                for achievement in achievements:
                    if achievement['achieved']:
                        game_achievements.append([
                            steamid,
                            f"{appid}_{achievement['apiname']}",
                            self._format_timestamp(achievement['unlocktime'])
                        ])
            library.append(appid)

    def get_achievement_history(self, connection: extensions.connection, steamid: str, appids: Set[int]):
        json_content = self.fetch_data(self.owned_games.format(config('API_KEY'), steamid), 'json')
        owned_games = json_content.get('response', {}).get('games', [])
        library, game_achievements = [], []
        with ThreadPoolExecutor() as executor:
            executor.map(lambda batches: self.get_data_from_steam(steamid, batches, library,
                game_achievements, appids), self._create_batches(owned_games))
        if not library:
            library = None
        # DATABASE_TABLE[4] = 'purchased_games'
        insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[4], [[steamid, library]])
        try:
            # DATABASE_TABLES[3] = 'achievements_history'
            insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[3], game_achievements)
        except IndexError:
            # We reach this point if the player has games,
            # but achievements in all of them are either not earned or missing
            pass
        except Error as e:
            # If the data was not inserted due to an error,
            # remove the player_id from the processed list
            delete_data(connection, STEAM_SCHEMA, DATABASE_TABLES[4], [[steamid]])
            LOGGER.warning(e)

    def start(self):
        with connect_to_database() as connection:
            steamids = self._get_players_id(connection)
            appids = self._get_appids_achievements(connection)
            for steamid in steamids:
                self.get_achievement_history(connection, steamid, appids)

def main():
    SteamAchievementsHistory().start()
