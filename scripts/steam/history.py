from typing import Generator, Optional, Union, List, Set
from concurrent.futures import ThreadPoolExecutor
from psycopg2 import Error, extensions
from datetime import datetime
from decouple import config
from pathlib import Path
from utils.database.connector import connect_to_database, insert_data, delete_data
from utils.constants import STEAM_SCHEMA, DATABASE_TABLES, STEAM_LOGS
from utils.fetcher import Fetcher, ForbiddenError
from utils.logger import configure_logger

LOGGER = configure_logger(Path(__file__).name, STEAM_LOGS)


class SteamHistory(Fetcher):
    def __init__(self, process_history: str, process_library: str):
        super().__init__()
        self.process_history = process_history
        self.process_library = process_library

        self.steam = 'https://api.steampowered.com/'
        self.owned_games = self.steam + 'IPlayerService/GetOwnedGames/v0001/?key={api_key}&steamid={steamid}&format=json'
        self.achievements = self.steam + 'ISteamUserStats/GetPlayerAchievements/v0001/?appid={appid}&key={api_key}&steamid={steamid}'
        self.new_achievements = self.steam + 'ISteamUserStats/GetSchemaForGame/v2/?appid={appid}&key={api_key}&cc=us'

        # Number of records added to the 'history' table
        self.added_history = 0
         # Number of records added to the 'library' table
        self.added_library = 0
    
    @staticmethod
    def _get_steamids(connection: extensions.connection) -> List[str]:
        # Retrieve data for players with no game history
        with connection.cursor() as cursor:
            # ORDER BY RANDOM() - for a representative sample
            query = """
                SELECT player_id
                FROM steam.players
                WHERE player_id NOT IN (SELECT player_id FROM steam.purchased_games)
                ORDER BY RANDOM();
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
                            connection: extensions.connection,
                            steamid: str,
                            owned_games: List[Optional[int]],
                            library: List[Optional[int]],
                            game_achievements: List[Optional[int]],
                            appids: Set[int],
                            db_achievements: Set[str]):
        for game in owned_games:
            appid = game['appid']
            # If the purchased game by the user exists in our database,
            # retrieve their game statistics
            if appid in appids:
                try:
                    achievements_steamid = self.achievements.format(appid=appid,
                                                                    api_key=config('API_KEY'),
                                                                    steamid=steamid)
                    json_content = self.fetch_data(achievements_steamid, 'json')
                except ForbiddenError:
                    # The player's profile is not private, but the game statistics are hidden
                    library.clear()
                    return
                
                achievements = json_content.get('playerstats', {}).get('achievements', [])
                for achievement in achievements:
                    check = True
                    achievement_id = f"{appid}_{achievement['apiname']}"
                    
                    # Select only the data where the player has earned an achievement
                    if achievement['achieved']:
                        # If the user has earned an achievement that is not in the database, 
                        # update the game's achievement data (related to newly added achievements)
                        if achievement_id not in db_achievements:
                            new_achievements_url = self.new_achievements(appid=appid, api_key=config('API_KEY'))
                            json_content = self.fetch_data(new_achievements_url, 'json')
                            
                            new_achievements = []
                            for new_achievement in json_content.get('game', {}).get('achievements', []):
                                new_achievements.append([
                                    f"{appid}_{new_achievement['name']}",
                                    appid,
                                    new_achievement['displayName'],
                                    new_achievement.get('description', None)
                                ])
                            
                            try:
                                # DATABASE_TABLES[1] = 'achievements'
                                insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[1], new_achievements)
                            except Error as e:
                                LOGGER.error(f'Failed to add new achievement data. Error: {e}')
                                check = False
                        
                        # If the data aligns with the rest of the table data, 
                        # update the achievement list
                        if check:    
                            game_achievements.append([
                                steamid,
                                achievement_id,
                                self._format_timestamp(achievement['unlocktime'])
                            ])
            library.append(appid)

    def get_achievement_history(self, connection: extensions.connection,
                                steamid: str, appids: Set[int],
                                achievementids: Set[str]):
        try:
            owned_games_url = self.owned_games.format(api_key=config('API_KEY'), steamid=steamid)
            json_content = self.fetch_data(owned_games_url, 'json')
        except ForbiddenError:
            # Private profiles
            # DATABASE_TABLE[4] = 'purchased_games'
            insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[4], [[steamid, None]])
            return
        
        owned_games = json_content.get('response', {}).get('games', [])
        library, game_achievements = [], []

        with ThreadPoolExecutor() as executor:
            executor.map(lambda batches: self.get_data_from_steam(
                connection, steamid, batches, library,
                game_achievements, appids, achievementids
            ), self._create_batches(owned_games))
        
        if not library:
            library = None
        
        # DATABASE_TABLE[4] = 'purchased_games'
        insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[4], [[steamid, library]])
        self.added_library += 1
        
        try:
            # DATABASE_TABLES[3] = 'history'
            insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[3], game_achievements)
            self.added_history += len(game_achievements)
        except IndexError:
            # We reach this point if the player has games,
            # but achievements in all of them are either not earned or missing
            pass
        except Error as e:
            # If the data was not inserted due to an error,
            # remove the player_id from the processed list
            delete_data(connection, STEAM_SCHEMA, DATABASE_TABLES[4], 'player_id', [[steamid]])
            self.added_library -= 1
            LOGGER.error(e)

    def start(self):
        with connect_to_database() as connection:
            steamids = self._get_steamids(connection)
            appids = self._get_appids_achievements(connection, 'games')
            achievementids = self._get_appids_achievements(connection, 'achievements')
            
            for steamid in steamids:
                self.get_achievement_history(connection, steamid, appids, achievementids)
        
        LOGGER.info(f'Added "{self.added_history}" new data to the table "steam.{self.process_history}"')
        LOGGER.info(f'Added "{self.added_library}" new data to the table "steam.{self.process_library}"')

def main():
    process_history, process_library = 'history', 'library'
    LOGGER.info(f'Process started')

    try:
        steam_history = SteamHistory(process_history, process_library)
        steam_history.start()
    except (Exception, KeyboardInterrupt) as e:
        if str(e) == '':
            e = 'Forced termination'
        LOGGER.error(f'An unhandled exception occurred with error: {str(e).strip()}')
        
        LOGGER.info(f'Added "{steam_history.added_history}" new data to the table "steam.{process_history}"')
        LOGGER.info(f'Added "{steam_history.added_library}" new data to the table "steam.{process_library}"')
        
        raise Exception(e)

main()