from requests.exceptions import JSONDecodeError
from psycopg2 import extensions, Error
from typing import Optional, List, Set
from datetime import datetime
from decouple import config
from time import sleep
import pickle
from utils.database.connector import connect_to_database, insert_data, delete_data
from utils.constants import (STEAM_SCHEMA, DATABASE_TABLES, S_GAMES_FILE_LOG,
                             CACHE_APPIDS, CACHE_ACHIEVEMENTS)
from utils.fetcher import Fetcher, TooManyRequestsError, ForbiddenError
from utils.logger import configure_logger

LOGGER = configure_logger(__name__, S_GAMES_FILE_LOG)


class SteamAchievements(Fetcher):
    def __init__(self):
        super().__init__()
        self.achievements = 'https://api.steampowered.com/ISteamUserStats/GetSchemaForGame/v2/?appid={}&key={}&cc=us'

    @staticmethod
    def _get_appids(connection: extensions.connection,
                    dump_achievements: Set[Optional[int]]) -> Set[Optional[int]]:
        with connection.cursor() as cursor:
            query = """
                SELECT game_id
                FROM steam.games;
            """
            cursor.execute(query)
            appids = [appid[0] for appid in cursor.fetchall()]
            return {appid for appid in appids if appid not in dump_achievements}

    def get_achievements(self, connection: extensions.connection, appid: int,
                         dump_achievements: Set[Optional[int]]):
        all_achievements = []
        url = self.achievements.format(appid, config('API_KEY'))
        try:
            json_content = self.fetch_data(url, 'json')
        except TooManyRequestsError:
            # The Steam Web API restricts data retrieval to 200 requests every 5 minutes
            sleep(305)
            json_content = self.fetch_data(url, 'json')
        except UnboundLocalError as e:
            # Sometimes, Steam enforces a forced connection termination
            LOGGER.warning(e)
            sleep(5)
            json_content = self.fetch_data(url, 'json')
        except ForbiddenError:
            # The exception captures playtest data that lacks a JSON structure
            dump_achievements.add(appid)
            # Updating the achievements dump
            with open('./resources/' + CACHE_ACHIEVEMENTS, 'wb') as file:
                pickle.dump(dump_achievements, file)
            return
        achievements = json_content.get('game', {}).get('availableGameStats', {}).get('achievements', [])
        for achievement in achievements:
            all_achievements.append([
                f"{appid}_{achievement['name']}",
                appid,
                achievement['displayName'],
                achievement.get('description', None)
            ])
        try:
            # DATABASE_TABLES[1] = 'achievements'
            insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[1], all_achievements)
            dump_achievements.add(appid)
        except Error as e:
            LOGGER.warning(e)
        except IndexError:
            # There is no achievement data for the game
            dump_achievements.add(appid)
        # Updating the achievements dump
        with open('./resources/' + CACHE_ACHIEVEMENTS, 'wb') as file:
            pickle.dump(dump_achievements, file)
    
    def start(self):
        with connect_to_database() as connection:
            try:
                with open('./resources/' + CACHE_ACHIEVEMENTS, 'rb') as file:
                    dump_achievements = pickle.load(file)
            except FileNotFoundError:
                dump_achievements = set()
            # Retrieving all appids whose achievements are not present in our database
            appids = self._get_appids(connection, dump_achievements)
            for appid in appids:
                self.get_achievements(connection, appid, dump_achievements)

class SteamGames(Fetcher):
    def __init__(self):
        super().__init__()
        self.applist = 'https://api.steampowered.com/ISteamApps/GetAppList/v0002/?format=json'
        self.appdetails = 'https://store.steampowered.com/api/appdetails?appids={}&cc=us'
    
    @staticmethod
    def _format_date(date: str) -> Optional[str]:
        # There are two date formats that need to be processed
        #        08 Jun, 2021          Jun 08, 2021
        try:
            date_object = datetime.strptime(date, '%d %b, %Y')
        except ValueError:
            try:
                date_object = datetime.strptime(date, '%b %d, %Y')
            except ValueError:
                return None
        return date_object.strftime('%Y-%m-%d')

    @staticmethod
    def _format_language(languages: List[str]) -> Optional[List[str]]:
        formatted_languages = []
        for language in languages:
            # Check if the language string contains a '<' character
            # The string may contain an HTML tag
            # Example: English, Italian\u003Cstrong\u003E*\u003C/strong\u003E, ...
            left_position = language.find('<')
            if left_position != -1:
                formatted_languages.append(language[:left_position])
            else:
                formatted_languages.append(language)
        return formatted_languages if formatted_languages else None

    def get_games(self, connection: extensions.connection, appids: List[int],
                  dump_appids: Set[Optional[int]]):
        for appid in appids:
            url = self.appdetails.format(appid)
            try:
                json_content = self.fetch_data(url, 'json')
            except TooManyRequestsError:
                # The Steam Web API restricts data retrieval to 200 requests every 5 minutes
                sleep(301)
                json_content = self.fetch_data(url, 'json')
            except JSONDecodeError:
                try:
                    delete_data(connection, STEAM_SCHEMA, DATABASE_TABLES[0], 'game_id', [[appid]])
                    dump_appids.add(appid)
                except Error as e:
                    LOGGER.warning(e)
                continue
            except UnboundLocalError as e:
                # Sometimes, Steam enforces a forced connection termination
                LOGGER.warning(e)
                sleep(5)
                json_content = self.fetch_data(url, 'json')
            if json_content[str(appid)]['success']:
                data = json_content[str(appid)]['data']
                coming_soon = data.get('release_date', {}).get('coming_soon', True)
                if data['type'] != 'game':
                    # We do not add games marked as 'coming soon' to the dump,
                    # as the data for these games will be updated later
                    dump_appids.add(appid)
                # Games that have not yet been released,
                # as well as DLCs, Tools, Soundtracks, etc., are not included in the database
                if not coming_soon and data['type'] == 'game':
                    title = data['name']
                    developers = data.get('developers', None)
                    publishers = data.get('publishers', None)
                    genres = [genre['description'] for genre in data.get('genres', [])]
                    if not genres:
                        genres = None
                    if data.get('supported_languages', None):
                        supported_languages = self._format_language(
                            [language.strip() for language in data['supported_languages'].split(',')])
                    else:
                        supported_languages = None
                    release_date = self._format_date(
                        data.get('release_date', {}).get('date', None)) if not coming_soon else None
                    game = [[
                        appid, title, developers, publishers,
                        genres, supported_languages, release_date
                    ]]
                    try:
                        # DATABASE_TABLES[0] = 'games'
                        delete_data(connection, STEAM_SCHEMA, DATABASE_TABLES[0], 'game_id', [[appid]])
                        insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[0], game)
                        dump_appids.add(appid)
                    except Error as e:
                        LOGGER.warning(e)
                        LOGGER.warning(f'The given appid "{appid}"' \
                                       f'was not successfully inserted/deleted into the database')
                else:
                    try:
                        delete_data(connection, STEAM_SCHEMA, DATABASE_TABLES[0], 'game_id', [[appid]])
                    except Error:
                        LOGGER.warning(e)
            else:
                try:
                    delete_data(connection, STEAM_SCHEMA, DATABASE_TABLES[0], 'game_id', [[appid]])
                    dump_appids.add(appid)
                except Error:
                    LOGGER.warning(e)
            # Recording the processed game data into the dump
            # Poor implementation, as Steam can terminate the connection at any moment
            with open('./resources/' + CACHE_APPIDS, 'wb') as file:
                pickle.dump(dump_appids, file)

    def start(self):
        with connect_to_database() as connection:
            json_content = self.fetch_data(self.applist, 'json').get('applist', {}).get('apps', [])
            # Retrieving the cache of the most recent appids data available in postgres
            try:
                with open('./resources/' + CACHE_APPIDS, 'rb') as file:
                    dump_appids = pickle.load(file)
            except FileNotFoundError:
                dump_appids = set()
            appids = []
            # Retrieving a list of new games not present in our database
            for app in json_content:
                if app.get('name', None) and app['appid'] not in dump_appids:
                    appids.append([app['appid'], app['name'], None, None, None, None, None])
            try:
                insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[0], appids)
            except Error as e:
                LOGGER.warning(e)
            except IndexError as e:
                LOGGER.warning(e)
            # This is done through the initial initialization followed by data updates,
            # because the original JSON received from the Steam Web API
            # lacks a clear data order structure
            query = """
                SELECT game_id
                FROM steam.games
                WHERE developers IS NULL AND
                      publishers IS NULL AND
                      genres IS NULL AND
                      supported_languages IS NULL AND
                      release_date IS NULL;
                """
            with connection.cursor() as cursor:
                cursor.execute(query)
                appids = [appid[0] for appid in cursor.fetchall()]
            self.get_games(connection, appids, dump_appids)

def main(process):
    processes = {
        'games': SteamGames,
        'achievements': SteamAchievements
    }
    if process in processes:
        processes[process]().start()
    else:
        raise ValueError(f'The specified process "{process}" is not included in the available options')
