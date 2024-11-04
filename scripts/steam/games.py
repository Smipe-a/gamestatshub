from concurrent.futures import ThreadPoolExecutor
from typing import Generator, Optional, List
from psycopg2 import extensions, Error
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep
from utils.constants import STEAM_SCHEMA, DATABASE_TABLES, S_GAMES_FILE_LOG
from utils.database.connector import connect_to_database, insert_data
from utils.fetcher import Fetcher, TooManyRequestsError
from utils.logger import configure_logger

LOGGER = configure_logger(__name__, S_GAMES_FILE_LOG)


class SteamAchievements(Fetcher):
    def __init__(self):
        super().__init__()
        self.url = 'https://steamcommunity.com/stats/{}/achievements'
    
    @staticmethod
    def _create_batches(appids: List[int], batch_size: int = 50) -> Generator[List[int], None, None]:
        for i in range(0, len(appids), batch_size):
            yield appids[i:i + batch_size]

    @staticmethod
    def _get_appids(connection: extensions.connection) -> List[int]:
        with connection.cursor() as cursor:
            query = """
                SELECT game_id
                FROM steam.games
                WHERE game_id NOT IN (SELECT game_id FROM steam.achievements)
                ORDER BY game_id;
            """
            cursor.execute(query)
            return [appid[0] for appid in cursor.fetchall()]

    def get_achievements(self, connection: extensions.connection, appids: List[int]):
        all_achievements = []
        for id in appids:
            print(id)
            html_content = self.fetch_data(self.url.format(id))
            soup = BeautifulSoup(html_content, 'html.parser')
            try:
                achievements = soup.find('div', id='mainContents').find_all('div', class_='achieveRow')
            except AttributeError:
                # AttributeError occurs when the game has no achievements
                continue
            for number, achievement in enumerate(achievements, 1):
                # achievement_id is constructed as -> achievement_number + 'g' + game_id
                achievement_id = f'{number}g{id}'
                title = achievement.find('div', class_='achieveTxt').find('h3').text.strip()
                description = achievement.find('div', class_='achieveTxt').find('h5').text.strip()
                if description == '':
                    description = None
                all_achievements.append([achievement_id, id, title, description])
        try:
            # DATABASE_TABLES[1] = 'achievements'
            insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[1], all_achievements)
        except Error as e:
            LOGGER.warning(e)
    
    def start(self):
        with connect_to_database() as connection:
            appids = list(self._create_batches(self._get_appids(connection)))
            with ThreadPoolExecutor() as executor:
                for batch in appids:
                    executor.submit(self.get_achievements, connection, batch)


class SteamGames(Fetcher):
    def __init__(self):
        super().__init__()
        self.applist = 'https://api.steampowered.com/ISteamApps/GetAppList/v0002/?format=json'
        self.appdetails = 'https://store.steampowered.com/api/appdetails?appids='
    
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

    def get_games(self, connection: extensions.connection, appids: List[int]):
        for id in appids:
            try:
                json_content = self.fetch_data(f'{self.appdetails}{id}', 'json')
            except TooManyRequestsError:
                # The Steam Web API restricts data retrieval to 200 requests every 5 minutes
                sleep(305)
                json_content = self.fetch_data(f'{self.appdetails}{id}', 'json')
            if json_content[str(id)]['success']:
                data = json_content[str(id)]['data']
                coming_soon = data.get('release_date', {}).get('coming_soon', True)
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
                        id, title, developers, publishers,
                        genres, supported_languages, release_date
                    ]]
                    try:
                        # DATABASE_TABLES[0] = 'games'
                        insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[0], game)
                    except Error as e:
                        LOGGER.warning(e)
                        LOGGER.warning(f'The given appid "{id}" was not successfully inserted into the database')
    
    def start(self):
        with connect_to_database() as connection:
            try:
                json_content = self.fetch_data(self.applist, 'json')
            except Exception:
                LOGGER.error(f'')
                return
            appids = [app.get('appid') for app in json_content.get('applist', {}).get('apps', [])]
            self.get_games(connection, appids)

def main(process):
    processes = {
        'games': SteamGames,
        'achievements': SteamAchievements
    }
    if process in processes:
        processes[process]().start()
    else:
        raise ValueError(f'The specified process "{process}" is not included in the available options')
