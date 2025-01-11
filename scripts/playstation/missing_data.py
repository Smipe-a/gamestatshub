from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Any
from psycopg2 import extensions
from bs4 import BeautifulSoup
from pathlib import Path
from utils.constants import MATCH_MISSING_DATA, PLAYSTATION_LOGS
from utils.database.connector import connect_to_database
from utils.logger import configure_logger
from scripts import ExophaseAPI

LOGGER = configure_logger(Path(__file__).name, PLAYSTATION_LOGS)


class PlayStationUpdateData(ExophaseAPI):
    def __init__(self, process: str):
        super().__init__()
        self.process = process

        self.truetrophies = 'https://www.truetrophies.com'
        self.search = '/searchresults.aspx?search={encoded_title}'

        # Number of records updated to the 'games' table
        self.updated = 0
    
    @staticmethod
    def _get_missing(connection: extensions.connection) -> List[Optional[List[Any]]]:
        # Update the game data for those that have Null in any of the specified columns
        try:
            with connection.cursor() as cursor:
                query = """
                    SELECT *
                    FROM playstation.games
                    WHERE developers IS NULL OR
                          publishers IS NULL OR
                          genres IS NULL OR
                          release_date IS NULL
                    ORDER BY gameid DESC;
                """
                cursor.execute(query)
                return [[data for data in game] for game in cursor.fetchall()]
        except Exception as e:
            LOGGER.error('Failed to retrieve data from the database. ' \
                         'Error: %s', str(e).strip())
            return []

    @staticmethod
    def _update_data(connection: extensions.connection, app: List[Any]):
        try:
            with connection.cursor() as cursor:
                query = """
                    UPDATE playstation.games
                    SET developers = %s, publishers = %s,
                        genres = %s, release_date = %s
                    WHERE gameid = %s;
                """
                cursor.execute(query, (
                    app[3],  # developers
                    app[4],  # publishers
                    app[5],  # genres
                    app[7],  # release_date
                    app[0]   # gameid
                ))
                connection.commit()
        except Exception as e:
            LOGGER.warning('Failed to update the missing data. Error: %s', str(e).strip())

    def get_data(self, connection: extensions.connection, app: List[Any]):
        # Encode the game title for the URL query,
        # ensuring special characters are properly escaped (app[1] - title)
        encoded_title = self._construct_query(app[1])
        
        html_content = self.fetch_data(
            self.truetrophies + self.search.format(encoded_title=encoded_title))
        soup = BeautifulSoup(html_content, 'html.parser')
        
        try:
            games = soup.find('table', class_='maintable leaderboard').find_all('tr')[1:]
            
            # Create pairs of candidates (title, href)
            candidates = {}
            for game in games:
                tag_a = game.find('td', class_='gamerwide').find('a')
                candidate_href = self.truetrophies + tag_a.get('href')
                candidate_title = tag_a.text.strip()
                candidates[candidate_title] = candidate_href
            
            # Using Levenshtein distance to get the best matching string
            best_match = self._find_best_match(app[1], candidates.keys(), MATCH_MISSING_DATA)
            if best_match:
                html_content = self.fetch_data(candidates[best_match])
                soup_2 = BeautifulSoup(html_content, 'html.parser')
                data = self.get_details(soup_2)
            else:
                # If the best matching game title for the target is not found,
                # leave the values as None
                data = [None, None, None, None, None]
        except AttributeError:
            # Check if the page with game data is found immediately
            try:
                data = self.get_details(soup)
            except:
                # If we enter this exception, it means no candidate was found
                data = [None, None, None, None, None]
        
        # Poor construction for readability
        # app[2] - developers; app[3] - publishers; app[4] - genres; app[6] - release_date
        # data - [developers, publishers, genres, supported_languages, release]
        if app[3] is None:
            app[3] = data[0]
        if app[4] is None:
            app[4] = data[1]
        if app[5] is None:
            app[5] = data[2]
        if app[7] is None:
            app[7] = data[4]
        
        self._update_data(connection, app)
        self.updated += 1
            
    def start(self):
        with connect_to_database() as connection:
            with ThreadPoolExecutor() as executor:
                executor.map(lambda app: self.get_data(connection, app),
                    self._get_missing(connection))
        
        LOGGER.info(f'Updated "{self.updated}" data to the table "playstation.{self.process}"')

def main():
    process = 'games'
    LOGGER.info(f'Process started')

    playstation_update_data = PlayStationUpdateData(process)
    
    try:
        playstation_update_data.start()
    except (Exception, KeyboardInterrupt) as e:
        if str(e) == '':
            e = 'Forced termination'
        LOGGER.error(f'An unhandled exception occurred with error: {str(e).strip()}')
        LOGGER.info(f'Updated "{playstation_update_data.updated}" data to the table "playstation.{process}"')
        
        raise Exception(e)
