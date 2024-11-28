from concurrent.futures import ThreadPoolExecutor
from psycopg2 import extensions, Error
from typing import Tuple, Any, List
from datetime import datetime
from bs4 import BeautifulSoup
import json
from utils.constants import (XBOX_SCHEMA, DATABASE_TABLES, X_GAMES_FILE_LOG)
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from scripts.xbox import Xbox

LOGGER = configure_logger(__name__, X_GAMES_FILE_LOG)


class XboxGames(Xbox):
    def __init__(self):
        super().__init__()
        self.games = 'https://api.exophase.com/public/archive/platform/xbox/page/{page}?q=&sort=added'
        self.added = 0
    
    @staticmethod
    def _format_date(date: str) -> str:
        return datetime.strptime(date, "%B %d, %Y")

    def get_details(self, gameid: int, gametitle: str, url: str) -> Tuple[List[Any], List[Any]]:
        developers, publishers, genres = None, None, None
        supported_languages, release = None, None
        html_content = self._request(url)
        soup = BeautifulSoup(html_content, 'html.parser')
        try:
            attributes = soup.find('dl', class_='details').find_all('dt')
        except AttributeError:
            attributes = []
        for attribute in attributes:
            info = attribute.find_next('dd')
            if attribute.text == 'Developer:':
                developers = [developer.text.strip() for developer in info.find_all('a')]
            elif attribute.text == 'Publisher:':
                publishers = [publisher.text.strip() for publisher in info.find_all('a')]
            elif attribute.text == 'Genre:':
                genres = [genre.text.strip() for genre in info.find_all('a')]
            elif attribute.text == 'Languages:':
                supported_languages = [language.text.strip() for language in info.find_all('a')]
            elif attribute.text == 'Release Date:':
                release = self._format_date(info.text.strip())
        details = [gameid, gametitle, developers, publishers, genres, supported_languages, release]
        # ----------------- Block of achievements -----------------
        achievements = []
        table = soup.find('div', id='awards').find_all('li')
        for achievement in table:
            achievementid = f"{gameid}_{achievement.get('id')}"
            title = achievement.find('div',
                class_='text-medium award-title hidden-toggle fw-bolder').find('a').text.strip()
            description = achievement.find('div',
                class_='award-description hidden-toggle').find('p').text.strip()
            try:
                points = achievement.find('div',
                    class_='col-12 col-lg mt-3 mt-lg-0 award-points text-center').find('span').text.strip()
            except AttributeError:
                # Points for achievement acquisition are missing
                points = None
            achievements.append([achievementid, gameid, title, description, points])
        return details, achievements

    def get_games(self, connection: extensions.connection, page: int):
        games = json.loads(self._request(self.games.format(page=page))).get('games', {}).get('list', [])
        batch_games, batch_achievements = [], []
        for game in games:
            gameid = game['master_id']
            gametitle = game['title']
            gameurl = game['endpoint_awards']
            details, achievements = self.get_details(gameid, gametitle, gameurl)
            if details:
                batch_games.append(details)
                self.added += 1
                if achievements:
                    batch_achievements.extend(achievements)
        try:
            # DATABASE_TABLES[0] = 'games'
            # DATABASE_TABLES[1] = 'achievements'
            insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[0], batch_games)
            insert_data(connection, XBOX_SCHEMA, DATABASE_TABLES[1], batch_achievements)
        except (Error, IndexError) as e:
            LOGGER.warning(e)
            self.added -= len(batch_games)

    def start(self):
        with connect_to_database() as connection:
            last_page = json.loads(
                self._request(self.games.format(page=1))).get('games', {}).get('pages', 0)
            with ThreadPoolExecutor() as executor:
                executor.map(lambda page: self.get_games(connection, page),
                             range(1, last_page + 1))
            LOGGER.info(f'Received "{self.added}" games during execution')

def main():
    XboxGames().start()
