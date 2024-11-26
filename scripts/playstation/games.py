from datetime import datetime
from bs4 import BeautifulSoup
from typing import Optional
from time import sleep
import psycopg2
import re
from utils.constants import DATABASE_TABLES, PLAYSTATION_SCHEMA, P_GAMES_FILE_LOG
from utils.database.connector import connect_to_database, insert_data
from utils.fetcher import Fetcher, TooManyRequestsError
from utils.logger import configure_logger

LOGGER = configure_logger(__name__, P_GAMES_FILE_LOG)


class PSNGames(Fetcher):
    def __init__(self):
        super().__init__()
        self.url = 'https://psnprofiles.com{}'

    @staticmethod
    def _format_date(date: str) -> Optional[str]:
        # There are two date formats that need to be processed
        #        Jun 08, 2021          October 2, 2009
        try:
            if len(date.split()[0]) == 3:
                date_object = datetime.strptime(date, '%b %d, %Y')
            else:
                date_object = datetime.strptime(date, '%B %d, %Y')
        except ValueError:
            return None
        return date_object.strftime('%Y-%m-%d')

    def _get_last_page(self) -> int:
        try:
            html_content = self.fetch_data(self.url.format('/games'))
            soup = BeautifulSoup(html_content, 'html.parser')
            return int(soup.find_all('ul', class_='pagination')[1].find_all(
                'li')[-2].text.strip().replace(',', ''))
        except Exception as e:
            LOGGER.error('Failed to retrieve the number of pages ' \
                         'for data collection. The process was ' \
                         'interrupted with error: %s', str(e).strip())
            return 0
    
    def start(self):
        with connect_to_database() as connection:
            for page in range(1, self._get_last_page() + 1):
                # Default values
                games, achievements = [], []
                appsurl = self.url.format(f'/games?page={page}')
                try:
                    html_content = self.fetch_data(appsurl)
                    soup = BeautifulSoup(html_content, 'html.parser')
                except TooManyRequestsError:
                    # Catch 429
                    sleep(3)
                    html_content = self.fetch_data(appsurl)
                    soup = BeautifulSoup(html_content, 'html.parser')
                except Exception as e:
                    LOGGER.warning(f'Failed to retrieve the code ' \
                                   f'of page "{page}". Error: {str(e).strip()}')
                    continue
                # Iterate through each game from 1 to 50
                for app in soup.find('table', class_='zebra').find_all('tr'):
                    developers, publishers, genres, release = None, None, None, None
                    appinfo = app.find('td', style='width: 100%;').find('div')
                    appid = re.search(r'/trophies/(\d+)-', appinfo.find('a').get('href')).group(1)
                    game_title = appinfo.find('a').text.strip()
                    platform = app.find('span',
                        class_='separator right').find('span').text.strip()
                    # Delve into each game URL
                    appurl = self.url.format(appinfo.find('a').get('href'))
                    try:
                        html_content = self.fetch_data(appurl)
                        soup = BeautifulSoup(html_content, 'html.parser')
                    except TooManyRequestsError:
                        # Catch 429
                        sleep(3)
                        html_content = self.fetch_data(appurl)
                        soup = BeautifulSoup(html_content, 'html.parser')
                    except Exception as e:
                        LOGGER.warning(f'Failed to retrieve the code ' \
                                       f'of page "{page}" and game_id "{appid}". ' \
                                       f'Error: {str(e).strip()}')
                        continue
                    gameinfo = soup.find('table', class_='gameInfo zebra')
                    if gameinfo:
                        for block in gameinfo.find_all('tr'):
                            attribute = block.find('td').text
                            if attribute in {'Developers', 'Developer'}:
                                developers = [developer.text.strip() for developer in block.find_all('a')]
                            elif attribute in {'Publishers', 'Publisher'}:
                                publishers = [publisher.text.strip() for publisher in block.find_all('a')]
                            elif attribute in {'Genres', 'Genre'}:
                                genres = [genre.text.strip() for genre in block.find_all('a')]
                            elif attribute in {'Releases', 'Release'}:
                                release = self._format_date(block.find_all(
                                    'td')[1].text.strip().split('\n')[1].split('\t')[-1])
                    # -----------------------------------------
                    # Here, we work with the achievements block 
                    # and the database table achievements
                    position = 0
                    game_dlc = soup.find_all('div', class_='box no-top-border')[:-1]
                    # Games with DLC have an empty <tr> block 
                    # in the data list that needs to be processed
                    if len(game_dlc) > 1 or game_title in {'Smash Cars', 'flOw'}:
                        position = 1
                    for list_achievements in game_dlc:
                        for achievement in list_achievements.find_all('table',
                                                class_='zebra')[-1].find_all('tr')[position:]:
                            info_block = achievement.find('td', style='width: 100%;')
                            # For PRIMARY KEY
                            # achievement_id is defined as -> appid_achievementNo 
                            achievementid = info_block.find('a').get('href').split('/')[-1].split('-')[0]
                            achievement_id = f"{appid}_{achievementid}"
                            achievement_title = info_block.find('a').text.strip()
                            description = info_block.text.strip()[len(achievement_title):]
                            rarity = achievement.find(
                                'td', style='padding-right: 10px').find('img').get('title')
                            achievements.append([
                                achievement_id, appid, achievement_title, description, rarity])
                    games.append(
                        [appid, game_title, platform, developers, publishers, genres, release])
                try:
                    # DATABASE_TABLES[0] = 'games'
                    # DATABASE_TABLES[1] = 'achievements'
                    insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[0], games)
                    insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[1], achievements)
                    # Increasing the delay for data retrieval as 
                    # the site is returning a 429 error
                    sleep(0.5)
                except psycopg2.Error as e:
                    LOGGER.warning(f'Error on page "{page}": {str(e).strip()}')

def main():
    PSNGames().start()
