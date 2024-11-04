from datetime import datetime
from bs4 import BeautifulSoup
from typing import Optional
from time import sleep
import psycopg2
import re
from utils.constants import DATABASE_TABLES, PLAYSTATION_SCHEMA, P_GAMES_FILE_LOG
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger
from utils.fetcher import Fetcher

LOGGER = configure_logger(__name__, P_GAMES_FILE_LOG)


class PSNGames(Fetcher):
    def __init__(self):
        super().__init__()
        self.url = 'https://psnprofiles.com'

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

    def _get_last_page(self) -> Optional[int]:
        try:
            html_content = self.fetch_data(f'{self.url}/games')
            soup = BeautifulSoup(html_content, 'html.parser')
            return int(soup.find_all('ul', class_='pagination')[1].find_all(
                'li')[-2].text.strip().replace(',', ''))
        except Exception as e:
            LOGGER.error('Failed to retrieve the number of pages ' \
                         'for data collection. The process was ' \
                         'interrupted with error:', str(e).strip())
            return None
    
    def get_games(self):
        with connect_to_database() as connection:
            last_page = self._get_last_page()
            if last_page:
                for page in range(1, last_page + 1):
                    print(page)
                    # Default values
                    games, achievements = [], []
                    try:
                        html_content = self.fetch_data(f'{self.url}/games?page={page}')
                        soup = BeautifulSoup(html_content, 'html.parser')
                    except Exception as e:
                        LOGGER.warning(f'Failed to retrieve the code ' \
                                       f'of page "{page}". Error:', str(e).strip())
                        continue
                    table = soup.find('table', class_='zebra').find_all('tr')
                    # Iterate through each game from 1 to 50
                    for row in table:
                        region, developers, publishers, genres = None, None, None, None
                        release, completion_time, difficulty = None, None, None
                        game = row.find('td', style='width: 100%;').find('div')
                        game_url = game.find('a').get('href')
                        game_id = int(re.search(r'/trophies/(\d+)-', game_url).group(1))
                        game_title = game.find('a').text.strip()
                        platform = row.find('span',
                            class_='separator right').find('span').text.strip()
                        if game.find('bullet'):
                            region = game.text.strip().split(' • ')[-1]
                        # Delve into each game URL
                        try:
                            html_content = self.fetch_data(f'{self.url}{game_url}')
                            soup = BeautifulSoup(html_content, 'html.parser')
                        except Exception as e:
                            LOGGER.warning(f'Failed to retrieve the code ' \
                                           f'of page "{page}" and game_id "{game_id}". ' \
                                           f'Error:', str(e).strip())
                            continue
                        game_info = soup.find('table', class_='gameInfo zebra')
                        if game_info:
                            for block in game_info.find_all('tr'):
                                attribute = block.find('td').text
                                if attribute in {'Developers', 'Developer'}:
                                    developers = block.find('a').text.strip()
                                elif attribute in {'Publishers', 'Publisher'}:
                                    publishers = block.find('a').text.strip()
                                elif attribute in {'Genres', 'Genre'}:
                                    genres = []
                                    for genre in block.find_all('a'):
                                        genres.append(genre.text.strip())
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
                                # achievement_id is defined as -> achievement_No + g + game_id
                                achievement_id = info_block.find(
                                    'a').get('href').split('/')[-1].split('-')[0] +'g' + str(game_id)
                                achievement_title = info_block.find('a').text.strip()
                                description = info_block.text.strip().replace(achievement_title, '')
                                # If the title and description of the achievements match, return the description
                                if description == '':
                                    description = achievement_title
                                rarity = achievement.find(
                                    'td', style='padding-right: 10px').find('img').get('title')
                                achievements.append([
                                    achievement_id, game_id, achievement_title,description, rarity])
                        # -----------------------------------------
                        # Check if the game has a user guide
                        check_guide = soup.find('div', class_='guide-page-info sm')
                        if check_guide:
                            guide_url = check_guide.find('a').get('href')
                            try:
                                html_content = self.fetch_data(f'{self.url}{guide_url}')
                                soup = BeautifulSoup(html_content, 'html.parser')
                            except Exception as e:
                                LOGGER.warning(f'Failed to retrieve the code for user guide ' \
                                               f'of game_id "{game_id}". ' \
                                               f'Error:', str(e).strip())
                                continue
                            addition_info = soup.find(
                                'div', class_='overview-info').find_all('span', class_='typo-top')
                            # difficulty values between 1 and 10
                            difficulty = int(addition_info[0].text.strip().split('/')[0])
                            # completion_time - hours for 100%
                            completion_time = int(addition_info[2].text.strip())
                        games.append([game_id, game_title, platform, region,
                                      developers, publishers, genres, release,
                                      completion_time, difficulty])
                    try:
                        # DATABASE_TABLES[0] = 'games'
                        # DATABASE_TABLES[1] = 'achievements'
                        insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[0], games)
                        insert_data(connection, PLAYSTATION_SCHEMA, DATABASE_TABLES[1], achievements)
                        # Increasing the delay for data retrieval as 
                        # the site is returning a 429 error
                        sleep(0.5)
                    except psycopg2.Error as e:
                        LOGGER.warning(f'Error on page "{page}":', str(e).strip())

def main():
    PSNGames().get_games()
