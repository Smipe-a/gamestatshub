from concurrent.futures import ThreadPoolExecutor
from typing import Generator, Optional, List, Set
from psycopg2 import Error, extensions
from bs4 import BeautifulSoup
from datetime import datetime
from decouple import config
from pathlib import Path
from time import sleep
import pycountry
import pickle
import re
from utils.constants import (STEAM_SCHEMA, DATABASE_TABLES,
                             STEAM_LOGS, CASHE_PLAYERS)
from utils.fetcher import Fetcher, TooManyRequestsError, ForbiddenError
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger

LOGGER = configure_logger(Path(__file__).name, STEAM_LOGS)


def _create_batches(appids: List[int], batch_size: int = 100) -> Generator[List[int], None, None]:
    for i in range(0, len(appids), batch_size):
        yield appids[i:i + batch_size]


class SteamReviews(Fetcher):
    def __init__(self):
        super().__init__()
        self.steam = 'https://api.steampowered.com/ISteamUser/'
        self.user_data = self.steam + 'GetPlayerSummaries/v0002/?key={api_key}&steamids={steamids}'
        self.reviews = '{player_url}recommended/?p={page}'

        # Number of records added to the 'reviews' table
        self.added = 0
    
    @staticmethod
    def get_steamids(connection: extensions.connection) -> List[Optional[str]]:
        try:
            # The approach for retrieving the list of steamids is such that
            # if we have already obtained a user's review information and
            # they later add a new review, we will no longer be able to
            # insert the new information into the database
            with connection.cursor() as cursor:
                query = """
                    SELECT p.player_id
                    FROM steam.players p
                    WHERE NOT EXISTS (SELECT 1 FROM steam.reviews r WHERE r.player_id = p.player_id) AND
                          NOT EXISTS (SELECT 1 FROM steam.private_steamids ps WHERE ps.player_id = p.player_id);
                """
                cursor.execute(query)
                return [steamid[0] for steamid in cursor.fetchall()]
        except Exception as e:
            LOGGER.warning(f'Failed to retrieve the list of steamids from the database. ' \
                           f'Error: {str(e).strip()}')
            return []
    
    @staticmethod
    def get_gameids(connection: extensions.connection) -> Set[str]:
        try:
            with connection.cursor() as cursor:
                query = """
                    SELECT game_id
                    FROM steam.games;
                """
                cursor.execute(query)
                return {steamid[0] for steamid in cursor.fetchall()}
        except Exception as e:
            LOGGER.error(f'Failed to retrieve the list of gameids from the database. ' \
                         f'Error: {str(e).strip()}')
            raise Exception(e)

    @staticmethod
    def formatted_date(date: str) -> str:
        # Example: Posted 13 October, 2021. Last edited 23 November, 2023. -> 2023-11-23
        date = date.replace(',', '').split('.')[-2].split()
        construct_date = f'{date[-3]} {date[-2]} {date[-1]}'
        try:
            return datetime.strptime(construct_date, "%d %B %Y")
        except ValueError:
            # There may also be a case where the review is created in the current year
            # Example: Posted 28 June. -> 2024-06-28
            construct_date = f'{date[-2]} {date[-1]} {datetime.now().year}'
            return datetime.strptime(construct_date, "%d %B %Y")

    def get_reviews(self, connection: extensions.connection, steamid: str,
                          player_url: str, gameids: Set[int]):
        page, user_reviews = 1, []

        while True:
            html_content = self.fetch_data(self.reviews.format(player_url=player_url, page=page))
            soup = BeautifulSoup(html_content, 'html.parser')

            # This try-except block is needed to avoid infinite iteration through pages
            # If the reviews block is not found, it means we have processed all the user's data
            try:
                reviews = soup.find('div', id='leftContents').find_all('div', class_='review_box')
            except AttributeError:
                break
            
            if not reviews:
                # This is reached when processing all the user's reviews (navigating to an existing page)
                break
            
            for review in reviews:
                gameid = int(review.find('div', class_='leftcol').find('a').get('href').split('/')[-1])
                # Check if the game exists in our database
                # If it doesn't, we simply don't record this review
                if gameid not in gameids:
                    continue
                
                description = review.find('div', class_='rightcol').find('div', class_='content').text.strip()

                posted = self.formatted_date(
                    review.find('div', class_='rightcol').find('div', class_='posted').text.strip())

                review_info = review.find('div', class_='header')

                helpful = funny = 0
                header_text = review_info.text.strip()
                helpful_match = re.search(r'(\d+)\s+people found this review helpful', header_text)
                funny_match = re.search(r'(\d+)\s+people found this review funny', header_text)
                if helpful_match:
                    helpful = int(helpful_match.group(1))
                if funny_match:
                    funny = int(funny_match.group(1))

                awards = 0
                try:
                    award_quantity = review_info.find(
                        'div', class_='review_award_ctn').find_all('div', class_='review_award tooltip')
                except AttributeError:
                    award_quantity = []
                for award in award_quantity:
                    awards += int(award.find('span').text.strip())
                
                user_reviews.append([steamid, gameid, description, helpful, funny, awards, posted])
            page += 1
        try:
            # DATABASE_TABLES[6] = 'reviews'
            insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[6], user_reviews)
            self.added += len(user_reviews)
        except Error as e:
            LOGGER.warning(e)
        except IndexError as e:
            # The player's data is either hidden, or the player has not written any reviews
            # DATABASE_TABLES[8] = 'private_steamids'
            insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[8], [[steamid]])

    def start(self):
        with connect_to_database() as connection:
            gameids = self.get_gameids(connection)
            steamids = self.get_steamids(connection)

            for batch in _create_batches(steamids):
                steamids = self.user_data.format(api_key=config('API_KEY'), steamids=','.join(batch))
                try:
                    json_content = self.fetch_data(steamids, 'json')
                except TooManyRequestsError:
                    # The Steam Web API restricts data retrieval to 200 requests every 5 minutes
                    sleep(301)
                    # Sometimes throws TooManyRequestsError,
                    # which is handled in an external try-except block
                    json_content = self.fetch_data(steamids, 'json')

                profiles = [(players['profileurl'], players['steamid'])
                            for players in json_content.get('response', {}).get('players', [])]

                with ThreadPoolExecutor() as executor:
                    # profile[0] - steamurl, profile[1] - steamid
                    executor.map(lambda profile: 
                                 self.get_reviews(connection, profile[1], profile[0], gameids), profiles)
            
            LOGGER.info(f'Added "{self.added}" new data to the table "steam.reviews"')

class SteamPlayers(Fetcher):
    def __init__(self):
        super().__init__()
        self.steam = 'https://api.steampowered.com/ISteamUser/'
        self.user_data = self.steam + 'GetPlayerSummaries/v0002/?key={api_key}&steamids={steamids}'
        self.friends = self.steam + 'GetFriendList/v0001/?key={api_key}&steamid={steamid}&relationship=friend'
        
        # Number of records added to the 'players' table
        self.added = 0
        # Number of records added to the 'friends' table
        self.added_friends = 0

    @staticmethod
    def _format_country(country_code: Optional[str]) -> Optional[str]:
        # ISO 3166-1 alpha-2
        try:
            return pycountry.countries.get(alpha_2=country_code).name
        except (LookupError, AttributeError):
            return None

    @staticmethod
    def _format_timestamp(timestamp: Optional[int]) -> Optional[str]:
        # UNIX-timestamp
        if timestamp:
            return datetime.fromtimestamp(timestamp)
        return None

    def start(self):
        with connect_to_database() as connection:
            # Initial steamids collected from different sections of Steam
            # and various game categories
            try:
                with open('./resources/' + CASHE_PLAYERS, 'rb') as file:
                    steamids = pickle.load(file)
                if not isinstance(steamids, list) or len(steamids) == 0:
                    raise FileNotFoundError
            except FileNotFoundError:
                steamids = [
                    '76561198039237628', '76561198029302470', '76561198025633383',
                    '76561198196298282', '76561198117967228', '76561198080218537',
                    '76561198146253210', '76561197970417960', '76561198083134207', 
                    '76561197972971221', '76561197990056992', '76561198043902016'
                ]
            
            # The snowball method is used
            # Tracking processed steamids
            visited_steamids = set()
            while steamids and self.added != 4e6:
                for batch in _create_batches(steamids):
                    steamids_url = self.user_data.format(api_key=config('API_KEY'), steamids=','.join(batch))
                    
                    try:
                        json_content = self.fetch_data(steamids_url, 'json')
                    except TooManyRequestsError:
                        # The Steam Web API restricts data retrieval to 200 requests every 5 minutes
                        sleep(301)
                        json_content = self.fetch_data(steamids_url, 'json')
                    
                    players = []
                    for player in json_content.get('response', {}).get('players', []):
                        players.append([
                            player['steamid'],
                            self._format_country(player.get('loccountrycode', None)),
                            self._format_timestamp(player.get('timecreated', None))
                        ])

                    try:
                        # DATABASE_TABLES[2] = 'players'
                        insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[2], players)
                        self.added += len(players)
                    except Error as e:
                        LOGGER.error(e)
                    except IndexError as e:
                        LOGGER.warning(e)
                    
                    visited_steamids.update(batch)

                # Initially, steamids contains the complete list of users
                # Rewriting steamids with successfully processed ones
                steamids = [player[0] for player in players]
                batches = _create_batches(steamids)

                # Clearing the list because it will store data only
                # about the friends from the first batch (to avoid overloading the process)
                steamids = []
                for batch in batches:
                    total_friends = []
                    # Iterating through each user
                    for steamid in batch:
                        friends_url = self.friends.format(api_key=config('API_KEY'), steamid=steamid)
                        try:
                            json_content = self.fetch_data(friends_url, 'json')
                        except TooManyRequestsError:
                            # The Steam Web API restricts data retrieval to 200 requests every 5 minutes
                            sleep(301)
                            json_content = self.fetch_data(friends_url, 'json')
                        except ForbiddenError:
                            # The player's profile or data is hidden
                            total_friends.append([steamid, None])
                            continue
                        
                        friends = []
                        for friend in json_content.get('friendslist', {}).get('friends', []):
                            friendid = friend['steamid']
                            # Eliminating potential loops
                            if friendid not in visited_steamids:
                                steamids.append(friendid)
                            friends.append(friendid)
                        
                        if not friends:
                            total_friends.append([steamid, None])
                        else:
                            total_friends.append([steamid, friends])
                        
                    try:
                        # DATABASE_TABLES[7] = 'friends'
                        insert_data(connection, STEAM_SCHEMA, DATABASE_TABLES[7], total_friends)
                        # Updating the counter for successful user processing
                        self.added_friends += len(total_friends)
                    except Error as e:
                        LOGGER.error(e)
                    except IndexError as e:
                        LOGGER.warning(e)

                    with open('./resources/' + CASHE_PLAYERS, 'wb') as file:
                        pickle.dump(steamids, file)
                    break
            
            LOGGER.info(f'Added "{self.added}" new data to the table "steam.players"')
            LOGGER.info(f'Added "{self.added_friends}" new data to the table "steam.friends"')

def main(process):
    processes = {
        'players': SteamPlayers,
        'reviews': SteamReviews
    }

    if process in processes:
        LOGGER.info(f'Process started with parameter process="{process}"')
        try:
            process_class = processes[process]()
            process_class.start()
        except (Exception, KeyboardInterrupt) as e:
            if str(e) == '':
                e = 'Forced termination'
            LOGGER.error(f'An unhandled exception occurred with error: {str(e).strip()}')
            LOGGER.info(f'Added "{process_class.added}" new data to the table "steam.{process}"')
            if process == 'players':
                LOGGER.info(f'Added "{process_class.added_friends}" new data to the table "steam.friends"')
            
            raise Exception(e)
    else:
        message = f'The specified process "{process}" is not included in the available options'
        LOGGER.error(f'Process returned an error: {message}')
        raise ValueError(message)
