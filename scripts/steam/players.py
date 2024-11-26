from typing import Generator, Optional, List
from decouple import config
from psycopg2 import Error
from time import sleep
import pycountry
import datetime
import pickle
from utils.constants import (STEAM_SCHEMA, DATABASE_TABLES,
                             S_PLAYERS_FILE_LOG, CASHE_PLAYERS)
from utils.fetcher import Fetcher, TooManyRequestsError, ForbiddenError
from utils.database.connector import connect_to_database, insert_data
from utils.logger import configure_logger

LOGGER = configure_logger(__name__, S_PLAYERS_FILE_LOG)


class SteamPlayers(Fetcher):
    def __init__(self):
        super().__init__()
        self.steamuser = 'https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={}&steamids={}'
        self.friends = 'https://api.steampowered.com/ISteamUser/GetFriendList/v0001/?key={}&steamid={}&relationship=friend'
        self.added = 0
    
    @staticmethod
    def _create_batches(appids: List[int], batch_size: int = 100) -> Generator[List[int], None, None]:
        for i in range(0, len(appids), batch_size):
            yield appids[i:i + batch_size]

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
            return datetime.datetime.fromtimestamp(timestamp)
        return None

    def start(self):
        with connect_to_database() as connection:
            # Initial steamids collected from different sections of Steam and various game categories
            try:
                with open(CASHE_PLAYERS, 'rb') as file:
                    steamids = pickle.load(file)
            except FileNotFoundError:
                steamids = [
                    '76561198039237628', '76561198029302470', '76561198025633383',
                    '76561198196298282', '76561198117967228', '76561198080218537',
                    '76561198146253210', '76561197970417960', '76561198083134207', 
                    '76561197972971221', '76561197990056992', '76561198043902016'
                ]
            # Tracking processed steamids
            visited_steamids = set()
            while steamids and self.added != 4e6:
                for batch in self._create_batches(steamids):
                    query = ','.join(batch)
                    try:
                        json_content = self.fetch_data(self.steamuser.format(config('API_KEY'), query), 'json')
                    except TooManyRequestsError:
                        # The Steam Web API restricts data retrieval to 200 requests every 5 minutes
                        sleep(301)
                        json_content = self.fetch_data(self.steamuser.format(config('API_KEY'), query), 'json')
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
                    except Error as e:
                        LOGGER.warning(e)
                    # Updating the counter for successful user processing
                    self.added += len(batch)
                    visited_steamids.update(batch)
                batches = self._create_batches(steamids)
                steamids = []
                for batch in batches:
                    for steamid in batch:
                        try:
                            json_content = self.fetch_data(self.friends.format(config('API_KEY'), steamid), 'json')
                        except TooManyRequestsError:
                            # The Steam Web API restricts data retrieval to 200 requests every 5 minutes
                            sleep(301)
                            json_content = self.fetch_data(self.steamuser.format(config('API_KEY'), query), 'json')
                        except ForbiddenError:
                            # The player's profile or data is hidden
                            continue
                        for friend in json_content.get('friendslist', {}).get('friends', []):
                            friendid = friend['steamid']
                            if friendid not in visited_steamids:
                                steamids.append(friendid)
                    with open(CASHE_PLAYERS, 'wb') as file:
                        pickle.dump(steamids, file)
                    break

def main():
    SteamPlayers().start()
