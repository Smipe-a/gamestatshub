from typing import Dict, List
import os

DATABASE_TABLES: List[str] = [
    'games', 'achievements', 'players',
    'history', 'purchased_games', 'prices',
    'reviews', 'friends', 'private_steamids'
]

PLAYSTATION_SCHEMA: str = 'playstation'
STEAM_SCHEMA: str = 'steam'
XBOX_SCHEMA: str = 'xbox'

# Currency - USD, EUR, GBP, JPY, RUB
CURRENCY: Dict[str, List[str]] = {
    'steam': ['us', 'de', 'gb', 'jp', 'ru'],
    'playstation': ['region-us', 'region-de', 'region-gb', 'region-jp', 'region-ru'],
    'xbox': ['region-us', 'region-de', 'region-gb', 'region-jp', 'region-ru']
}

# We obtain the current directory and its parent directory.
# An absolute path is constructed based on the parent path
PROJECT_DIRECTORY: str = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
LOG_CATALOG: str = 'logs'

DATABASE_INFO_FILE_LOG: str = 'database_info.log'
STEAM_LOGS: str = 'steam.log'
PLAYSTATION_LOGS: str = 'playstation.log'
XBOX_LOGS: str = 'xbox.log'

X_PLAYERS_FILE_LOG: str = 'x_players.log'
X_GAMES_FILE_LOG: str = 'x_games.log'
X_HISTORY_FILE_LOG: str = 'x_history.log'
X_PRICES_FILE_LOG: str = 'x_prices.log'
X_MISSING_DATA_FILE_LOG: str = 'x_missing_data.log'

# ------------------------ Steam ------------------------
CACHE_APPIDS: str = 'appids.pkl'
CACHE_ACHIEVEMENTS: str = 'achievements.pkl'
CASHE_PLAYERS: str = 'players.pkl'

# --------------------- PlayStation ---------------------
CASHE_PLAYSTATIONURLS: str = 'playstationurls.pkl'
MATCH_MISSING_DATA: str = 'missing_data.csv'

# ------------------------ Xbox -------------------------
CASHE_XBOXURLS: str = 'xboxurls.pkl'
