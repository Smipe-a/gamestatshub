from typing import Dict, List
import os

DATABASE_TABLES: List[str] = [
    'games', 'achievements', 'players',
    'history', 'purchased_games', 'prices'
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
# Prefixes P_ - PlayStation; S_ - Steam; X_ - Xbox
P_PLAYERS_FILE_LOG: str = 'p_players.log'
P_GAMES_FILE_LOG: str = 'p_games.log'
P_HISTORY_FILE_LOG: str = 'p_history.log'
P_PRICES_FILE_LOG: str = 'p_prices.log'
S_PLAYERS_FILE_LOG: str = 's_players.log'
S_GAMES_FILE_LOG: str = 's_games.log'
S_HISTORY_FILE_LOG: str = 's_history.log'
S_PRICES_FILE_LOG: str = 's_prices.log'
X_PLAYERS_FILE_LOG: str = 'x_players.log'
X_GAMES_FILE_LOG: str = 'x_games.log'
X_HISTORY_FILE_LOG: str = 'x_history.log'
X_PRICES_FILE_LOG: str = 'x_prices.log'

CACHE_APPIDS: str = 'appids.pkl'
CACHE_ACHIEVEMENTS: str = 'achievements.pkl'
CASHE_PLAYERS: str = 'players.pkl'
