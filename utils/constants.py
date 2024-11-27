from typing import List
import os

DATABASE_TABLES: List[str] = ['games', 'achievements', 'players', 'achievements_history', 'purchased_games']
PLAYSTATION_SCHEMA: str = 'playstation'
STEAM_SCHEMA: str = 'steam'
XBOX_SCHEMA: str = 'xbox'

# We obtain the current directory and its parent directory.
# An absolute path is constructed based on the parent path
PROJECT_DIRECTORY: str = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
LOG_CATALOG: str = 'logs'

DATABASE_INFO_FILE_LOG: str = 'database_info.log'
# Prefixes P_ - PlayStation; S_ - Steam; X_ - Xbox
P_PLAYERS_FILE_LOG: str = 'p_players.log'
P_GAMES_FILE_LOG: str = 'p_games.log'
P_ACHIEVEMENTS_HISTORY_FILE_LOG: str = 'p_achievements_history.log'
S_PLAYERS_FILE_LOG: str = 's_players.log'
S_GAMES_FILE_LOG: str = 's_games.log'
S_ACHIEVEMENTS_HISTORY_FILE_LOG: str = 's_achievements_history.log'
X_PLAYERS_FILE_LOG: str = 'x_players.log'
X_GAMES_FILE_LOG: str = 'x_games.log'

CACHE_APPIDS: str = 'appids.pkl'
CACHE_ACHIEVEMENTS: str = 'achievements.pkl'
CASHE_PLAYERS: str = 'players.pkl'
