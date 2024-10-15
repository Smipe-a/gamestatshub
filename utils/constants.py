from typing import List
import os

DATABASE_TABLES: List[str] = ['games', 'achievements', 'players', 'achievements_history']
SCHEMA_NAME: str = 'public'

# We obtain the current directory and its parent directory.
# An absolute path is constructed based on the parent path
PROJECT_DIRECTORY: str = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
RESOURCE_CATALOG: str = 'resources'
LOG_CATALOG: str = 'logs'

DATABASE_INFO_FILE_LOG: str = 'database_info.log'
PLAYERS_FILE_LOG: str = 'players.log'
GAMES_FILE_LOG: str = 'games.log'
ACHIEVEMENTS_HISTORY: str = 'achievements_history.log'
