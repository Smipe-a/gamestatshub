from typing import Optional, Tuple
import os
from utils.constants import (PLAYSTATION_SCHEMA, STEAM_SCHEMA, XBOX_SCHEMA,
                            DATABASE_TABLES, DATABASE_INFO_FILE_LOG)
from utils.database.connector import connect_to_database
from utils.logger import configure_logger

LOGGER = configure_logger(os.path.basename(__file__), DATABASE_INFO_FILE_LOG)


def queries(schema_name: str, table_name: str) -> Optional[str]:
    query = {
        'playstation': {
            'games': """
                CREATE TABLE playstation.games (
                    game_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    platform VARCHAR(10),
                    developers TEXT[],
                    publishers TEXT[],
                    genres TEXT[],
                    release_date DATE
                );
            """,
            'achievements': """
                CREATE TABLE playstation.achievements (
                    achievement_id TEXT PRIMARY KEY,
                    game_id TEXT NOT NULL REFERENCES playstation.games (game_id) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    rarity TEXT NOT NULL,
                    CHECK (rarity IN ('Platinum', 'Gold', 'Silver', 'Bronze'))
                );
            """,
            'players': """
                CREATE TABLE playstation.players (
                    nickname TEXT PRIMARY KEY,
                    country TEXT NOT NULL
                );
            """,
            'history': """
                CREATE TABLE playstation.history (
                    nickname TEXT REFERENCES playstation.players (nickname) ON DELETE CASCADE,
                    achievement_id TEXT REFERENCES playstation.achievements (achievement_id) ON DELETE CASCADE,
                    date_acquired TIMESTAMP,
                    PRIMARY KEY (nickname, achievement_id)
                );
            """,
            'purchased_games': """
                CREATE TABLE playstation.purchased_games (
                    nickname TEXT PRIMARY KEY REFERENCES playstation.players (nickname) ON DELETE CASCADE,
                    library TEXT[]
                );
            """
        },
        'steam': {
            'games': """
                CREATE TABLE steam.games (
                    game_id INT PRIMARY KEY,
                    title TEXT NOT NULL,
                    developers TEXT[],
                    publishers TEXT[],
                    genres TEXT[],
                    supported_languages TEXT[],
                    release_date DATE
                );
            """,
            'achievements': """
                CREATE TABLE steam.achievements (
                    achievement_id TEXT PRIMARY KEY,
                    game_id INT NOT NULL REFERENCES steam.games (game_id) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    description TEXT
                );
            """,
            'players': """
                CREATE TABLE steam.players (
                    player_id TEXT PRIMARY KEY,
                    country TEXT,
                    created TIMESTAMP
                );    
            """,
            'history': """
                CREATE TABLE steam.history (
                    player_id TEXT NOT NULL REFERENCES steam.players (player_id) ON DELETE CASCADE,
                    achievement_id TEXT NOT NULL REFERENCES steam.achievements (achievement_id) ON DELETE CASCADE,
                    date_acquired TIMESTAMP,
                    PRIMARY KEY (player_id, achievement_id)
                );
            """,
            'purchased_games': """
                CREATE TABLE steam.purchased_games (
                    player_id TEXT PRIMARY KEY REFERENCES steam.players (player_id) ON DELETE CASCADE,
                    library INT[]
                );
            """
        },
        'xbox': {
            'games': """
                CREATE TABLE xbox.games (
                    gameid INT PRIMARY KEY,
                    title TEXT NOT NULL,
                    developers TEXT[],
                    publishers TEXT[],
                    genres TEXT[],
                    supported_languages TEXT[],
                    release_date DATE
                );
            """,
            'achievements': """
                CREATE TABLE xbox.achievements (
                    achievementid TEXT PRIMARY KEY,
                    gameid INT NOT NULL REFERENCES xbox.games (gameid) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    points INT
                );
            """,
            'players': """
                CREATE TABLE xbox.players (
                    playerid INT PRIMARY KEY,
                    nickname TEXT
                );
            """,
            'history': """
                CREATE TABLE xbox.history (
                    playerid INT NOT NULL REFERENCES xbox.players (playerid) ON DELETE CASCADE,
                    achievementid TEXT NOT NULL REFERENCES xbox.achievements (achievementid) ON DELETE CASCADE,
                    date_acquired TIMESTAMP,
                    PRIMARY KEY (playerid, achievementid)
                );
            """,
            'purchased_games': """
                CREATE TABLE xbox.purchased_games (
                    playerid INT PRIMARY KEY REFERENCES xbox.players (playerid) ON DELETE CASCADE,
                    library INT[]
                );
            """
        }
    }
    return query.get(schema_name, {}).get(table_name, None)

def _check_schema(cursor, schema_name: str) -> Optional[Tuple[str]]:
    cursor.execute('SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s;', (schema_name,))
    return cursor.fetchone()

def is_schema(cursor, schema_name: str) -> Optional[bool]:
    try:
        existing_schema = _check_schema(cursor, schema_name)
        if not existing_schema:
            cursor.execute(f'CREATE SCHEMA {schema_name};')
            connection.commit()
            LOGGER.info(f'Schema "{schema_name}" has been successfully created in the database')
        else:
            LOGGER.info(f'The schema "{schema_name}" already exist')
        return True
    except Exception as e:
        LOGGER.error('Error creating schema:', str(e).strip())
        return None

def create_table(cursor, schema_name: str, table_name: str) -> None:
    try:
        check_table_query = """
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = %s AND table_name = %s
                            """
        cursor.execute(check_table_query, (schema_name, table_name))
        # If such a table is absent, we create it
        if cursor.fetchone() is None:
            query = queries(schema_name, table_name)
            if query:
                cursor.execute(queries(schema_name, table_name))
                connection.commit()
                LOGGER.info(f'Successfully created table "{table_name}"')
            else:
                LOGGER.warning(f'The specified table "{table_name}" or ' \
                               f'schema "{schema_name}" does not match the list of queries for table creation')
        else:
            LOGGER.info(f'The table "{table_name}" in the schema "{schema_name}" already exist')
    except Exception as e:
        LOGGER.error('Error creating table:', str(e).strip())


if __name__ == '__main__':
    with connect_to_database() as connection, connection.cursor() as cursor:
        for schema_name in [PLAYSTATION_SCHEMA, STEAM_SCHEMA, XBOX_SCHEMA]:
            if is_schema(cursor, schema_name):
                for table_name in DATABASE_TABLES:
                    create_table(cursor, schema_name, table_name)
