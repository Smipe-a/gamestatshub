from typing import Optional, Tuple
import os
from utils.constants import DATABASE_TABLES, DATABASE_INFO_FILE_LOG
from utils.database.connector import connect_to_database
from utils.logger import configure_logger

LOGGER = configure_logger(os.path.basename(__file__), DATABASE_INFO_FILE_LOG)


def queries(name_table: str) -> str:
    query = {
        'games': f"""
            CREATE TABLE public.games (
                game_id INT PRIMARY KEY,
                title VARCHAR(150) NOT NULL,
                platform VARCHAR(10) NOT NULL,
                region VARCHAR(5),
                developer VARCHAR(80),
                publisher VARCHAR(80),
                genres TEXT[],
                release_date DATE,
                completion_time INT,
                difficulty INT,
                CHECK (difficulty > 0 AND difficulty < 11)
            );
        """,
        'achievements': f"""
            CREATE TABLE public.achievements (
                achievement_id VARCHAR(15) PRIMARY KEY,
                game_id INT NOT NULL REFERENCES public.games (game_id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                rarity TEXT NOT NULL,
                CHECK (rarity IN ('Platinum', 'Gold', 'Silver', 'Bronze'))
            );
        """,
        'players': f"""
            CREATE TABLE public.players (
                nickname TEXT PRIMARY KEY,
                country TEXT NOT NULL
            );
        """,
        'achievements_history': f"""
            CREATE TABLE public.achievements_history (
                nickname TEXT REFERENCES public.players (nickname) ON DELETE CASCADE,
                achievement_id VARCHAR(15) REFERENCES public.achievements (achievement_id) ON DELETE CASCADE,
                date_acquired TIMESTAMP,
                PRIMARY KEY (nickname, achievement_id)
            );
        """
    }

    return query[name_table]


def _check_schema(cursor, schema_name: str) -> Optional[Tuple[str]]:
    cursor.execute('SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s;', (schema_name,))
    return cursor.fetchone()


def create_schema(cursor, schema_name: str) -> Optional[bool]:
    try:
        existing_schema = _check_schema(cursor, schema_name)

        if not existing_schema:
            cursor.execute(f'CREATE SCHEMA {schema_name};')
            connection.commit()
            LOGGER.info(f'Schema "{schema_name}" has been successfully created in the database.')
        else:
            LOGGER.info(f'The schema "{schema_name}" already exist.')
        return True

    except Exception as e:
        LOGGER.error(f'Error creating schema: {str(e).strip()}.')
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
            cursor.execute(queries(table_name))
            connection.commit()
        else:
            LOGGER.info(f'The table "{table_name}" in the schema "{schema_name}" already exist.')

    except Exception as e:
        LOGGER.error(f'Error creating table: {str(e).strip()}.')


if __name__ == '__main__':
    with connect_to_database() as connection, connection.cursor() as cursor:
        created_tables = 0
        is_exist_schema = create_schema(cursor, 'public')
        
        for table_name in DATABASE_TABLES:
            if not is_exist_schema:
                LOGGER.warning('Failed to initialize the database.')
                break

            create_table(cursor, 'public', table_name)
            created_tables += 1
                
        
        LOGGER.info(f'Successfully created {created_tables} tables ' \
                    f'out of {len(DATABASE_TABLES)} for schema "public".')
