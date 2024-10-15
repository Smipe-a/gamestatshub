from psycopg2 import connect, extensions, OperationalError, Error
from typing import List, Any
from decouple import config
from utils.constants import DATABASE_INFO_FILE_LOG
from utils.logger import configure_logger

LOGGER = configure_logger(__name__, DATABASE_INFO_FILE_LOG)


def connect_to_database() -> extensions.connection:
    try:
        with connect(
            database='gamestatshub',
            user=config('PG_USER'),
            password=config('PG_PASSWORD'),
            host=config('PG_HOST'),
            port='5432'
        ) as current_connection:
            return current_connection
    except OperationalError as e:
        LOGGER.fatal(f'Error connecting to the database:', {str(e).strip()})
        raise

def insert_data(connection: extensions.connection, 
                schema_name: str, table_name: str, data: List[List[Any]]) -> None:
    try:
        with connection.cursor() as cursor:
            placeholders = ', '.join(['%s'] * len(data[0]))
            query = f"""
                INSERT INTO {schema_name}.{table_name} VALUES (
                    {placeholders}
                ) ON CONFLICT DO NOTHING;
                """
            cursor.executemany(query, data)
            connection.commit()
    except Error as e:
        connection.rollback()
        raise Error(f'Error inserting data into "{schema_name}.{table_name}":', str(e).strip())
