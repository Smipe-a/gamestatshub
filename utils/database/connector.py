from psycopg2 import connect, extensions, OperationalError, Error
from typing import List, Any
from decouple import config
from pathlib import Path
from utils.constants import DATABASE_INFO_FILE_LOG
from utils.logger import configure_logger

LOGGER = configure_logger(Path(__file__).name, DATABASE_INFO_FILE_LOG)


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
        LOGGER.fatal(f'Error connecting to the database: {str(e).strip()}')
        raise

def insert_data(connection: extensions.connection, 
                schema_name: str, table_name: str, data: List[List[Any]]) -> None:
    try:
        with connection.cursor() as cursor:
            # Retrieving the table column names
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
            """, (schema_name, table_name))
            # Excluding review_id because it is a SERIAL PK
            columns = [col[0] for col in cursor.fetchall() if col[0] != 'review_id']

            # Number of values being inserted
            placeholders = ', '.join(['%s'] * len(data[0]))
            
            query = f"""
                INSERT INTO {schema_name}.{table_name} ({', '.join(columns)}) 
                VALUES ({placeholders}) 
                ON CONFLICT DO NOTHING;
            """
            cursor.executemany(query, data)
            connection.commit()
    except Error as e:
        connection.rollback()
        raise Error(f'Error inserting data into "{schema_name}.{table_name}": {str(e).strip()}')
    except IndexError:
        raise IndexError(f'Attempt to insert an empty number of rows into the database "{schema_name}.{table_name}"')

def delete_data(connection: extensions.connection,
                schema_name: str, table_name: str, column_name: str,
                data: List[List[Any]]) -> None:
    try:
        with connection.cursor() as cursor:
            query = f"""
                DELETE FROM {schema_name}.{table_name}
                WHERE {column_name} = %s;
                """
            cursor.executemany(query, data)
            connection.commit()
    except Error as e:
        connection.rollback()
        raise Error(e)
