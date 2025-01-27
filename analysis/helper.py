from typing import Optional, Dict, List, Set
import pandas as pd
from utils.database.connector import connect_to_database

PLATFORMS: List[str] = ['steam', 'playstation', 'xbox']
COLORS: List[str] = ['#e2b35c', '#87ceeb', '#73c991']

GENRES: Set[str] = {
    'Action', 'Indie', 'Adventure', 'Casual',
    'RPG', 'Strategy', 'Sports', 'Education', 'Racing',
    'Simulation', 'Sexual Content', 'Puzzle', 'Platformer',
    'Point & Click', 'Fighting', 'Survival', 'RPG', 'Horror'
}

def format_genre(genre_name: Optional[str]) -> Optional[str]:
    """
    Formats the given genre name into a standardized genre category

    The function accepts a genre name, capitalizes it, and then maps it to a standardized genre\\
    using a predefined dictionary. If the genre is not found in the dictionary, it returns 
    the original genre name

    Args:
        genre_name (Optional[str]): The genre name to be formatted

    Returns:
        Optional[str]: The formatted genre name or the original genre name if it was not found in the mapping,
                       or None if the input is not a string
    """
    try:
        genre_name = genre_name.capitalize()
    except AttributeError:
        return None

    formatted_genre: Dict[str, str] = {
        'Бойовики': 'Action', 'Экшены': 'Action', 'Ação': 'Action', '动作': 'Action',
        'First Person Shooter': 'Action', 'Run & Gun': 'Action', 'Shooter': 'Action',
        'Third Person Shooter': 'Action', 'FPS': 'Action', "Shoot 'em up": 'Action',
        'Hack & Slash': 'Action', 'Vehicular Combat': 'Action', 'Naval': 'Action',
        "Beat 'em up": 'Action',

        'Інді': 'Indie', 'Инди': 'Indie', '独立': 'Indie',

        'Стратегії': 'Strategy', 'Стратегии': 'Strategy', 'Estratégia': 'Strategy', '策略': 'Strategy',

        'Occasionnel': 'Casual', 'Казуальные игры': 'Casual', 'カジュアル': 'Casual', '休闲': 'Casual',
        'Card & Board': 'Casual', 'Casino': 'Casual', 'Collectable Card Game': 'Casual',
        'Pinball': 'Casual', 'Board Games': 'Casual', 'Collection': 'Casual',

        '冒險': 'Adventure', 'Приключенческие игры': 'Adventure', '冒险': 'Adventure',
        'Action-Adventure': 'Adventure', 'ARCADE': 'Adventure',

        '模擬': 'Simulation', 'Simulação': 'Simulation', 'シミュレーション': 'Simulation',

        'Nudity': 'Sexual Content',

        'Corrida': 'Racing', 'Automobile': 'Racing', 'Arcade Racing': 'Racing', 'Simulation Racing': 'Racing',
        'Motocross': 'Racing',
        
        'Equestrian Sports': 'Sports', 'Australian Football': 'Sports', 'Esportes': 'Sports',
        '体育': 'Sports', 'Volleyball': 'Sports', 'Basketball': 'Sports', 'Boxing': 'Sports', 'Golf': 'Sports',
        'Football': 'Sports', 'Cue Sports': 'Sports', 'Bowling': 'Sports', 'Skateboarding': 'Sports',
        'Health & Fitness': 'Sports', 'Skating': 'Sports', 'Snowboarding': 'Sports', 'Tennis': 'Sports',
        'Fishing': 'Sports', 'Baseball': 'Sports', 'Wrestling': 'Sports', 'American Football': 'Sports',
        'Dance': 'Sports', 'Table Tennis': 'Sports', 'Cricket': 'Sports', 'Hunting': 'Sports', 'Darts': 'Sports',
        'Rugby': 'Sports', 'Handball': 'Sports', 'Dodgeball': 'Sports', 'Classics': 'Sports', 'Hockey': 'Sports',
        'Bullfighting': 'Sports', 'Surfing': 'Sports', 'Lacrosse': 'Sports', 'Flying': 'Sports',
        'Skydiving': 'Sports', 'Kinect': 'Sports', 'Cycling': 'Sports', 'Bull Sports': 'Sports',

        '角色扮演': 'RPG', 'Ролевые игры': 'RPG', 'Role Playing': 'RPG', 'Action-RPG': 'RPG',
        'Role-Playing Games (RPG)': 'RPG',

        'Survival Horror': 'Horror', 'Action Horror': 'Horror',

        'Educational & Trivia': 'Education', 'Educational': 'Education',

        'Metroidvania': 'Platformer'
    }
    
    return formatted_genre.get(genre_name, genre_name)

def postgres_data(schema_name: str, table_name: str) -> pd.DataFrame:
    """
    Retrieves data from a specified table in a PostgreSQL database

    Args:
        schema_name (str): The schema in the database where the table is located
        table_name (str): The name of the table to query

    Returns:
        pd.DataFrame: A DataFrame containing the data from the specified table
    """
    with connect_to_database() as connection:
        query = """
            SELECT *
            FROM {schema_name}.{table_name};
        """
        
        if table_name == 'history':
            query = """
                SELECT *
                FROM {schema_name}.history
                WHERE DATE_PART('YEAR', date_acquired) = '2024';
            """

        df = pd.read_sql_query(
            query.format(schema_name=schema_name, table_name=table_name),
            connection,
            parse_dates=['release_date', 'date_acquired', 'created', 'posted', 'only_date']
        ).rename(columns={
            'game_id': 'gameid',
            'achievement_id': 'achievementid',
            'player_id': 'playerid'
        })

        return df
