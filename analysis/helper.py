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
    Formats the given genre name into a standardized genre category.

    The function accepts a genre name, capitalizes it, and then maps it to a standardized genre  
    using a predefined dictionary. If the genre is not found in the dictionary, it returns  
    the original genre name.

    Args:
        **genre_name (Optional[str])** - The genre name to be formatted.

    Returns:
        **Optional[str]** - The formatted genre name or the original genre name if it was not found in the mapping,
                       or None if the input is not a string.
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


def define_currency(country: str) -> str:
    """
    Determines the currency for a given country based on predefined sets of countries.

    Args:
        **country (str)** - The name of the country for which the currency needs to be determined.

    Returns:
        **str** - The currency code corresponding to the provided country. 
             Possible values are 'EUR', 'GBP', 'RUB', 'JPY', or 'USD' by default.
    """
    eur = {
        'Austria', 'Belgium', 'Croatia', 'Cyprus', 'Estonia',
        'Finland', 'France', 'Germany', 'Greece', 'Poland',
        'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg',
        'Malta', 'Netherlands', 'Portugal', 'Sweden', 'Slovakia', 
        'Slovenia', 'Spain'
    }
    gbp = {
        'United Kingdom', 'Scotland', 'Wales',
        'Northern Ireland', 'England'
    }
    rub = {
        'Russian Federation', 'Kazakhstan', 'Uzbekistan',
        'Ukraine', 'Kyrgyzstan', 'Armenia', 'Belarus',
        'Moldova', 'Tajikistan', 'Turkmenistan', 'Azerbaijan'
    }
    jpy = {
        'Japan'
    }
    
    if country in eur:
        return 'EUR'
    elif country in gbp:
        return 'GBP'
    elif country in rub:
        return 'RUB'
    elif country in jpy:
        return 'JPY'
    
    return 'USD'


def assign_region(country: str) -> str:
    """
    Assigns a region based on the provided country.

    Args:
        **country (str)** - The name of the country to categorize.

    Returns:
        **str** - The region the country belongs to. Possible values are 'Europe', 'US & Canada', 'Asia', or 'Rest of the world'.
    """
    eu_countries = {
        'Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic',
        'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary',
        'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta',
        'Netherlands', 'Poland', 'Portugal', 'Romania', 'Slovakia', 'Slovenia',
        'Spain', 'Sweden', 'Georgia', 'Iceland', 'Monaco', 'Norway', 'Serbia',
        'Switzerland', 'United Kingdom', 'England', 'Northern Ireland', 'Wales',
        'Scotland'
    }

    us_canada_countries = {
        'United States', 'Canada'
    }

    asian_countries = {
        'Afghanistan', 'Armenia', 'Azerbaijan', 'Bahrain', 'Bangladesh', 'Bhutan', 
        'Brunei', 'Cambodia', 'China', 'India', 'Indonesia', 
        'Iran', 'Iraq', 'Israel', 'Japan', 'Jordan', 'Kazakhstan', 'Korea', 
        'Kuwait', 'Kyrgyzstan', 'Laos', 'Lebanon', 'Malaysia', 
        'Maldives', 'Mongolia', 'Myanmar', 'Nepal', 'Oman', 'Pakistan', 'Palestine', 
        'Philippines', 'Qatar', 'Russia', 'Saudi Arabia', 'Singapore', 'Sri Lanka', 
        'Syria', 'Tajikistan', 'Thailand', 'Timor-Leste', 'Türkiye', 'Turkmenistan', 
        'United Arab Emirates', 'Uzbekistan', 'Vietnam', 'Yemen', 'Russian Federation',
        'Ukraine', 'Belarus', 'Moldova'
    }

    if country in eu_countries:
        return 'Europe'
    elif country in us_canada_countries:
        return 'US & Canada'
    elif country in asian_countries:
        return 'Asia'
    else:
        return 'Rest of the world'


def define_game(achievementid: str) -> int:
    """
    Extracts and returns the UniqueGameID from a given achievementID.

    The achievementID is expected to be in the format 'UniqueGameID_NonUniqueAchievementID'.  
    This function splits the string at the underscore ('_') and converts the first part (UniqueGameID) into an integer.

    Args:
        **achievementid (str)** - The achievementID.

    Returns:
        **int** - The UniqueGameID as an integer.

    Example:
        '12345_achievement1' -> 12345
    """
    return int(achievementid.split('_')[0])


def postgres_data(schema_name: str, table_name: str, year: int = 2024) -> pd.DataFrame:
    """
    Retrieves data from a specified table in a PostgreSQL database.

    Args:
        **schema_name (str)** - The schema in the database where the table is located.
        **table_name (str)** - The name of the table to query.

    Returns:
        **pd.DataFrame** - A DataFrame containing the data from the specified table.
    """
    with connect_to_database() as connection:
        query = """
            SELECT *
            FROM {schema_name}.{table_name};
        """
        
        if table_name == 'history':
            query = f"""
                SELECT *
                FROM {schema_name}.history
                WHERE DATE_PART('YEAR', date_acquired) = '{year}';
            """

        df = pd.read_sql_query(
            query.format(schema_name=schema_name, table_name=table_name),
            connection,
            parse_dates=['release_date', 'date_acquired', 'created',
                         'posted', 'only_date']
        ).rename(columns={
            'game_id': 'gameid',
            'achievement_id': 'achievementid',
            'player_id': 'playerid'
        })

        return df
