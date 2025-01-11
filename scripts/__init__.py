from typing import Optional, List, Any
from fuzzywuzzy import process
from bs4 import BeautifulSoup
from datetime import datetime
import urllib.request
import csv
from utils.fetcher import Fetcher


class ExophaseAPI(Fetcher):
    def __init__(self):
        super().__init__()
        self.url = 'https://www.exophase.com'
        self.api = 'https://api.exophase.com'

    @staticmethod
    def _format_date(date: str) -> Optional[str]:
        """
        Converts a string representing a date into a datetime object

        Args:
            date (str): A string representing a date, either in the format "Month day, Year" 
                        or "day Month Year"

        Returns:
            Optional[str]: A datetime object if the date is in a valid format
        """
        if date in {'To be announced', 'Yesterday', 'Tomorrow'}:
            return None
        
        try:
            # Date example: October 25, 2024
            return datetime.strptime(date, "%B %d, %Y")
        except ValueError:
            # Date example: 25 October 2024
            return datetime.strptime(date, "%d %B %Y")

    @staticmethod
    def _find_best_match(target: str, candidates: List[Optional[str]],
                         filename: str) -> Optional[str]:
        """
        Finds the best match for a target string from a list of candidates and records the results in a CSV file

        Args:
            target (str): The string to match against the candidates
            candidates (List[Optional[str]]): A list of candidate strings to compare with the target
            filename (str): The name of the CSV file where match results will be recorded

        Returns:
            Optional[str]: The best match from the candidates if the match coefficient is above a threshold
        """
        best_match, coeff = None, None
        result = process.extractOne(target, candidates)
        if result:
            best_match, coeff = result[0], result[1]
            
        # Record all matches with their coefficients
        # in a CSV file for further deviation plotting
        with open(f'./resources/{filename}', mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            if file.tell() == 0:
                writer.writerow(['target', 'candidate', 'coeff'])
            writer.writerow([target, best_match, coeff])
        
        # The value of 90 was obtained empirically. At this threshold,
        # there is still a match between the target and candidate strings
        if coeff is not None and coeff <= 90:
            return None
        return best_match

    @staticmethod
    def _construct_query(title: str) -> str:
        """
        Encodes a string to be safely used in a URL by percent-encoding special characters

        Args:
            title (str): The string (typically a title) to be URL-encoded

        Returns:
            str: The URL-encoded version of the input string
        """
        # Function encodes a string for use in a URL
        # by percent-encoding special characters
        return urllib.parse.quote(title)

    @staticmethod
    def _request(url: str) -> str:
        """
        Makes an HTTP request to the provided URL and returns the HTML content as a decoded string

        Args:
            url (str): The URL to which the HTTP request will be made

        Returns:
            str: The HTML content of the page retrieved from the URL, decoded as a UTF-8 string
        """
        request = urllib.request.Request(url)
        request.add_header('User-Agent',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0')
        request.add_header('Accept',
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8')
        request.add_header('Accept-Language',
            'en-US,en;q=0.5')
        return urllib.request.urlopen(request).read().decode('utf-8')

    def get_achievements(self, soup: BeautifulSoup, gameid: int) -> List[Optional[List[Any]]]:
        """
        Extracts achievements information from a game's details page

        Args:
            soup (BeautifulSoup): A BeautifulSoup object representing the HTML content of the game details page
            gameid (int): The unique identifier of the game for generating achievement IDs

        Returns:
            List[Optional[List[Any]]]: A list of achievements
        """
        table = soup.find('div', id='awards').find_all('li')
        achievements = []
        
        for achievement in table:
            achievementid = f"{gameid}_{achievement.get('id')}"
            title = achievement.find('div',
                class_='text-medium award-title hidden-toggle fw-bolder').find('a').text.strip()
            description = achievement.find('div',
                class_='award-description hidden-toggle').find('p').text.strip()
            
            try:
                points = achievement.find('div',
                    class_='col-12 col-lg mt-3 mt-lg-0 award-points text-center').find('span').text.strip()
            except AttributeError:
                # This exception is triggered if the points foran Xbox game
                # achievement are missing or if we are parsing data for PlayStation games
                try:
                    points = achievement.find('div',
                        class_='col-12 col-lg mt-3 mt-lg-0 award-points text-center').find('i').get('class')[-1]
                    points = points.split('-')[-1].capitalize()
                except AttributeError:
                    # Points for achievement acquisition are missing
                    points = None
            
            achievements.append([achievementid, gameid, title, description, points])
        return achievements

    def get_details(self, soup: BeautifulSoup) -> List[Any]:
        """
        Extracts details (developers, publishers, genres, supported languages, release date)\\
        from the provided BeautifulSoup object representing the game details page

        Args:
            soup (BeautifulSoup): A BeautifulSoup object representing the HTML content of the game details page
        
        Returns:
            List[Any]: A list containing extracted information
        """
        # Default values for empty data
        developers, publishers, genres = None, None, None
        supported_languages, release = None, None
        
        try:
            attributes = soup.find('dl', class_='details').find_all('dt')
        except AttributeError:
            try:
                attributes = soup.find('dl', class_='game-info').find_all('dt')
            except AttributeError:
                attributes = []
        
        for attribute in attributes:
            info = attribute.find_next('dd')
            
            # Check for specific attribute labels and extract data accordingly
            if attribute.text in {'Developer:', 'Developer', 'Developers'}:
                developers = [developer.text.strip() for developer in info.find_all('a')]
            elif attribute.text in {'Publisher:', 'Publisher', 'Publishers'}:
                publishers = [publisher.text.strip() for publisher in info.find_all('a')]
            elif attribute.text in {'Genre:', 'Genre', 'Genres'}:
                genres = [genre.text.strip() for genre in info.find_all('a')]
            elif attribute.text == 'Languages:':
                supported_languages = [language.text.strip() for language in info.find_all('a')]
            elif attribute.text in {'Release Date:', 'Release'}:
                release = self._format_date(info.text.strip())
        
        return [developers, publishers, genres, supported_languages, release]

    def last_page(self, url: str) -> int:
        """
            Retrieves the last page number from a paginated HTML page

            Args:
                url (str): The URL of the page to parse
            
            Returns:
                int: The last page number extracted from the pagination
        """
        try:
            html_content = self._request(url)
            soup = BeautifulSoup(html_content, 'html.parser')
            
            pagination = soup.find('ul', class_='p-4 pagination justify-content-center')
            
            return int(pagination.find_all('li')[-2].text.strip())
        except Exception as e:
            raise Exception(f'Failed to retrieve the last page number. Error: {str(e).strip()}')
