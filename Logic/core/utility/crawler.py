from typing import List

import requests
from requests import get
from bs4 import BeautifulSoup
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Lock
import json


class IMDbCrawler:
    """
    put your own user agent in the headers
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
    }
    top_250_URL = 'https://www.imdb.com/chart/top/'

    def __init__(self, crawling_threshold=1000):
        """
        Initialize the crawler

        Parameters
        ----------
        crawling_threshold: int
            The number of pages to crawl
        """
        self.crawling_threshold = crawling_threshold
        self.not_crawled = []
        self.crawled = []
        self.added_ids = []
        self.add_list_lock = None
        self.add_queue_lock = None

    def get_id_from_URL(self, URL):
        """
        Get the id from the URL of the site. The id is what comes exactly after title.
        for example the id for the movie https://www.imdb.com/title/tt0111161/?ref_=chttp_t_1 is tt0111161.

        Parameters
        ----------
        URL: str
            The URL of the site
        Returns
        ----------
        str
            The id of the site
        """
        return URL.split('/')[4]

    def write_to_file_as_json(self):
        """
        Save the crawled files into json
        """
        with open("IMDB_crawled.json",'w') as f :
            json.dump(self.crawled,f)
        with open("IMDB_not_crawled.json",'w') as f:
            json.dump(self.not_crawled,f)

    def read_from_file_as_json(self):
        """
        Read the crawled files from json
        """
        try:
            with open('IMDB_crawled.json', 'r') as f:
                self.crawled = json.load(f)
        except:
            print("can't read crawled")
        try:
            with open('IMDB_not_crawled.json', 'r') as f:
                self.not_crawled = json.load(f)
        except:
            print("can't read not crawled")
        self.added_ids = None

    def crawl(self, URL):
        """
        Make a get request to the URL and return the response

        Parameters
        ----------
        URL: str
            The URL of the site
        Returns
        ----------
        requests.models.Response
            The response of the get request
        """
        return get(url=URL, headers=self.headers)

    def extract_top_250(self):
        """
        Extract the top 250 movies from the top 250 page and use them as seed for the crawler to start crawling.
        """
        r = self.crawl(self.top_250_URL)
        soup = BeautifulSoup(r.content, "html.parser")
        urls = self.get_next_links(soup)
        self.not_crawled = urls
        ids = []
        for url in urls:
            ids.append(self.get_id_from_URL(url))
        self.added_ids = ids

    def get_next_links(self,soup,id=None):
        link_elements = soup.select("a[href]")
        urls = []
        for link_element in link_elements:
            url = link_element['href']
            if id :
                if url.startswith("/title") and not(id in url):
                    split = url.split('/')
                    urls.append('/' + split[1] + '/' + split[2])
            else:
                if url.startswith("/title") :
                    split = url.split('/')
                    urls.append('/' + split[1] + '/' + split[2])
        temp_urls = set(urls)
        urls = []
        for url in temp_urls:
            urls.append("https://www.imdb.com" + url)
        return urls
    def get_imdb_instance(self):
        return {
            'id': None,  # str
            'title': None,  # str
            'first_page_summary': None,  # str
            'release_year': None,  # str
            'mpaa': None,  # str
            'budget': None,  # str
            'gross_worldwide': None,  # str
            'rating': None,  # str
            'directors': None,  # List[str]
            'writers': None,  # List[str]
            'stars': None,  # List[str]
            'related_links': None,  # List[str]
            'genres': None,  # List[str]
            'languages': None,  # List[str]
            'countries_of_origin': None,  # List[str]
            'summaries': None,  # List[str]
            'synopsis': None,  # List[str]
            'reviews': None,  # List[List[str]]
        }

    def start_crawling(self):
        """
        Start crawling the movies until the crawling threshold is reached.

        ThreadPoolExecutor is used to make the crawler faster by using multiple threads to crawl the pages.
        You are free to use it or not. If used, not to forget safe access to the shared resources.
        """

        if len(self.not_crawled) == 0:
            self.extract_top_250()
        futures = []
        crawled_counter = 0
        lock = Lock()
        with ThreadPoolExecutor(max_workers=20) as executor:
            while len(self.crawled) <= self.crawling_threshold:
                URL = self.not_crawled.pop(0)
                futures.append(executor.submit(self.crawl_page_info, URL, lock))
                if len(self.not_crawled ) == 0:
                    wait(futures)
                    futures = []
            if len(self.crawled) > self.crawling_threshold:
                print("stop")
                executor.shutdown(wait=False)
    def crawl_page_info(self, URL,lock):
        """
        Main Logic of the crawler. It crawls the page and extracts the information of the movie.
        Use related links of a movie to crawl more movies.
        
        Parameters
        ----------
        URL: str
            The URL of the site
        """
        print("new iteration")
        id = self.get_id_from_URL(URL)
        r = self.crawl(URL)
        soup = BeautifulSoup(r.content, "html.parser")
        urls = self.get_next_links(soup,id)
        for url in urls:
            if (url not in self.not_crawled) and (url not in self.crawled) :
                lock.acquire()
                self.not_crawled.append(url)
                lock.release()
        movie = self.get_imdb_instance()
        movie['id'] = id
        self.extract_movie_info(res=r,movie=movie,URL=URL)
        lock.acquire()
        self.crawled.append(movie)
        lock.release()
        lock.acquire()
        if len(self.crawled) % 50 == 0:
            print("write")
            self.write_to_file_as_json()
        lock.release()
        print("not crawled: " , len(self.not_crawled))
        print("crawled: ",len(self.crawled))

    def extract_movie_info(self, res, movie, URL):
        """
        Extract the information of the movie from the response and save it in the movie instance.

        Parameters
        ----------
        res: requests.models.Response
            The response of the get request
        movie: dict
            The instance of the movie
        URL: str
            The URL of the site
        """
        soup = BeautifulSoup(res.content, "html.parser")
        movie['title'] = IMDbCrawler.get_title(soup)
        movie['first_page_summary'] = IMDbCrawler.get_first_page_summary(soup)
        movie['release_year'] = str(IMDbCrawler.get_release_year(soup))
        movie['mpaa'] = IMDbCrawler.get_mpaa(soup)
        movie['budget'] = str(IMDbCrawler.get_budget(soup))
        movie['gross_worldwide'] = str(IMDbCrawler.get_gross_worldwide(soup))
        movie['directors'] = IMDbCrawler.get_director(soup)
        movie['writers'] = IMDbCrawler.get_writers(soup)
        movie['stars'] = IMDbCrawler.get_stars(soup)
        movie['related_links'] = self.get_related_links(soup)
        movie['genres'] = IMDbCrawler.get_genres(soup)
        movie['languages'] = IMDbCrawler.get_languages(soup)
        movie['countries_of_origin'] = IMDbCrawler.get_countries_of_origin(soup)
        movie['rating'] = str(IMDbCrawler.get_rating(soup))
        summary_url = IMDbCrawler.get_summary_link(URL)
        summary_soup = BeautifulSoup(self.crawl(summary_url).content, 'html.parser')
        movie['summaries'] = IMDbCrawler.get_summary(summary_soup)
        movie['synopsis'] = IMDbCrawler.get_synopsis(summary_soup)
        review_url = IMDbCrawler.get_review_link(URL)
        review_soup = BeautifulSoup(self.crawl(review_url).content, 'html.parser')
        movie['reviews'] = self.get_reviews_with_scores(review_soup)

    def get_summary_link(url):
        """
        Get the link to the summary page of the movie
        Example:
        https://www.imdb.com/title/tt0111161/ is the page
        https://www.imdb.com/title/tt0111161/plotsummary is the summary page

        Parameters
        ----------
        url: str
            The URL of the site
        Returns
        ----------
        str
            The URL of the summary page
        """
        try:
            return url + "/plotsummary"
        except:
            print("failed to get summary link")
            return None

    def get_review_link(url:str):
        """
        Get the link to the review page of the movie
        Example:
        https://www.imdb.com/title/tt0111161/ is the page
        https://www.imdb.com/title/tt0111161/reviews is the review page
        """
        try:
            return url + "/reviews"
        except:
            print("failed to get review link")
            return None

    def get_title(soup):
        """
        Get the title of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        str
            The title of the movie

        """
        try:
            return soup.select_one('.hero__primary-text').string
        except:
            print("failed to get title")
            return None

    def get_first_page_summary(soup):
        """
        Get the first page summary of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        str
            The first page summary of the movie
        """
        try:
            return json.loads(soup.find('script', type='application/json').string)['props']['pageProps']['aboveTheFoldData']['plot']['plotText']['plainText']
        except:
            print("failed to get first page summary")
            return None

    def get_director(soup):
        """
        Get the directors of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        List[str]
            The directors of the movie
        """
        try:
            credits_raw = json.loads(soup.find('script', type='application/json').string)['props']['pageProps']['aboveTheFoldData']['principalCredits']
            directors = []
            for credit in credits_raw:
                if credit['category']['text'] == 'Directors' or credit['category']['text'] == 'Director':
                    for director in credit['credits']:
                        directors.append(director['name']['nameText']['text'])
            return directors
        except:
            print("failed to get director")
            return None

    def get_stars(soup):
        """
        Get the stars of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        List[str]
            The stars of the movie
        """
        try:
            credits_raw = json.loads(soup.find('script', type='application/json').string)['props']['pageProps']['aboveTheFoldData']['principalCredits']
            stars = []
            for credit in credits_raw:
                if credit['category']['text'] == 'Stars' or credit['category']['text'] == 'Star':
                    for star in credit['credits']:
                        stars.append(star['name']['nameText']['text'])
            return stars
        except:
            print("failed to get stars")
            return None

    def get_writers(soup):
        """
        Get the writers of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        List[str]
            The writers of the movie
        """
        try:
            credits_raw = json.loads(soup.find('script', type='application/json').string)['props']['pageProps']['aboveTheFoldData']['principalCredits']
            writers = []
            for credit in credits_raw:
                if credit['category']['text'] == 'Writers' or credit['category']['text'] == 'Writer':
                    for writer in credit['credits']:
                        writers.append(writer['name']['nameText']['text'])
            return writers
        except:
            print("failed to get writers")
            return None

    def get_related_links(self,soup):
        """
        Get the related links of the movie from the More like this section of the page from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        List[str]
            The related links of the movie
        """
        try:
            return self.get_next_links(soup)
        except:
            print("failed to get related links")
            return None

    def get_summary(soup):
        """
        Get the summary of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        List[str]
            The summary of the movie
        """
        try:
            summaries_and_synopsis = json.loads(soup.find('script', type='application/json').string)['props']['pageProps']['contentData']['categories']
            summaries = []
            for s in summaries_and_synopsis:
                if s['name'] == "Summaries":
                    summaries_raw = s['section']['items']
                    for sr in summaries_raw:
                        summaries.append(sr['htmlContent'])
            return summaries
        except:
            print("failed to get summary")
            return None

    def get_synopsis(soup):
        """
        Get the synopsis of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        List[str]
            The synopsis of the movie
        """
        try:
            summaries_and_synopsis = json.loads(soup.find('script', type='application/json').string)['props']['pageProps']['contentData']['categories']
            synopsis = []
            for s in summaries_and_synopsis:
                if s['name'] == "Synopsis":
                    synopsis_raw = s['section']['items']
                    for sr in synopsis_raw:
                        synopsis.append(sr['htmlContent'])
            return synopsis
        except:
            print("failed to get synopsis")
            return None

    def get_reviews_with_scores(self,soup):
        """
        Get the reviews of the movie from the soup
        reviews structure: [[review,score]]

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        List[List[str]]
            The reviews of the movie
        """
        try:
            link_elements = soup.select("a[href]")
            urls = []
            for link_element in link_elements:
                url = link_element['href']
                if url.startswith("/review") :
                    split = url.split('/')
                    urls.append('/' + split[1] + '/' + split[2])
            temp_urls = set(urls)
            urls = []
            for url in temp_urls:
                urls.append("https://www.imdb.com" + url)
            urls = urls[:10]
            reviews = List[List[str]]
            for url in  urls:
                review_entry = List[str]
                r = self.crawl(url)
                review_soup = BeautifulSoup(r.content,"html.parser")
                review_data = json.loads(review_soup.find('script', type='application/ld+json').string)
                try:
                    review = review_data['reviewBody']
                except:
                    review = None
                try:
                    score = review_data['reviewRating']['ratingValue']
                except:
                    score = None
                review_entry.append(review)
                review_entry.append(score)
                reviews.append(review_entry)
            return reviews
        except:
            print("failed to get reviews")
            return None

    def get_genres(soup):
        """
        Get the genres of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        List[str]
            The genres of the movie
        """
        try:
            genres_raw = json.loads(soup.find('script', type='application/json').string)['props']['pageProps']['aboveTheFoldData']['genres']['genres']
            genres = []
            for g in genres_raw:
                genres.append(g['text'])
            return genres
        except:
            print("Failed to get generes")
            return None

    def get_rating(soup):
        """
        Get the rating of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        str
            The rating of the movie
        """
        try:
            return json.loads(soup.find('script',type='application/json').string)['props']['pageProps']['aboveTheFoldData']['ratingsSummary']['aggregateRating']
        except:
            print("failed to get rating")
            return None

    def get_mpaa(soup):
        """
        Get the MPAA of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        str
            The MPAA of the movie
        """
        try:
            return json.loads(soup.find('script',type='application/json').string)['props']['pageProps']['aboveTheFoldData']['certificate']['rating']
        except:
            print("failed to get mpaa")
            return None

    def get_release_year(soup):
        """
        Get the release year of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        str
            The release year of the movie
        """
        try:
            return json.loads(soup.find('script',type='application/json').string)['props']['pageProps']['aboveTheFoldData']['releaseYear']['year']
        except:
            print("failed to get release year")
            return None

    def get_languages(soup):
        """
        Get the languages of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        List[str]
            The languages of the movie
        """
        try:
            languages_raw = json.loads(soup.find('script', type='application/json').string)['props']['pageProps']['mainColumnData']['spokenLanguages']['spokenLanguages']
            languages = []
            for l in languages_raw:
                languages.append(l['text'])
            return languages
        except:
            print("failed to get languages")
            return None

    def get_countries_of_origin(soup):
        """
        Get the countries of origin of the movie from the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        List[str]
            The countries of origin of the movie
        """
        try:
            countries_raw = json.loads(soup.find('script', type='application/json').string)['props']['pageProps']['mainColumnData']['countriesOfOrigin']['countries']
            countries = []
            for c in countries_raw:
                countries.append(c['text'])
            return countries
        except:
            print("failed to get countries of origin")
            return None

    def get_budget(soup):
        """
        Get the budget of the movie from box office section of the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        str
            The budget of the movie
        """
        try:
            return json.loads(soup.find('script', type='application/json').string)['props']['pageProps']['mainColumnData']['productionBudget']['budget']['amount']
        except:
            print("failed to get budget")
            return None

    def get_gross_worldwide(soup):
        """
        Get the gross worldwide of the movie from box office section of the soup

        Parameters
        ----------
        soup: BeautifulSoup
            The soup of the page
        Returns
        ----------
        str
            The gross worldwide of the movie
        """
        try:
            return json.loads(soup.find('script', type='application/json').string)['props']['pageProps']['mainColumnData']['worldwideGross']['total']['amount']
        except:
            print("failed to get gross worldwide")
            return None


def main():
    imdb_crawler = IMDbCrawler(crawling_threshold=1000)
    imdb_crawler.read_from_file_as_json()
    imdb_crawler.start_crawling()
    imdb_crawler.write_to_file_as_json()




if __name__ == '__main__':
    main()
