from pathlib import Path
import json
import requests
from bs4 import BeautifulSoup
import time
import random

from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (NoSuchElementException,
                                        StaleElementReferenceException,
                                        TimeoutException)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import WebDriverException
from constants import get_current_directory, ARTICLES_PATH, CRAWLING_STATUS_PATH


class WrongSeedURLError(Exception):
    """
    Raised when given URL-address is not
    present in config
    """


class Config:
    """
    Unpacks and validates configurations
    """

    def __init__(self, path_to_config: Path) -> None:
        """
        Initializes an instance of the Config class
        """
        self.path_to_config = path_to_config
        self.config = self._extract_config_content()

        self._seed_urls = self.config['seed_urls']
        self._num_articles = self.config['num_articles']
        self._headers = self.config['headers']
        self._encoding = self.config['encoding']
        self._timeout = self.config['timeout']
        self._should_verify_certificate = self.config['should_verify_certificate']
        self._headless_mode = self.config['headless_mode']

        self._apikey = self.config['apikey']
        self._params = self.config['params']

    def _extract_config_content(self) -> dict:
        """
        Returns config values
        """
        with open(self.path_to_config, 'r', encoding='utf-8') as path:
            config_dict = json.load(path)
        return config_dict

    def get_seed_urls(self) -> list[str]:
        """
        Retrieve seed urls
        """
        return self._seed_urls

    def get_num_articles(self) -> int:
        """
        Retrieve number of articles to scrape
        from each source
        """
        return self._num_articles

    def get_headers(self) -> dict[str, str]:
        """
        Retrieve headers to use during requesting
        """
        return self._headers

    def get_encoding(self) -> str:
        """
        Retrieve encoding to use during parsing
        """
        return self._encoding

    def get_timeout(self) -> int:
        """
        Retrieve number of seconds to wait for response
        """
        return self._timeout

    def get_verify_certificate(self) -> bool:
        """
        Retrieve whether to verify certificate
        """
        return self._should_verify_certificate

    def get_params(self) -> dict[str, str]:
        return self._params


def make_request(url: str, config: Config) -> requests.models.Response:
    """
    Delivers a response from a request
    with given configuration
    :param url: current URL
    :param config: Crawler's configurations
    :return: result of a request as a Response class instance
    """
    sleep_time = random.randint(1, 20)
    time.sleep(sleep_time)

    if '//iz' in url:
        params = config.get_params()
        params['url'] = url
        response = requests.get('https://api.zenrows.com/v1/',
                                # headers=config.get_headers(),
                                # timeout=config.get_timeout(),
                                params=params)

    else:
        response = requests.get(url,
                                headers=config.get_headers(),
                                timeout=config.get_timeout())
    response.encoding = config.get_encoding()

    return response


def make_request_selenium(url: str,
                          driver,
                          num_scrolls=0,
                          mode='pressing a button') -> str:
    """
    Delivers a response from a request
    with given parameters with the help of
    Selenium library
    :param url: current URL
    :param driver: Chrome driver instance
    :param num_scrolls: number of scrolls for dynamic page parsing
    :param mode: parsing mode
    :return: result of a request as a string with page's HTML code
    """
    driver.set_page_load_timeout(40)
    driver.get(url)
    count_scrolls = 0

    if mode == 'pressing a button':
        return driver.page_source
    else:
        while count_scrolls <= num_scrolls:

            if 'mk.ru' in url:
                if count_scrolls in [0, 1, 2]:
                    driver.execute_script("window.scrollTo(0,document.body.scrollHeight - 1650);")

                else:
                    try:
                        load_more_button = WebDriverWait(driver, 20).until(
                            ec.visibility_of_element_located((By.CLASS_NAME, "LoadMoreBtn_wrapper__A7ItH   "))
                        )
                        driver.execute_script("arguments[0].scrollIntoView();", load_more_button)
                        driver.execute_script("arguments[0].click();", load_more_button)

                        time.sleep(10)
                        # driver.execute_script("window.scrollTo(0,document.body.scrollHeight - 1700);")

                    except (NoSuchElementException, TimeoutException):
                       print('Crawling error')

                time.sleep(10)
                count_scrolls += 1

            elif 'iz' in url:
                driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
                time.sleep(10)
                count_scrolls += 1

    return driver.page_source


class Crawler:
    """
    Crawler implementation
    """

    def __init__(self, config: Config) -> None:
        """
        Initializes an instance of the Crawler class
        """
        self.config = config
        self.urls = []
        self._seed_urls = self.config.get_seed_urls()

    @staticmethod
    def _extract_url(seed_url: str, article_bs: BeautifulSoup) -> str:
        """
        Finds and retrieves URL from HTML-string
        with feed page's code
        :param seed_url: URL to the page with a feed
        :param article_bs: HTML-string of code with a link
        to the article
        :return: a string with a link to the article
        """
        match seed_url:

            case 'https://iz.ru/rubric/politika':
                if article_bs.find('div').get('data-type') == 'news':
                    url = 'https://iz.ru' + article_bs.find(
                        'a', {'class': 'node__cart__item__inside'}).get('href')
                else:
                    return ''

            case 'https://iz.ru/rubric/armiia':
                if article_bs.find('div').get('data-type') == 'news':
                    url = 'https://iz.ru' + article_bs.find(
                        'a', {'class': 'node__cart__item__inside'}).get('href')
                else:
                    return ''

            case 'https://iz.ru/rubric/mir':
                if article_bs.find('div').get('data-type') == 'news':
                    url = 'https://iz.ru' + article_bs.find(
                        'a', {'class': 'node__cart__item__inside'}).get('href')
                else:
                    return ''

            case 'https://rg.ru/tema/mir':
                url = 'https://rg.ru' + article_bs.get('href')

            case 'https://rg.ru/tema/gos':
                url = 'https://rg.ru' + article_bs.get('href')

            case 'https://www.mk.ru/news/2025/4/':
                url = article_bs.get('href')

            case _:
                raise WrongSeedURLError('Entered URL isn\'t present in '
                                        'the "seed_urls" parameter of the config file')

        if isinstance(url, str):
            return url
        return ''

    def find_articles(self) -> None:
        """
        Finds all articles links
        given a seed URL
        """
        num_arts = self.config.get_num_articles()

        for url in self._seed_urls:

            match url:
                case 'https://rg.ru/tema/gos':  # crawling article links from Российская газета feed

                    options = ChromeOptions()
                    options.add_argument('--headless')
                    driver = Chrome(options=options)
                    response = make_request_selenium(url,
                                                     driver,
                                                     num_arts // 50,
                                                     mode='scrolling')

                    main_bs = BeautifulSoup(response, 'lxml')
                    feed_lines_li = main_bs.find_all('div',
                                                      {'class': 'PageRubricContent_listItem__KVIae'})
                    feed_lines = [bs.find('a') for bs in feed_lines_li]
                    feed_lines = feed_lines[:num_arts]


                case 'https://rg.ru/tema/mir':  # crawling article links from Российская газета feed

                    options = ChromeOptions()
                    options.add_argument('--headless')
                    driver = Chrome(options=options)
                    response = make_request_selenium(url,
                                                     driver,
                                                     num_arts // 50,
                                                     mode='scrolling')

                    main_bs = BeautifulSoup(response, 'lxml')
                    feed_lines_li = main_bs.find_all('div',
                                                     {'class': 'PageRubricContent_listItem__KVIae'})
                    feed_lines = [bs.find('a') for bs in feed_lines_li]
                    feed_lines = feed_lines[:num_arts]


                case 'https://iz.ru/rubric/politika':  # crawling article links from Известия feed

                    if not CRAWLING_STATUS_PATH.exists():
                        responses_list = []
                        num_crawled = 0
                    else:
                        with open(CRAWLING_STATUS_PATH, 'r', encoding='utf-8') as fff:
                            crawling_status = json.load(fff)
                        responses_list = crawling_status['crawled_pages']
                        num_crawled = crawling_status['crawled_pages_num']

                    for n_iter in range(num_crawled + 1, num_arts // 16 + 1):
                        response = make_request(f'https://iz.ru/rubric/politika?page={n_iter}', self.config)
                        responses_list.append(response.text)

                    feed_lines = []
                    for resp in responses_list:
                        main_bs = BeautifulSoup(resp, 'lxml')
                        feed_lines_bs = main_bs.find('div', {'class': 'four-col-news'})
                        feed_lines_sub = feed_lines_bs.find_all('div',
                                {'class': 'node__cart__item show_views_and_comments'})
                        feed_lines.extend(feed_lines_sub)
                    feed_lines = feed_lines[:num_arts]


                case 'https://iz.ru/rubric/armiia':  # crawling article links from Известия feed

                    if not CRAWLING_STATUS_PATH.exists():
                        responses_list = []
                        num_crawled = 0
                    else:
                        with open(CRAWLING_STATUS_PATH, 'r', encoding='utf-8') as fff:
                            crawling_status = json.load(fff)
                        responses_list = crawling_status['crawled_pages']
                        num_crawled = crawling_status['crawled_pages_num']

                    for n_iter in range(num_crawled + 1, num_arts // 16 + 1):
                        response = make_request(f'https://iz.ru/rubric/armiia?page={n_iter}', self.config)
                        responses_list.append(response.text)

                    feed_lines = []
                    for resp in responses_list:
                        main_bs = BeautifulSoup(resp, 'lxml')
                        feed_lines_sub = main_bs.find_all('div',
                                                          {'class': 'node__cart__item show_views_and_comments'})
                        feed_lines.extend(feed_lines_sub)
                    feed_lines = feed_lines[:num_arts]

                case 'https://iz.ru/rubric/mir':  # crawling article links from Известия feed

                    if not CRAWLING_STATUS_PATH.exists():
                        responses_list = []
                        num_crawled = 0
                    else:
                        with open(CRAWLING_STATUS_PATH, 'r', encoding='utf-8') as fff:
                            crawling_status = json.load(fff)
                        responses_list = crawling_status['crawled_pages']
                        num_crawled = crawling_status['crawled_pages_num']

                    for n_iter in range(num_crawled + 1, num_arts // 16 + 1):
                        response = make_request(f'https://iz.ru/rubric/mir?page={n_iter}', self.config)
                        responses_list.append(response.text)

                    feed_lines = []
                    for resp in responses_list:
                        main_bs = BeautifulSoup(resp, 'lxml')
                        feed_lines_sub = main_bs.find_all('div',
                                                          {'class': 'node__cart__item show_views_and_comments'})
                        feed_lines.extend(feed_lines_sub)
                    feed_lines = feed_lines[:num_arts]


                case 'https://www.mk.ru/news/2025/4/':  # crawling article links from Московский комсомолец feed
                    responses_list = []
                    response = make_request(f'https://www.mk.ru/news/',
                                            self.config).text
                    responses_list.append(response)
                    for n_iter in range(24, 0, -1):
                        response = make_request(f'https://www.mk.ru/news/2025/4/{n_iter}/',
                                                self.config).text
                        responses_list.append(response)
                        if len(responses_list) > num_arts // 50:
                            break

                    feed_lines = []
                    for resp in responses_list:
                        main_bs = BeautifulSoup(resp, 'lxml')
                        feed_lines_sub = main_bs.find_all('a',
                                                          {'class': 'news-listing__item-link'})
                        feed_lines.extend(feed_lines_sub)

                    feed_lines = feed_lines[:num_arts]

                case _:
                    raise WrongSeedURLError('Entered URL isn\'t present in '
                                            'the "seed_urls" parameter of the config file')


            for line in feed_lines:
                if link := self._extract_url(url, line):
                    if link not in self.urls:

                        match url:
                            case 'https://www.mk.ru/news/2025/4/':
                                if 'mk.ru/politics' in link:
                                    self.urls.append(link)
                            case _:
                                self.urls.append(link)

    def get_search_urls(self) -> list:
        """
        Returns seed_urls param
        :return: list of seed URLs
        """
        return self._seed_urls


class Parser:
    """
    Parser implementation
    """

    def __init__(self, full_url: str, config: Config) -> None:
        """
        Initializes an instance of the Parser class
        """
        self.full_url = full_url
        self.config = config

    def _extract_text(self, article_soup: BeautifulSoup) -> str:
        """
        Given an HTML-string of page's code,
        extracts the text of the article
        :param article_soup: HTML-string of page's code
        :return: string with article content
        """

        if '//rg' in self.full_url:
            article = article_soup.find('div', {
                'class': 'PageArticleContent_content__mdxza'
            })
            try:
                article_list = article.find_all('p')
            except AttributeError:
                try:
                    article = article_soup.find('div', {
                        'class': 'PageArticleContent_content__mdxza _inv'
                    })
                    article_list = article.find_all('p')
                except AttributeError:
                    article_list = []
            try:
                article_lead = article_soup.find('div', {
                    'class': 'PageArticleContent_lead__l9TkG commonArticle_zoom__SDMjc'
                })
            except AttributeError:
                article_lead = ''
            article_list.insert(0, article_lead)
            try:
                article_title = article_soup.find('h1', {'class': 'PageArticleCommonTitle_title__fUDQW'})
            except AttributeError:
                article_title = ''
            article_list.insert(0, article_title)

        elif '//iz' in self.full_url:
            try:
                article = article_soup.find('div', {'itemprop': 'articleBody'})
                article_list = article.find_all('p')
            except AttributeError:
                article_list = []
            try:
                article_title = article_soup.find('h1', {'itemprop': 'headline'}).find('span')
            except AttributeError:
                article_title = ''
            article_list.insert(0, article_title)

        elif 'mk.ru' in self.full_url:
            article = article_soup.find('main', {'class': 'article'})
            try:
                article_body = article.find('div', {'class': 'article__body'})
                article_list = article_body.find_all('p')
            except AttributeError:
                article_list = []
            try:
                article_lead = article.find('p', {'class': 'article__subtitle'})
            except AttributeError:
                article_lead = ''
            article_list.insert(0, article_lead)
            try:
                article_title = article.find('h1', {'class': 'article__title'})
            except AttributeError:
                article_title = ''
            article_list.insert(0, article_title)

        else:
            raise WrongSeedURLError('Entered URL isn\'t present in '
                                    'the "seed_urls" parameter of the config file')

        paragraphs = [par.text for par in article_list if par]
        return '\n'.join(paragraphs)

    def _write_to_txt(self, text: str, article_num: int) -> None:
        """
        Creates and fills article file (.txt)
        in a corresponding directory
        :param text: text of the article
        :param article_num: the order of the current article
        in articles links list
        """

        curr_dir = get_current_directory(self.full_url, ARTICLES_PATH)

        if not curr_dir.exists():
            curr_dir.mkdir(parents=True)
        txt_file_link = curr_dir / f'{article_num}.txt'

        with open(txt_file_link, 'w', encoding='utf-8') as file:
            file.write(text)

    def parse_and_save_file(self, article_num: int) -> None:
        """
        Parses each article and saves
        in a .txt file
        :param article_num: the order of the current article
        in articles links list
        """
        options = ChromeOptions()
        options.add_argument('--headless')
        driver = Chrome(options=options)

        # try:
        #     response = make_request_selenium(self.full_url, driver)
        # except WebDriverException:
        #     print('Ошибка драйвера')
        #     return
        time.sleep(10)
        response = make_request(self.full_url, self.config).text

        b_s = BeautifulSoup(response, 'lxml')
        text = self._extract_text(b_s)

        self._write_to_txt(text, article_num)
