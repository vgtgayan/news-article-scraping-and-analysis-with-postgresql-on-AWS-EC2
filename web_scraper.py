"""Scrape data from a news provider website and store validated data in a postgreSQL database hosted on AWS EC2 instance

    Classes
    ----------
    WebScraper:
        Web scraper for news articles

    Functions
    ----------
    get_article_urls:
        Retrieve article urls(links) from CNN website
    scrape_news_articles:
        Scrape data from articles from CNN website and store in a data frame
    clean_dataset:
        Clean and validate, scraped data
    populate_db:
        Populate data into postgreSQL db server on EC2

"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import random
from typing import List

# Connecting postgreSQL to Python'
import psycopg2
from db_config import config


class WebScraper:
    """Web scraper for news articles
    ...

    Attributes
    ----------
    url : str
        Valid url of news article

    Methods
    -------
    get_title()
        Returns title of the article
    get_paragraphs()
        Returns list of paragraphs inside the given tag
    get_author()
        Return author of an article
    get_posted_date()
        Return posted date of an article
    """

    def __init__(self, url: str):
        """Get url data and parse with "BeautifulSoup"

        :param url: str
            Valid url of news article
        """
        self.url = url
        try:
            self.result = requests.get(self.url)
        except requests.exceptions.RequestException as e:
            raise
        self.doc = BeautifulSoup(self.result.text, "html.parser")

    def get_title(self) -> str:
        """
            Returns title of the article
        """
        result = self.doc.find("h1", class_="pg-headline")
        return result.string if (result is not None) else ""

    def get_paragraphs(self) -> List:
        """
            Returns list of paragraphs inside the given tag
        """
        paragraphs = self.doc.find_all("div", class_="zn-body__paragraph")
        paragraphs_list = [p.string for p in paragraphs]
        return paragraphs_list

    def get_author(self) -> str:
        """Return author of an article"""
        author_line = self.doc.find("span", class_="metadata__byline__author")
        if author_line is not None:
            author_name = author_line.find("a")
            return author_name.string if (author_name is not None) else ""
        else:
            return ""

    def get_posted_date(self) -> str:
        """Return posted date of an article"""
        time_tag = self.doc.find("p", class_="update-time")
        if time_tag is not None:
            time_str = time_tag.contents[0]
            date_ = re.search(r'(\w+ \d+, \d{4})', time_str)
            return date_.group(0)
        else:
            return ""


def get_article_urls(url="https://edition.cnn.com", max_links=250):
    """
    Retrieve article urls(links) from CNN website
    :param url: Base url of the news provider
    :param max_links: maximum no: of links to obtain
    :return: list of article urls
    """
    article_list = []
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
    tag = BeautifulSoup(response.text, "html.parser")
    categories = tag.select('li.sc-kAzzGY.fDMFSn')

    for cat in categories:
        cat_href = cat.a['href']
        sub_categories = cat.select('li.sc-kGXeez.femHHJ')
        for sub_cat in sub_categories:
            sub_cat_href = sub_cat.a['href']
            partial_url = f"{url}{sub_cat_href}"
            # Get list of articles within sub category
            try:
                sub_cat_response = requests.get(partial_url)
            except requests.exceptions.RequestException as e:
                print(e)
                continue
            sub_cat_tag = BeautifulSoup(sub_cat_response.text, "html.parser")
            article_tags = sub_cat_tag.find_all("h3", class_="cd__headline")
            if len(article_tags) != 0:
                article_urls = [f"{url}{t.a['href']}"
                                if (t.a is not None) and t.a['href'].endswith(".html")
                                else ""
                                for t in article_tags]
                article_list.extend(article_urls)
    article_list = list(filter(None, article_list))
    return random.choices(article_list, k=max_links)


def scrape_news_articles(url="https://edition.cnn.com", max_articles=250):
    """
    Scrape data from articles from CNN website and store in a data frame
    :param url: Base url of the news provider
    :param max_articles: maximum no: of articles to scrape
    :return: pandas dataframe with scraped data
    """
    # Retrieve article urls from news provider
    article_urls_ = get_article_urls(url=url, max_links=max_articles)
    # List to store scraped data
    article_data_list = []
    for article_url in article_urls_:
        try:
            article_scraper = WebScraper(url=article_url)
        except requests.exceptions.RequestException as e:
            print(e)
            continue
        article_data = {
            "title": article_scraper.get_title(),
            "paragraphs": article_scraper.get_paragraphs(),
            "author": article_scraper.get_author(),
            "posted_date": article_scraper.get_posted_date()
        }
        article_data_list.append(article_data)
        # Delete WebScraper object
        del article_scraper
    # Return dataframe with scraped data
    return pd.DataFrame(article_data_list)


def clean_dataset(df: pd.DataFrame):
    """
    Clean and validate, scraped data
    :param df: Dirty and unvalidated dataset
    :return: clean and validated dataset
    """
    # Clean data ----------------
    # Data type conversions
    df["paragraphs"] = df["paragraphs"].astype(str)
    # df["posted_date"] = pd.to_datetime(df["posted_date"])

    # Replace null/empty values with None
    df.replace(to_replace=["", '[]', 'NA', pd.NaT], value=None, inplace=True)

    # Reformat "posted_date" (remove time info)
    # df["posted_date"] = pd.to_datetime(df["posted_date"], format='%Y%m%d')

    # Drop empty/Null rows
    df.dropna(subset=['title', 'paragraphs'], inplace=True)

    # Drop duplicate rows
    df.drop_duplicates(inplace=True)

    # Validate data ----------------
    # Check null values
    print("Null values summary: \n", df.isnull().sum())
    print("\n Non-Null values summary: \n")
    print(df.info())
    # Check for unique values
    print("\n Unique values summary: \n", df.describe(include='all'))

    return df


def populate_db(data: pd.DataFrame, config_file="db.ini"):
    """
    Populate data into postgreSQL db server on EC2
    :param config_file: postgresql server configurations
    :param data: data to populate in db
    :return:
    """
    # Convert dataframe to list of tuples in order to insert in db
    records = data.to_records(index=False)
    records = list(records)

    # Connect to db
    connection = None
    try:
        params = config(filename=config_file)
        print('Connecting to the postgreSQL database ...')
        connection = psycopg2.connect(**params)

        # create a cursor
        crsr = connection.cursor()
        print('PostgreSQL database version: ')
        crsr.execute('SELECT version()')
        db_version = crsr.fetchone()
        print(db_version)

        # Delete table if exist
        sql = '''
            DROP TABLE IF EXISTS article;
        '''
        crsr.execute(sql)

        # create Table
        sql = '''
            CREATE TABLE IF NOT EXISTS article(
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) UNIQUE NOT NULL,
                paragraphs TEXT,
                author VARCHAR(50),
                posted_date DATE
            );
        '''
        crsr.execute(sql)

        # Populate data
        args_str = ','.join(crsr.mogrify("(%s,%s,%s,%s)", x).decode("utf-8") for x in records)
        sql = '''
            INSERT INTO article
                (title, paragraphs, author, posted_date)
            VALUES
        '''
        sql = str(sql) + str(args_str)
        # print(sql)
        crsr.execute(sql)
        connection.commit()

        # Read table data from db
        sql = '''
            SELECT id, title, author, posted_date FROM article;
        '''
        crsr.execute(sql)
        print("Read back populated data \n")
        print(crsr.fetchall())

        crsr.close()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection terminated.')


if __name__ == "__main__":
    """
        Standalone testing of WebScraper
    """
    # Arguments
    url_ = "https://edition.cnn.com"  # Base url of the news provider
    max_articles_ = 350  # No: of articles to be scraped
    config_file_ = "db.ini"  # File with remote database configurations

    # Scrape news articles
    scraped_df = scrape_news_articles(url=url_, max_articles=max_articles_)
    # Clean and validate scraped data
    cleaned_df = clean_dataset(df=scraped_df)
    # Store data in database
    populate_db(data=cleaned_df, config_file=config_file_)
