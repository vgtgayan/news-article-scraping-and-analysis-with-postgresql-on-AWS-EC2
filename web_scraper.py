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
        # Extract the base url
        base_url = re.search(r'(https:\/\/[\w.]+\.com)\/[\/\w-]+\.html$', url)
        self.base_url = base_url.group(1) if base_url is not None else ""
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
            return re.search(r'(\w[\w\. \']+\w)', author_name.string).group(1) if (author_name is not None) else ""
        else:
            return ""

    def get_author_profile_url(self) -> str:
        """Return author of an article"""
        author_line = self.doc.find("span", class_="metadata__byline__author")
        if author_line is not None:
            author_name = author_line.find("a")
            return self.base_url + author_name['href'] if (author_name is not None) else ""
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


def test_web_scraper():
    """
    Unit tests for WebScraper class methods
    :return:
    """
    url = "https://edition.cnn.com/2022/07/31/asia/pelosi-visits-hawaii-as-she-heads-on-asia-tour-hnk-intl/index.html"
    web_scraper = WebScraper(url=url)

    # Test get_author_profile_url()
    profile_url = web_scraper.get_author_profile_url()
    assert profile_url == "https://edition.cnn.com/profiles/alex-stambaugh-profile"


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

    for cat in set(categories):
        cat_href = cat.a['href']
        sub_categories = cat.select('li.sc-kGXeez.femHHJ')
        for sub_cat in set(sub_categories):
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
                article_urls = [(f"{url}{t.a['href']}", cat.a.string, sub_cat.a.string)
                                if (t.a is not None) and t.a['href'].endswith(".html")
                                else ""
                                for t in article_tags]
                article_list.extend(article_urls)
    article_list = list(filter(None, article_list))
    article_list = list(set(article_list))
    print("No of: all articles: ", len(article_list))
    return random.choices(article_list, k=max_links)


def test_get_article_urls():
    """
    Testcase for get_article_urls()
    """
    url = "https://edition.cnn.com"
    max_links = 250
    article_urls_ = get_article_urls(url=url, max_links=max_links)
    assert article_urls_[0][0].startswith(url) and article_urls_[0][0].endswith(".html")
    assert len(article_urls_) == max_links


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
    for article_url, cat, sub_cat in article_urls_:
        try:
            article_scraper = WebScraper(url=article_url)
        except requests.exceptions.RequestException as e:
            print(e)
            continue
        article_data = {
            "url": article_url,
            "title": article_scraper.get_title(),
            "paragraphs": article_scraper.get_paragraphs(),
            "author": article_scraper.get_author(),
            "author_profile_url": article_scraper.get_author_profile_url(),
            "posted_date": article_scraper.get_posted_date(),
            "category": cat,
            "sub_category": sub_cat
        }
        article_data_list.append(article_data)
        # Delete WebScraper object
        del article_scraper

    article_df = pd.DataFrame(article_data_list)

    # Author data
    author_data_list = []
    authors = set(article_df['author'].tolist())
    for author in authors:
        if author in [None, ""]:
            continue
        author_data = {
            "url": article_df[article_df['author'] == author]['author_profile_url'].tolist()[0],
            "name": author,
            "articles": article_df[article_df['author'] == author]['url'].tolist()
        }
        author_data["article_count"] = len(author_data["articles"])
        author_data_list.append(author_data)
    author_df = pd.DataFrame(author_data_list)
    # Dropping duplicate profile urls
    author_df.drop_duplicates(subset=['url'], inplace=True)
    # Dropping author_profile_url column since it is included in author data
    article_df = article_df.drop('author_profile_url', axis=1)
    # Check consistency of "author" name between article and author datasets
    # +1 is for '' value is removed in author_df
    assert len(authors) == len(author_df['name'].tolist()) + 1

    # Category data
    category_data_list = []
    categories = set(article_df['category'].tolist())
    for cat in categories:
        sub_categories = set(article_df[article_df['category'] == cat]['sub_category'].tolist())
        for sub_cat in sub_categories:
            cat_data = {
                "category": cat,
                "sub_category": sub_cat,
                "articles": article_df[(article_df['category'] == cat) & (article_df['sub_category'] == sub_cat)][
                    'url'].tolist()
            }
            cat_data["article_count"] = len(cat_data["articles"])
            category_data_list.append(cat_data)
    category_df = pd.DataFrame(category_data_list)

    # Return dataframe with scraped data
    return article_df, author_df, category_df


def clean_dataset(df: pd.DataFrame):
    """
    Clean and validate, scraped data
    :param df: Dirty and unvalidated dataset
    :return: clean and validated dataset
    """
    # Clean data ----------------
    # Convert all to string
    df = df.astype(str)
    # Replace null/empty values with None
    df.replace(to_replace=["", '[]', 'NA', pd.NaT], value=None, inplace=True)

    # Drop empty/Null rows
    df.dropna(inplace=True)

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


def populate_db(
        article_data: pd.DataFrame,
        author_data: pd.DataFrame,
        category_data: pd.DataFrame,
        config_file="db.ini"
):
    """
    Populate data into postgreSQL db server on EC2
    :param config_file: postgresql server configurations
    :param data: data to populate in db
    :return:
    """
    # Convert dataframes to list of tuples in order to insert in db
    article_records = article_data.to_records(index=False)
    article_records = list(article_records)
    author_records = author_data.to_records(index=False)
    author_records = list(author_records)
    category_records = category_data.to_records(index=False)
    category_records = list(category_records)

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

        # # Create custom schema
        # sql = '''CREATE SCHEMA IF NOT EXISTS cnn_news;'''
        # crsr.execute(sql)

        # Author table -----------
        # Delete table if exist
        sql = '''DROP TABLE IF EXISTS cnn_news.author CASCADE;'''
        crsr.execute(sql)

        # create Table
        sql = '''
                CREATE TABLE IF NOT EXISTS cnn_news.author(
                    url VARCHAR(255) UNIQUE NOT NULL,
                    name VARCHAR(255) UNIQUE NOT NULL,
                    articles TEXT,
                    article_count INT,
                    PRIMARY KEY(url)
                );
            '''
        crsr.execute(sql)

        # Populate data
        args_str = ','.join(crsr.mogrify("(%s,%s,%s,%s)", x).decode("utf-8") for x in author_records)
        sql = '''
                    INSERT INTO cnn_news.author
                        (url, name, articles, article_count)
                    VALUES
                '''
        sql = str(sql) + str(args_str)
        # print(sql)
        crsr.execute(sql)

        # Category table -----------
        # Delete table if exist
        sql = '''
                    DROP TABLE IF EXISTS cnn_news.category CASCADE;
                '''
        crsr.execute(sql)

        # create Table
        sql = '''
                    CREATE TABLE IF NOT EXISTS cnn_news.category(
                        category VARCHAR(255) NOT NULL,
                        sub_category VARCHAR(255) NOT NULL,
                        articles TEXT,
                        article_count INT,
                        PRIMARY KEY(category, sub_category)
                    );
                '''
        crsr.execute(sql)

        # Populate data
        args_str = ','.join(crsr.mogrify("(%s,%s,%s,%s)", x).decode("utf-8") for x in category_records)
        sql = '''
                    INSERT INTO cnn_news.category
                        (category, sub_category, articles, article_count)
                    VALUES
                '''
        sql = str(sql) + str(args_str)
        # print(sql)
        crsr.execute(sql)

        # Article table -----------
        # Delete table if exist
        sql = '''
                    DROP TABLE IF EXISTS cnn_news.article CASCADE;
                '''
        crsr.execute(sql)

        # create Table
        sql = '''
                    CREATE TABLE IF NOT EXISTS cnn_news.article(
                        url VARCHAR(255) UNIQUE NOT NULL,
                        title VARCHAR(255) UNIQUE NOT NULL,
                        paragraphs TEXT,
                        author VARCHAR(50),
                        posted_date DATE,
                        category VARCHAR(50),
                        sub_category VARCHAR(50),
                        CONSTRAINT pk_article
                            PRIMARY KEY(url),
                        CONSTRAINT fk_author
                            FOREIGN KEY (author)
                                REFERENCES cnn_news.author(name)
                                ON DELETE SET NULL,
                        CONSTRAINT fk_category
                            FOREIGN KEY(category, sub_category)
                                REFERENCES cnn_news.category(category, sub_category)
                                ON DELETE SET NULL
                    );
                '''
        crsr.execute(sql)

        # Populate data
        args_str = ','.join(crsr.mogrify("(%s,%s,%s,%s,%s,%s,%s)", x).decode("utf-8") for x in article_records)
        sql = '''
                    INSERT INTO cnn_news.article
                        (url, title, paragraphs, author, posted_date, category, sub_category)
                    VALUES
                '''
        sql = str(sql) + str(args_str)
        # print(sql)
        crsr.execute(sql)

        # Commit all changes
        connection.commit()

        # # Read table data from db
        # sql = '''
        #     SELECT url, title, author, posted_date FROM article;
        # '''
        # crsr.execute(sql)
        # print("Read back populated data \n")
        # print(crsr.fetchall())

        # Close the cursor
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
    max_articles_ = 500  # No: of articles to be scraped
    config_file_ = "db.ini"  # File with remote database configurations

    # Scrape news articles
    scraped_article_df, scraped_author_df, scraped_category_df = scrape_news_articles(url=url_,
                                                                                      max_articles=max_articles_)
    # Clean and validate scraped data
    cleaned_article_df = clean_dataset(df=scraped_article_df)
    cleaned_author_df = clean_dataset(df=scraped_author_df)
    cleaned_category_df = clean_dataset(df=scraped_category_df)
    # Store data in database
    populate_db(
        article_data=cleaned_article_df,
        author_data=cleaned_author_df,
        category_data=cleaned_category_df,
        config_file=config_file_
    )
