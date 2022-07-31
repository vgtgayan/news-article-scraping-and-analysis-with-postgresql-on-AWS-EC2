import requests
from bs4 import BeautifulSoup
import re

from typing import List


class WebScraper:
    """
        Web scraper for news articles(CNN)
    """

    def __init__(self, url: str):
        self.url = url
        self.result = requests.get(self.url)
        self.doc = BeautifulSoup(self.result.text, "html.parser")

    def get_title(self) -> str:
        """
            Returns title of the article
        """
        result = self.doc.find("h1", class_="pg-headline")
        return result.string

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
        author_name = author_line.find("a")
        return author_name.string

    def get_posted_date(self) -> str:
        """Return posted date of an article"""
        time_tag = self.doc.find("p", class_="update-time")
        time_str = time_tag.contents[0]
        date_ = re.search(r'(\w+ \d+, \d{4})', time_str)
        return date_.group(0)


def get_article_urls(url="https://edition.cnn.com", max_links=250):
    """
    Retrieve article urls(links) from CNN website
    :param url: Base url of the news provider
    :param max_links: maximum no: of links to obtain
    :return: list of article urls
    """
    article_list = []
    response = requests.get(url)
    tag = BeautifulSoup(response.text, "html.parser")
    # categories = tag.find_all("a", class_="sc-fjdhpX")
    categories = tag.select('li.sc-kAzzGY.fDMFSn')
    # print(categories[-1].find_all("a"))
    # category_hrefs = [[f"{cat.a['href']}{sub_cat.string}" for sub_cat in cat.find_all("a", class_="tqJTs")]
    #                   for cat in categories]
    for cat in categories:
        cat_href = cat.a['href']
        print(f"Category - {cat_href}: \n")
        sub_categories = cat.select('li.sc-kGXeez.femHHJ')
        # print(sub_categories)
        for sub_cat in sub_categories:
            sub_cat_href = sub_cat.a['href']
            partial_url = f"{url}{sub_cat_href}"
            print(partial_url)
            # Get list of articles within sub category
            try:
                sub_cat_response = requests.get(partial_url)
            except:
                print("Error: Please check the validity of the url:")
                print(partial_url)
                continue
            sub_cat_tag = BeautifulSoup(sub_cat_response.text, "html.parser")
            article_tags = sub_cat_tag.find_all("h3", class_="cd__headline")
            if len(article_tags) != 0:
                article_urls = [f"{url}{t.a['href']}" if (t.a is not None) and t.a['href'].endswith(".html") else "" for
                                t in article_tags]
                article_list.extend(article_urls)
                # print(article_urls)

    return article_list


if __name__ == "__main__":
    """
        Standalone testing of WebScraper
    """
    url = "https://edition.cnn.com/2022/07/28/politics/joe-biden-xi-jinping-call/index.html"
    cnn_scraper = WebScraper(url=url)
    print(cnn_scraper.get_posted_date())
