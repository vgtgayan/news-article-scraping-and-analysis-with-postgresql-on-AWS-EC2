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


if __name__ == "__main__":
    """
        Standalone testing of WebScraper
    """
    url = "https://edition.cnn.com/2022/07/28/politics/joe-biden-xi-jinping-call/index.html"
    cnn_scraper = WebScraper(url=url)
    print(cnn_scraper.get_posted_date())
