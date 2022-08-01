# news-article-scraping-and-analysis-with-postgresql-on-AWS-EC2
## Scrape data from a news provider website and store validated data in a postgreSQL database hosted on AWS EC2 instance

### Run command:
  >python3 web_scraper.py

### Main script: web_scraper.py
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

### Dependencies:
  db_config.py 
  
  db.ini (not included in repo) - File with db credentials
  
    Template:
    
      [postgresql]
      host = <db-server-public-IPv4>
      database = <database-name>
      user = <db-server-login-username>
      password = <db-server-login-password>

### Experimental files:
  scrape_news.ipynb
  
  connect_db.ipynb
