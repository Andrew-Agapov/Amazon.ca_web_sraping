# This repository is a web scraping tool for Amazon Canada:

The purpose of it is to build a tracker which could automatically collect data on SERP (search engine results pages) rankings, including SERP position, produc title, brand, pricing, sponsored/organic status, and availabaility (in stock or not).

For example, we have a set of keywords that matter for "garbage bags" category: "garbage bags", "trash bags", "compostable bags" etc.
We want to build a tracker which automatically collects and stores data about N-top results on the SERP and to be able to track the data daily.

The repository contains two Python files: <b> scrape_full.py </b> contains all functions that are needed for the script to run:
<ul>
<li> function amazon_rankings: sends API requests with a provided keywords list and saves a DataFrame of the results which includes search rankings for every keyword
<li> function amz_price_tracker: scrapes price, stock and product details for the list of provided ASINs
<li> function category_scraper: combines two functions and saves the output into a tracker file with details. Every time it is called, it appends data to the latest saved file, so that all historical data is stored in one Excel file
<li> function reframe: this function takes the output file and creates a shorted dashboard with top-20 rankings for each category.
</ul>
Please read detailed documentation inside the script.

<b> all_categories_scraping.py </b> -- this script executes the web scraping tool for the list of required categories and keywords, and saves the results.

You can copy all of the repository to your local drive and try running the script.

Please note! If you get "na" or "TITLE_ERR" data, that means you need to change the headers in API request (this means that requests are bounced back by Amazon website)

In order to overcome it, check your cookie header in DevTools when accessing amazon.ca, and change variable 'cookie' inside functions amz_price_tracker and amazon_rankings

Libraries used:
- requests
- pandas
- time
- timer_convert
- datetime
- glob
- bs4
- numpy

Let me know if you have any questions or suggestions!
