from scrape_full import category_scraper, reframe

''' 
In order to work correctly, the script requires the following:
    1) folder named 'search_history' in the same folder with the scripts
    2) inside the search_history folder you need to create folders with categories names
    exactly as keys in dictionary query. In this case those are: 'waste','foil','parchment','paper plates'
    
   3) csv files containing search keywords for each category, named {caregory}_keywords.csv 
    ('waste_keywords.csv', 'foil_keywords.csv', etc.)

This is an execution script which calls functions from scrape_full.py.

IMPORTANT! This script has been successfully tested, and you can see sample results in the folders.
! If running this script doesn't reproduce results, you may need to update headers in API requests:
    in scrape_full.py -- function amz_price_tracker and amazon_rankings:
    update the cookie variable to cookie value in API request in your browser. That should solve the issue

Running this script will update/create two files within search_history/{category}:
    rankings_{category}_{YYYY-MM-DD}.xls = top 20 brands returned for each keywords search on amazon.ca with date and time stamp.
    SEARCH_HISTORY_{category}_{YYYY-MM-DD HH-MM} = search history with recent and past searched including rankings for each keyword,
    prices, availability, sponsored/organic position, etc.
    

'''

waste_brands = ['Hefty','Glad','Kirkland','Amazon','365 Everyday','Earth Rated', 'BioBag', 'Biosak', 'Bag to Earth']

foil_brands = ['Alcan','Reynolds Wrap','365 Everyday','Glad']

parchment_brands = ['Reynolds','Katbite','Kirkland','Delicasea']

paper_plates_brands = ['Ecosave','Glad','Dixie','Solo','Royal Chinet','Stack Man']

query = {'waste':waste_brands, 
         'foil':foil_brands, 
         'parchment':parchment_brands,
         'paper plates':paper_plates_brands}

for key in query:
    print(f'Working on {key} category...')
    print(f'List of brands: {query[key]}')
    category_scraper(key, query[key])
    reframe(key)