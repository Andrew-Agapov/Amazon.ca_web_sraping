import pandas as pd
import requests
import time
from timer_convert import time_convert
from datetime import datetime
from glob import glob
from bs4 import BeautifulSoup
from time import sleep
import numpy as np

def amz_price_tracker(df, sleep_step=4):
    """ 
    This functions does product details scraping by ASIN.
    df (mandatory): Pandas DataFrame containing columns 'ASIN' and 'URL' (which corresponds to ASIN)
    sleep_step (optional): int, 8 by default. Used to generate random sleep time before next get request within a cycle
    
    
    """
    
    url_list = df['URL'] 
    now = datetime.now().strftime('%Y-%m-%d %H:%M') #setting todays date&time
    tracker_log = pd.DataFrame() #empty tracker log to append the scraping results
    progress_counter = 0
    
    cookie = 'ubid-acbca=131-8811406-8397347; s_cc=true; s_vnum=2072121012859&vn=1; s_ppv=79; s_nr=1640121197991-New; s_dslv=1640121197994; s_sq=acsca-prod=%26pid%3DG508510%26pidt%3D1%26oid%3Dhttps%253A%252F%252Fwww.amazon.ca%252Fgp%252Fhelp%252Fcustomer%252Fcontact-us%26ot%3DA; JSESSIONID=8142230128FC43DD81EA6E44BCBAC765; csd-key=eyJ3YXNtVGVzdGVkIjp0cnVlLCJ3YXNtQ29tcGF0aWJsZSI6dHJ1ZSwid2ViQ3J5cHRvVGVzdGVkIjpmYWxzZSwidiI6MSwia2lkIjoiNWM2ODViIiwia2V5IjoiRHdoZEhMNi9MMUJnUHd4SjFiUi9Cd3M3bHZXU2ZPWHI1eWlGcER4VStqTURxN1dZMzVhOS95UW1LVlNhWlFvK2gyM0JDdVA2eFdQSWtUUDdHelhaeUlOVERpUTc1YUlmb2FKamxlR0pSaU1Jb3ArVGpwY1pLdWFxai82WG5adVk5SWQ1dkNkMHFTbmNoNEJNWmVJNjhJVmQvVUxZNC82ZWlyU2JsaUZHcUY4elN1ekExeFphL0d1SVpNVWpueXFqZStIRkRkcHRqak5ycGFkWVhOMHdiQjlxbUUyNXVzelIzVUxNWDBJbE1nemdwbUp1STAvU1hvelhWQjlENkxsWXBNNTBvL0VaemU3UWJ0R2FjTnBQTUw4NndqbEc1ZUQ5azdwNXdxK0ZpMVJxeEJ4SjJrS1lUVDQ3YWpXRzR2V25sVUtZdUdaeXYzK1RuMTYzVlAyT1VBPT0ifQ==; lc-acbca=en_CA; session-id=134-3546177-6545941; s_fid=60003CEB0B154EF8-2E3EDBC81E927F36; _rails-root_session=Wkx0U2lIS0E0aFpTU3pIMjNQNTdwbUJvRU96OE02RzJ1RmhNSHFMMCs0R0dSUG81aXNjS1lDdVZqazU1Zk4wcG16am43QVZ5MTJubkIxYmloVkU1dFdaTXNURmFRR2Fzckp2U3hHS1VnOG5mZ2pmUXhNU0NWWDUveVRSRUFGNjVlWnh1aUV6V1NrMktVdzFYVTlqUDNac3h3dHVnRGtIU1Bsa3ZNek5TV1Y3QlBJeHRMUVkzQ1ZlY1p5Mm9oamdHLS1qY2lCUkE4NXJWcnY4S0RYMlhvTkhnPT0=--1bd1afcc4e3676c7110565462dc028d94ac64dcc; x-acbca="Uer7FQGjhg?5vGV7GgmRg8jSK4I3SV16op5FiVQtyMJ7EGbJIQvmQ8sKb?uNpt7J"; at-acbca=Atza|IwEBIIRxpkNBe1QTBG2scRXBmlf9YcUnSI-S1sPHSZGPolmVckiCWk6ROqmplv-CXP4e5bSyL5U-Uq1V0mDhwEXWmQeUWU4uldRpr8mK5JoAiG1dWXNtBC_Z8xt5iZ9GSGPNkJp10vLaeYFRjUzq3Jk8P0OScjToRAorUYomAXI1h3MArGajGmO6VBtMI_h2iR0ZI3ThhX-6N_URt4d9tnDPVGDh; sess-at-acbca="t3mFRCFeFd7/MQo4kFMnohvtl43G8Qr2VEmvM67pNVQ="; sst-acbca=Sst1|PQFZVVkV8Sdg2k-DK0OOQDg-CVOMgHQvA8uUHekURl8JS51MieSS8oMzCrR9A_ykyhHu2KhcPg4y9PSWASuU_UAzeRnZdxj6oxp-EYoVvyl9q4Or6JS_jOmbm0hGjwfesv3TpiyxAv3j6bgPLEk6Gij72OwgFJgfB-D3lrYuTcdip5vqXMywgD3xMc_Iap1ij9brgsAuPHKr9bW9ExFnBt16Cfykb9DC9-QFtfXGWj5CzpfMDZOSuh_VI2PgXEHNdvfYwx_gQNkITLf6MCHrJheo4Qag2D2XeSDFHUP-d_1iUEA; session-id-time=2082787201l; i18n-prefs=CAD; csd-key=eyJ3YXNtVGVzdGVkIjp0cnVlLCJ3YXNtQ29tcGF0aWJsZSI6dHJ1ZSwid2ViQ3J5cHRvVGVzdGVkIjpmYWxzZSwidiI6MSwia2lkIjoiNDE3NWIwIiwia2V5IjoiWVNIcThoV2NpdDFpVUdPQVQ3TDIyc3EyU0NzUE5aeWFoeVR2WFlxTmd1OXZwbmJ3SVB0L1VkTk1ENzBBQTF5NU9qclRhd21keXBjaGsvR1NjRXVwMTBBMmI5eDRNaG43bVJRZU9IdnY5OUhGMlAyU0NIN0JEQjdacnR1akt0VmpPOE9qN3VDNkJtS01JTlBlR2xsVW9xM3BYZm9xL2NBTS9TQnlUMmV0eUFVTFVFM0NrTlhxU2s5K20zV1NuRUZzUDVLcURUaE9CSTJQNWYxSUMrS2Z0S0kyZmVNaFlUd3B6QUp4Qm5NQ3BnV0dMeDVtai9HU3ptU0c4eWsrSW9WcWlwaTNINTZGVmU1VG1MYmhDcWduWHBpSzBDSEVEaVllSk5vOVZlZzFtRWxkSHFuNE5QT3hCbWRhS0toQ1Q0ZWJCY21kRWdvdzJCaGJ1OHBpLzJkbHdBPT0ifQ==; session-token=A60y4CUDATjjt8JDncJKZUNrgIButQ59lnbe71KrWadyr/a8NgUKlHxoAy7wViDksfes571V6n3W6GJJnLI7ERx6R1X9rR9zJTOBM9bzvgn5FAbETGIqoBUPnQ/VjRG7O+A/M0rHYmXAi6GAZO4BdrVCMo+vLdv9+dmvw/vKzJiYVmHze4xwxe3cTfJzuiddLVvu6VDbjbBBBEfHXtEjZo+YRLWkDY1pKkobZxjQaNYw5Qgy2kQcxsB/aFWOXf/7OosTIYlqDP+Bs3VsvS4N9D+KSCz7TubU; csm-hit=tb:KES4H4KM2G8ME62ZZT6D+s-FSWT3FEF6Z7V6K81YEVG|1648846314956&t:1648846314956&adb:adblk_no'
    
    headers = {'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
               'accept-encoding':'gzip, deflate, br','accept-language':'en-US,en;q=0.9,uk;q=0.8,ru-RU;q=0.7,ru;q=0.6',
               'cache_control':'max-age=0','downlink':'10','ect':'4g','rtt':'50',
               'sec-ch-ua-mobile':'?0', 'sec-ch-platform':'"Windows"', 'sec-fetch-dest':'document','sec-fetch-mode':'navigate', 
               'sec-fetch-site':'none','sec-fetch-user':'?1','service-worker-navigation-preload':'true',
               'upgrade-insecure-requests':'1', 'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36',
               'cookie':cookie}
    
    for i, url in enumerate(url_list):
        #send the reponse:
        resp = requests.get(url, headers=headers)
        soup = BeautifulSoup(resp.content, 'lxml')
            
        #Extract the product title:
        try:
            product_title = soup.find('span', id='productTitle').text.strip()
        except:
            product_title = 'TITLE_ERR'
            
        #Extract and convert the price:
        try:
            price_block = soup.find('div',{'data-feature-name':"corePrice"})
            price = price_block.find('span',{'class':"a-offscreen"}).text.strip()
            #price_block = soup.find("span", {"class": "a-price aok-align-center priceToPay"})
            #price = price_block.find("span", {"class": "a-offscreen"}).text
        except:
            price = 'PRICE_NOT_FOUND'
        
        price=price.replace('$',"")
            
        try:
            price = float(price)
        except:
            price = 'NaN'
            
        try:
            stock = soup.find('div', id="availability").text.strip().split('.')[0]
            soup.find('div', id="availability").text.strip()
            
        except:
            stock = 'Unknown'
                
            
        log = pd.DataFrame({'date':now, 
                            'ASIN':df['ASIN'][i],
                            'URL':url,
                            'Title':product_title,
                            'Price':price,
                            'Stock':stock}, index=[i])
            
        tracker_log = tracker_log.append(log)
        print('appended '+ str(df['ASIN'][i]) +'\n' + product_title + '\n' + str(price) + '\n')
        progress_counter += 1
        progress = round((progress_counter/len(url_list))*100,1)
        left = len(url_list) - progress_counter
        
        print(f'{progress}% completed, {left} ASINs to go' + '\n')
        sleep_timer = np.random.randint(1,sleep_step)
        
        print(f'timeout for {sleep_timer} seconds...' + '\n\n')
        sleep(sleep_timer)
        
        
    print('end of API requests cycle!')
    
   
    return tracker_log            


def amazon_rankings(keyword,n=50, url_prefix='https://www.amazon.ca/s?k='):
    
    """
    This function sends API request with determined keywords to amazon.ca, and pulls ranking of objects
    url_prefix: str, optional -- url prefix for the path (by default it is amazon.ca)
    keyword: str, mandatory -- keywords to search
    n: int, optional -- how many search blocks to scan, 50 by default
    
    """
    cookie = 'ubid-acbca=131-8811406-8397347; s_cc=true; s_vnum=2072121012859&vn=1; s_ppv=79; s_nr=1640121197991-New; s_dslv=1640121197994; s_sq=acsca-prod=%26pid%3DG508510%26pidt%3D1%26oid%3Dhttps%253A%252F%252Fwww.amazon.ca%252Fgp%252Fhelp%252Fcustomer%252Fcontact-us%26ot%3DA; JSESSIONID=8142230128FC43DD81EA6E44BCBAC765; csd-key=eyJ3YXNtVGVzdGVkIjp0cnVlLCJ3YXNtQ29tcGF0aWJsZSI6dHJ1ZSwid2ViQ3J5cHRvVGVzdGVkIjpmYWxzZSwidiI6MSwia2lkIjoiNWM2ODViIiwia2V5IjoiRHdoZEhMNi9MMUJnUHd4SjFiUi9Cd3M3bHZXU2ZPWHI1eWlGcER4VStqTURxN1dZMzVhOS95UW1LVlNhWlFvK2gyM0JDdVA2eFdQSWtUUDdHelhaeUlOVERpUTc1YUlmb2FKamxlR0pSaU1Jb3ArVGpwY1pLdWFxai82WG5adVk5SWQ1dkNkMHFTbmNoNEJNWmVJNjhJVmQvVUxZNC82ZWlyU2JsaUZHcUY4elN1ekExeFphL0d1SVpNVWpueXFqZStIRkRkcHRqak5ycGFkWVhOMHdiQjlxbUUyNXVzelIzVUxNWDBJbE1nemdwbUp1STAvU1hvelhWQjlENkxsWXBNNTBvL0VaemU3UWJ0R2FjTnBQTUw4NndqbEc1ZUQ5azdwNXdxK0ZpMVJxeEJ4SjJrS1lUVDQ3YWpXRzR2V25sVUtZdUdaeXYzK1RuMTYzVlAyT1VBPT0ifQ==; lc-acbca=en_CA; session-id=134-3546177-6545941; s_fid=60003CEB0B154EF8-2E3EDBC81E927F36; _rails-root_session=Wkx0U2lIS0E0aFpTU3pIMjNQNTdwbUJvRU96OE02RzJ1RmhNSHFMMCs0R0dSUG81aXNjS1lDdVZqazU1Zk4wcG16am43QVZ5MTJubkIxYmloVkU1dFdaTXNURmFRR2Fzckp2U3hHS1VnOG5mZ2pmUXhNU0NWWDUveVRSRUFGNjVlWnh1aUV6V1NrMktVdzFYVTlqUDNac3h3dHVnRGtIU1Bsa3ZNek5TV1Y3QlBJeHRMUVkzQ1ZlY1p5Mm9oamdHLS1qY2lCUkE4NXJWcnY4S0RYMlhvTkhnPT0=--1bd1afcc4e3676c7110565462dc028d94ac64dcc; x-acbca="Uer7FQGjhg?5vGV7GgmRg8jSK4I3SV16op5FiVQtyMJ7EGbJIQvmQ8sKb?uNpt7J"; at-acbca=Atza|IwEBIIRxpkNBe1QTBG2scRXBmlf9YcUnSI-S1sPHSZGPolmVckiCWk6ROqmplv-CXP4e5bSyL5U-Uq1V0mDhwEXWmQeUWU4uldRpr8mK5JoAiG1dWXNtBC_Z8xt5iZ9GSGPNkJp10vLaeYFRjUzq3Jk8P0OScjToRAorUYomAXI1h3MArGajGmO6VBtMI_h2iR0ZI3ThhX-6N_URt4d9tnDPVGDh; sess-at-acbca="t3mFRCFeFd7/MQo4kFMnohvtl43G8Qr2VEmvM67pNVQ="; sst-acbca=Sst1|PQFZVVkV8Sdg2k-DK0OOQDg-CVOMgHQvA8uUHekURl8JS51MieSS8oMzCrR9A_ykyhHu2KhcPg4y9PSWASuU_UAzeRnZdxj6oxp-EYoVvyl9q4Or6JS_jOmbm0hGjwfesv3TpiyxAv3j6bgPLEk6Gij72OwgFJgfB-D3lrYuTcdip5vqXMywgD3xMc_Iap1ij9brgsAuPHKr9bW9ExFnBt16Cfykb9DC9-QFtfXGWj5CzpfMDZOSuh_VI2PgXEHNdvfYwx_gQNkITLf6MCHrJheo4Qag2D2XeSDFHUP-d_1iUEA; session-id-time=2082787201l; i18n-prefs=CAD; csd-key=eyJ3YXNtVGVzdGVkIjp0cnVlLCJ3YXNtQ29tcGF0aWJsZSI6dHJ1ZSwid2ViQ3J5cHRvVGVzdGVkIjpmYWxzZSwidiI6MSwia2lkIjoiNDE3NWIwIiwia2V5IjoiWVNIcThoV2NpdDFpVUdPQVQ3TDIyc3EyU0NzUE5aeWFoeVR2WFlxTmd1OXZwbmJ3SVB0L1VkTk1ENzBBQTF5NU9qclRhd21keXBjaGsvR1NjRXVwMTBBMmI5eDRNaG43bVJRZU9IdnY5OUhGMlAyU0NIN0JEQjdacnR1akt0VmpPOE9qN3VDNkJtS01JTlBlR2xsVW9xM3BYZm9xL2NBTS9TQnlUMmV0eUFVTFVFM0NrTlhxU2s5K20zV1NuRUZzUDVLcURUaE9CSTJQNWYxSUMrS2Z0S0kyZmVNaFlUd3B6QUp4Qm5NQ3BnV0dMeDVtai9HU3ptU0c4eWsrSW9WcWlwaTNINTZGVmU1VG1MYmhDcWduWHBpSzBDSEVEaVllSk5vOVZlZzFtRWxkSHFuNE5QT3hCbWRhS0toQ1Q0ZWJCY21kRWdvdzJCaGJ1OHBpLzJkbHdBPT0ifQ==; session-token="o/98UEZ3jh4ofaC6E+5bVk+COc6sGOrleOujY19BU0D5yXVFyay7gfU4+IRiLRP15rncktqrWPuI5B+ePn48DbIdfip998Xq29ucOjN/8u44tMXHkaHk2MjCcJbLGVEOgC7gUONtZIGwxjZaql9iWo5mCoAvD1MxVqNt3oSKrZmampF0Gec76yvydk2MckFpztDR3anip8cpFTjc0ZCv1A=="; csm-hit=tb:s-792RPFXT98DMQ3C5SDQ4|1648503456320&t:1648503456803&adb:adblk_no'
    
    headers = {'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
               'accept-encoding':'gzip, deflate, br','accept-language':'en-US,en;q=0.9,uk;q=0.8,ru-RU;q=0.7,ru;q=0.6',
               'cache_control':'max-age=0','downlink':'10','ect':'4g','rtt':'50',
               'sec-ch-ua-mobile':'?0', 'sec-ch-platform':'"Windows"', 'sec-fetch-dest':'document','sec-fetch-mode':'navigate', 
               'sec-fetch-site':'none','sec-fetch-user':'?1','service-worker-navigation-preload':'true',
               'upgrade-insecure-requests':'1', 'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36',
               'cookie':cookie}    
    
    # Create API request URLs
    url = url_prefix + keyword.replace(" ", "+")
    
    #send the response
    resp = requests.get(url, headers=headers)
    
    # parse results to BeautifulSoup object:
    soup = BeautifulSoup(resp.content, 'lxml')
    
    # empty dictionary to collect data
    ASINS = {}
    
    # explore the Beautiful Soup object and extract
    for i in range(1,n):
        # this finds the i block of content in search results
        block = soup.find('div', {'data-index':str(i)})
        # this is the inner block, which contains data on search index
        
        try:
            # just in case there is no i block in SERP
            block_attr = block.attrs
            
            try:
                block_inner = block.find('div').find('div')
                block_inner_attr = block_inner.attrs
                # this will return a string like 'search-results_7'
                search_index = block_inner_attr['class'][-1].split('=')[-1]
            except:
                # if there is no attribute, then there's no index (Like in AdBlock)
                search_index = 'no_index'
            
            if 'sg-col-4-of-12' in block_attr['class']:
                if 'AdHolder' in block_attr['class']:
                    ASINS[i] = [search_index, block_attr['data-asin'],'Sponsored']
                else:
                    ASINS[i] = [search_index, block_attr['data-asin'],'Organic']
            
            elif 'AdHolder' in block_attr['class']:
                ASINS[i] = [search_index, 'AdBlock','Sponsored']
            
            elif 'sg-col-0-of-12' in block_attr['class']:
                if 'Sponsored' in block.text:
                    ASINS[i] = [search_index,'AdBlock','Sponsored']
                else:
                    ASINS[i] = [search_index,'Other_ad','Other']
            else:
                ASINS[i] = [search_index, 'Other_block','Other']
            
        except:
            search_index = 'NaN'
            ASINS[i] = [search_index,'not_found','not_found']
                
    df = pd.DataFrame.from_dict(ASINS, orient='index', columns=['Rank','ASIN','Sponsored'])
    df['Search_date']=datetime.now().strftime('%Y-%m-%d %H:%M')
    df['Keyword'] = keyword.replace("+", " ")
    cols = df.columns.tolist()
    cols = cols[-2:] + cols[:-2]
    df = df[cols]
    df = df[~(df['ASIN']=='Other_block')]
    df['Rank'] = np.where(~df['Rank'].str.contains('search-results',na=False),"NA",df['Rank'])
    df = df[~(df.Sponsored == 'not_found')]
    
    return df


def category_scraper(category, brands):
    
    start_time = time.time()
    
    # Naming convention for CSV file with keywords: {category}_keywords.csv
    keywords_df = pd.read_csv(f'{category}_keywords.csv', header=None)

    keywords = keywords_df[0].to_list()

    df_rank_full = pd.DataFrame()

    # STEP 1. We'll scrape the search results for each keyword in the list

    counter = 0

    for keyword in keywords:
        counter +=1
        print(f"Appending keyword: {keyword}. {counter} out of {len(keywords)} done")
            
        results = amazon_rankings(keyword)
        print(results.shape)    
        df_rank_full = pd.concat([df_rank_full, results], sort=False)
            
        print(df_rank_full.shape)
        

    # STEP 2. We need to extract ASINS from ranks scraping: we are leaving only items which are 10 digits long (valid ASINS)

    ASIN_list = [asin for asin in df_rank_full.ASIN.unique() if len(asin) == 10]

    # STEP 3. Function amz_price_tracker needs Pandas DataFrame with ASIN and URL to work properly.
    # Therefore next step is to build the DataFrame out of the ASIN list knowing URL prefix for each Amazon listing

    df_asins = pd.DataFrame()

    df_asins['ASIN'] = ASIN_list
    df_asins['Prefix'] = 'https://www.amazon.ca/dp/'
    df_asins['URL'] = df_asins.Prefix + df_asins.ASIN

    # now we have a df_asins DataFrame which contains ASIN, and full URL to access this ASIN
    # now we can use this df as an input for function amz_price_tracker

    # STEP 3. Execute amz_price_tracker and assign to new DataFrame

    df_price_full = amz_price_tracker(df_asins)

    # STEP 4. Now we can merge two DataFrames on ASINs, we will leave only one date column from the search requests
    df_full = pd.merge(df_rank_full, df_price_full, how='left', on = 'ASIN')

    # we add a column 'Brand' and classify all as 'Other' we will reclassify brands on a full dataframe (in case we want to track new brands)
    df_full.drop(['date'], axis=1, inplace=True)

    #Noe let's reclassify the brands in the results dataframe:
        
    df_full['Brand'] = 'Other'

    
    for brand in brands:
        for row in range(df_full.shape[0]):
            try:
                if brand.lower() in df_full.loc[row,'Title'].lower():
                    df_full.loc[row,'Brand']=brand
                    print('row ', row, 'changed to ',brand)
            except AttributeError:
                df_full.loc[row,'Brand']='nan'
                print('row ', row, ' is nan')

    # STEP 5. Now we need to save the output by creating a new file, or appending to the previously created file

    try:
        last_search = glob(f'./search_history/{category}/*.csv')[-1]
        search_hist = pd.read_csv(last_search)
        print("Previous datafile located. Appending data to it")

    except:
        print("No previous datafile located, will create a new file")
        search_hist = pd.DataFrame()
        
    final_df = search_hist.append(df_full, sort=False)

    final_df.reset_index(drop=True, inplace=True)

                       
    print("\n \n Data pull completed \n")




    print('Checking need to reclassify brands')

    # STEP 6. reclassify the brands. We could move this block of code to a smaller DF
       
    df_brands = list(final_df['Brand'].unique())

    try:
        df_brands.remove('Other')
    except:
        print('No "Other" value in Brands column')

    try:
        df_brands.remove('nan')
    except:
        print('No "nan" value in Brands column')
        

    df_brands = [value for value in df_brands if type(value)==str]

    if df_brands.sort() == brands.sort():
        print("No need to update Brands in historical database")

    else:
        for brand in brands:
            for row in range(final_df.shape[0]):
                try:
                    if brand.lower() in final_df.loc[row,'Title'].lower():
                        final_df.loc[row,'Brand']=brand
                        print('row ', row, 'changed to ',brand)
                except AttributeError:
                    final_df.loc[row,'Brand']='nan'
                    print('row ', row, ' is nan')
                
    now_filename = datetime.now().strftime('%Y-%m-%d %H-%M')
                
    final_df.to_csv(f'search_history/{category}/SEARCH_HISTORY_{category}_{now_filename}.csv', index=False)

    end_time = time.time()
                
    time_lapsed = end_time - start_time

    print("All done!")
    time_convert(time_lapsed)

    
def reframe(category, n=21):
    #Pulling out file with the latest search:
    last_search = glob(f'./search_history/{category}/*.csv')[-1]
    data_pull = pd.read_csv(last_search)

    # Pulling out the dates -- those would be the column names
    dates = list(data_pull['Search_date'].unique())

    # as we are looking at top 20 ranks, we create the selector
    ranks_range = range(1,n)

    ranks = ['search-results_'+str(i) for i in ranks_range]

    #pull list of keywords for files
    keywords = list(data_pull['Keyword'].unique())

    #select only the required rankings
    data_short = data_pull.drop(data_pull[~data_pull['Rank'].isin(ranks)].index)

    #create a dictionary of dataframes 
    dataframes = {keyword : pd.DataFrame(index=ranks, columns=dates) for keyword in keywords }

    for keyword in keywords:
        for date in dates:
            for rank in ranks:
                try:
                    selection = data_short[(data_short['Keyword']==keyword) & (data_short['Search_date']==date) & (data_short['Rank']==rank)].Brand
                    target_brand = selection.iloc[0]
                    dataframes[keyword].loc[rank, date] = target_brand
                except:
                    dataframes[keyword].loc[rank, date] = 'nan'
                


    with pd.ExcelWriter(f'search_history/{category}/rankings_{category}_{dates[-1][0:10]}.xlsx') as writer:
        for keyword in keywords:
            dataframes[keyword].to_excel(writer, sheet_name=keyword)
