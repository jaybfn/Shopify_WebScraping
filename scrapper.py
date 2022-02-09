"""As part of a thorough web scraping effort, products from the Decathlon website 
were grabbed and stored to a database! I'll also use pagination approaches to build 
and save all of the product data in a database. Using Dataset to handle our 
SQLite database, we can rapidly create a table and add data to it.
 """

# importing necessary libraries

import requests
import json
import dataset
import logging
from sqlalchemy import create_engine
import psycopg2

#Building a class fro scrapping data from website
class Web_Scrapper():

    def __init__(self, baseurl):
        self.baseurl = baseurl

    # function to download json page 

    def download_json_page(self, page):

        # website based on shopify platform, the data is stores in json format so adding 'products.json?limit=250&page={page}'
        # to ur url will list all the product in json format and {page} is the json page number as perpage we are listing 250 product
        # if you want more product then you can change the page to 2 to enter the 2nd page and so on.

        request = requests.get(self.baseurl + f'products.json?limit=250&page={page}', timeout = 30)
        if request.status_code != 200:
            #print('Corrupt:', request.status_code)
            logging.error('Corrupt_Page:', request.status_code)
        if len(request.json()['products']) > 0:
            data = request.json()['products']
            return data
        else:
            return

    # function to extract data from json page

    def extract_json(self, jsondata):
        products = []

        for product in jsondata:
            mainid = product['id']
            title = product['title']
            published_at = product['published_at']
            vendor = product['vendor']
            product_type = product['product_type']
            # for loop to extract various product parameters
            for product_variant in product['variants']:
                item = {
                    'seller':vendor,
                    'id' : mainid,
                    'subid': product_variant['id'],
                    'title' : title,
                    'product_type': product_type,
                    'published_at' : published_at,
                    'sku' : product_variant['sku'],
                    'price' : product_variant['price'],
                    'weigth_grams' : product_variant['grams'],
                    'compare_at_price' : product_variant['compare_at_price'],
                    'product_id': product_variant['product_id']
                }
                products.append(item)
        return products

# Here an object is created so initialize the Web_Scrapper class
# Next, all the pages are download and products from each page is 
# scrapped and stores into a SQllite database.
def main():
    logging.getLogger().setLevel(logging.INFO)
    Decathlon = Web_Scrapper('https://www.decathlon.com/')
    all_entries = []
    logging.info('Starting to scrape ....')
    for page in range(1,3):
        data = Decathlon.download_json_page(page)
        print('Scrapping page:',page)
        try:
            all_entries.append(Decathlon.extract_json(data))
        except:
            #print(f'All products scrapped, at page: {page-1})
            logging.info(f'All products scrapped, at page: {page-1}')
            break
    return all_entries

if __name__ == '__main__':
   
    database_table = dataset.connect('sqlite:///decathlon.db')                 # connecting to sqlite server and saving the data
    table = database_table.create_table('Decathlon',primary_id = 'subid' )     # creating a table and saving the data
    products = main()   	                                                   # calling the function main() and initializing to a variable
    all_data = [item for i in products for item in i]
    #print(all_data)

    for product in all_data:
        if not table.find_one(subid = product['subid']):
            table.insert(product)
            print('New_Listing:',product)
