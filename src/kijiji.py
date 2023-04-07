import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from dateutil.relativedelta import relativedelta
from datetime import datetime, date
import numpy as np
from azure.storage.filedatalake import DataLakeServiceClient

class KijijiDataProcessor:

    def __init__(self, storage_account_name, storage_account_key, file_system):
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.file_system = file_system
        self.base_url = "https://www.kijiji.ca"
        self.apartment_listings = "https://www.kijiji.ca/b-apartments-condos/gta-greater-toronto-area/page-{}/c37l1700272"
        self.house_lists = "https://www.kijiji.ca/b-apartments-condos/gta-greater-toronto-area/house/page-{}/c37l1700272a29276001"
        self.price_list = []
        self.title_list = []
        self.location_list = []
        self.posted_list = []
        self.bed_rooms_list = []
        self.url_list = []
        self.list_ids = []

    def parse_html(self,type_,start_page, end_page):
        """
            A function to extract required info
        
        """
        if type_ == "apartment":
            main_url = self.apartment_listings
        elif type_ == "house":
            main_url = self.house_lists

        for i in range(start_page,end_page):
            url_scrape = main_url.format(i)
            print(f"scraping page number : {i}")
            res = requests.get(url_scrape).text
            time.sleep(2)
            #get the price details
            soup = BeautifulSoup(res, features="lxml")
            for price in soup.find_all('div', {'class': 'price'}):
                self.price_list.append(price.text.strip())
            #get the listing title    
            for title in soup.find_all('div', {'class': 'title'}):
                self.title_list.append(title.text.strip())
            #get the location name
            for location in soup.find_all('div', {'class': 'location'}):
                self.location_list.append(location.find('span').text.strip())
            #get the posted date    
            for date_posted in soup.find_all('div', {'class': 'location'}):
                self.posted_list.append(date_posted.find('span',{'class': 'date-posted'}).text.strip())
            #get the number of bed rooms   
            for bed_rom in soup.find_all('span', {'class': 'bedrooms'}):
                self.bed_rooms_list.append(bed_rom.text.strip().replace("\n","").split("Beds:")[1].strip())
            #get the url for the listing
            for url in soup.find_all('div', {'class': 'title'}):
                item = self.base_url+url.find('a')['href']
                self.url_list.append(item)
                listing_id = item.split("/")[-1]
                self.list_ids.append(listing_id)
            
            data = [self.list_ids, self.title_list, self.location_list, self.price_list, self.bed_rooms_list, self.posted_list, self.url_list]
            status = all(len(x) == len(data[0]) for x in data)
            if not status:
                data = []
                break
            # break

        return data
        
    def generate_df(self,data):
        # create a data frame
        df = pd.DataFrame({
                "id":data[0],
                "title":data[1],
                "location":data[2],
                "rent":data[3],
                "bed_rooms":data[4],
                "date_posted":data[5],
                "url":data[6]
            })
        try:    
            df["date_posted"] = df.date_posted.apply(parse_date_string)
        except:
            pass
        now = datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        df['scraped_on'] = current_date
        return df


    def write_to_adls(self,df,df_name,dir_name):

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                                                "https", self.storage_account_name), 
                                            credential=self.storage_account_key
                                            )
        file_system_client = service_client.get_file_system_client(file_system=self.file_system)
        date_str = datetime.now().strftime('%Y-%m-%d')
        directory_name = f"{dir_name}/{date_str}"
        directory_client = file_system_client.create_directory(directory_name)
        directory_client = file_system_client.get_directory_client(directory_name)
        file_client = directory_client.create_file(df_name)
        csv_data = df.to_csv()
        file_client.append_data(data=csv_data, offset=0, length=len(csv_data))
        file_client.flush_data(len(csv_data))
        