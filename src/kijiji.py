import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from dateutil.relativedelta import relativedelta
from datetime import datetime, date
import numpy as np
from azure.storage.filedatalake import DataLakeServiceClient

class KijijiDataProcessor:
    """
    A class that processes data from Kijiji using web scraping and uploads it to an Azure Data Lake Storage container.
        Args:
            storage_account_name (str): The name of the Azure Data Lake Storage account.
            storage_account_key (str): The access key for the Azure Data Lake Storage account.
            file_system (str): The name of the file system (i.e. container) where data will be uploaded.
    """
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
            Extract rental listings of a specified type posted across the Greater Toronto Area from Kijiji.
            The function supports parallel execution by segmenting the total pages to be scraped between a start and end page number.
            
            Args:
                type_ (str): The type of rental listing to scrape (e.g. 'apartment','house').
                start_page (int): The page number to start scraping from.
                end_page (int): The page number to stop scraping at (inclusive).
                
            Returns:
                A nested list of extracted contents from the website in following format : List[List[str]]
        """
        if type_ == "apartment":
            main_url = self.apartment_listings
        elif type_ == "house":
            main_url = self.house_lists
        # loop through the start and end page and scrape the data
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

    def parse_date_string(self, date_string):
        """
            Parses a date string extracted from a Kijiji rental listing and returns a Python date object.
            Args:
                date_string (str): The date string to be parsed.
            Returns:
                A Python date object representing the date extracted from the input string.
        """
        now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # Check for special cases where the date string is not in the standard format
        if date_string == 'Yesterday':
            return (now - relativedelta(days=1)).date()
        elif 'minute' in date_string:
            return now.date()
        elif date_string.startswith('<'):
            # Convert relative time to absolute time
            minutes_ago = int(date_string.split()[1])
            return datetime.now() - timedelta(minutes=minutes_ago)
        else:
            try:
                return datetime.strptime(date_string, '%d/%m/%Y').date()
            except ValueError:
                raise ValueError(f"Invalid date string format: {date_string}")
        
    def generate_df(self,data):
        """
            Creates a pandas DataFrame from a nested list of rental listings extracted from Kijiji.

            Args:
                data (List[List[str]]): A nested list where each element corresponds to a rental listing 
                
            Returns:
                A pandas DataFrame where each row corresponds to a rental listing.
        """
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
        """
            Writes a Pandas DataFrame to a CSV file in Azure Data Lake Storage.

            Args:
                df (pandas.DataFrame): The DataFrame to be written.
                df_name (str): The name of the output CSV file.
                dir_name (str): The name of the output directory.
        """

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
        