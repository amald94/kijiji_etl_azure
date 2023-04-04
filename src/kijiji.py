import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from dateutil.relativedelta import relativedelta
from datetime import datetime, date
import numpy as np
from azure.storage.filedatalake import DataLakeServiceClient

base_url = "https://www.kijiji.ca"
apartment_listings = "https://www.kijiji.ca/b-apartments-condos/gta-greater-toronto-area/page-{}/c37l1700272"
house_lists = "https://www.kijiji.ca/b-apartments-condos/gta-greater-toronto-area/house/page-{}/c37l1700272a29276001"

def process_data(**context):
    df1 = context['ti'].xcom_pull(key='apartment_df')
    df2 = context['ti'].xcom_pull(key='house_df')
    df = pd.concat([df1, df2])
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    file_name = "kijiji_gta_"+current_date+".xlsx"
    df.to_excel(file_name)

def parse_html(type_,start_page, end_page):
    """
        A function to extract required info
       
    """
    if type_ == "apartment":
        main_url = apartment_listings
    elif type_ == "house":
        main_url = house_lists

    price_list = []
    title_list = []
    location_list = []
    posted_list = []
    bed_rooms_list = []
    url_list = []
    list_ids = []

    for i in range(start_page,end_page):
        url_scrape = main_url.format(i)
        print(f"scraping page number : {i}")
        res = requests.get(url_scrape).text
        time.sleep(2)
        #get the price details
        soup = BeautifulSoup(res, features="lxml")
        for price in soup.find_all('div', {'class': 'price'}):
            price_list.append(price.text.strip())
        #get the listing title    
        for title in soup.find_all('div', {'class': 'title'}):
            title_list.append(title.text.strip())
        #get the location name
        for location in soup.find_all('div', {'class': 'location'}):
            location_list.append(location.find('span').text.strip())
        #get the posted date    
        for date_posted in soup.find_all('div', {'class': 'location'}):
            posted_list.append(date_posted.find('span',{'class': 'date-posted'}).text.strip())
        #get the number of bed rooms   
        for bed_rom in soup.find_all('span', {'class': 'bedrooms'}):
            bed_rooms_list.append(bed_rom.text.strip().replace("\n","").split("Beds:")[1].strip())
        #get the url for the listing
        for url in soup.find_all('div', {'class': 'title'}):
            item = base_url+url.find('a')['href']
            url_list.append(item)
            listing_id = item.split("/")[-1]
            list_ids.append(listing_id)
        
        print(len(list_ids), len(title_list), len(location_list), len(price_list), len(bed_rooms_list), len(posted_list), len(url_list))
        data = [list_ids, title_list, location_list, price_list, bed_rooms_list, posted_list, url_list]
        status = all(len(x) == len(data[0]) for x in data)
        if not status:
            data = []
            break
        # break

    return data

def parse_date_string(date_string):
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if date_string == 'Yesterday':
        return (now - relativedelta(days=1)).date()
    elif 'minute' in date_string:
        return now.date()
    elif date_string.startswith('<'):
        # Convert relative time to absolute time
        minutes_ago = int(date_string.split()[1])
        return datetime.now() - timedelta(minutes=minutes_ago)
    else:
        return datetime.strptime(date_string, '%d/%m/%Y').date()

def generate_df(data):
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


def write_to_adls(df,df_name,storage_account_name,storage_account_key,file_system,dir_name):

    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                                            "https", storage_account_name), 
                                           credential=storage_account_key
                                          )
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    date_str = datetime.now().strftime('%Y-%m-%d')
    directory_name = f"{dir_name}/{date_str}"
    directory_client = file_system_client.create_directory(directory_name)
    directory_client = file_system_client.get_directory_client(directory_name)
    file_client = directory_client.create_file(df_name)
    csv_data = df.to_csv()
    file_client.append_data(data=csv_data, offset=0, length=len(csv_data))
    file_client.flush_data(len(csv_data))
    