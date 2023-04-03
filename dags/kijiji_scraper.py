from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from dateutil.relativedelta import relativedelta
from datetime import datetime, date
import numpy as np
############################################################
# DAG settings
############################################################
DAG_NAME = "kijiji_scraper"
DAG_DESCRIPTION = "A web scraper to scrape apartment/condo listings from kijiji across GTA!"
DAG_START_DATE = datetime(2023, 3, 29)
DAG_SCHEDULE_INTERVAL = "0 10 * * *"
DAG_CATCHUP = False # When set to true, DAG will start running from DAG_START_DATE instead of current date
DAG_PAUSED_UPON_CREATION = True # Defaults to False. When set to True, uploading a DAG for the first time, the DAG doesn't start directly 
DAG_MAX_ACTIVE_RUNS = 5 # Configure efficiency: Max. number of active runs for this DAG. Scheduler will not create new active DAG runs once this limit is hit.
############################################################
# Python functions 
############################################################
base_url = "https://www.kijiji.ca"
apartment_listings = "https://www.kijiji.ca/b-apartments-condos/gta-greater-toronto-area/page-{}/c37l1700272"
house_lists = "https://www.kijiji.ca/b-apartments-condos/gta-greater-toronto-area/house/page-{}/c37l1700272a29276001"
scrape_status = True
working_dir = "/path/to/container/folder/"
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

def parse_html(main_url,start_page, end_page):
    """
        A function to extract required info
       
    """
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
        break

    return data

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

def scrape_apartment_listings(start_page, end_page,**context):
    """
        A function to scrape apartment/condo listings posted across GTA!
        NB: public has only access to first 100 pages, so we will be scraping 100 pages only.
    """
    
    data = parse_html(apartment_listings,start_page, end_page)
    
    if len(data) > 1:
        df = generate_df(data)
        now = datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        file_name = working_dir+"apt/kijiji_gta_"+str(start_page)+"_"+current_date+".xlsx"
        df.to_excel(file_name)
    else:
        print("scraping failed!")
        scrape_status = False
                
def scrape_house_listings(start_page, end_page,**context):
    """
        A function to scrape house rental listings posted across GTA!
        NB: public has only access to first 100 pages, so we will be scraping 100 pages only.
    """
    data = parse_html(apartment_listings,start_page, end_page)
    if len(data) > 1:
        df = generate_df(data)
        now = datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        file_name = working_dir+"house/kijiji_gta_"+str(start_page)+"_"+current_date+".xlsx"
        df.to_excel(file_name)
    else:
        print("scraping failed!")
        scrape_status = False

def process_data(**context):
    df1 = context['ti'].xcom_pull(key='apartment_df')
    df2 = context['ti'].xcom_pull(key='house_df')
    df = pd.concat([df1, df2])
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    file_name = "kijiji_gta_"+current_date+".xlsx"
    df.to_excel(file_name)

def send_email_fun(scrape_type, **context):
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    if scrape_status:
        html = f"Task group {scrape_type} has completed!!!- {current_date}"
        subject = f"scraping completed for {scrape_type} - {current_date}"
    else:
        html = f"Task group {scrape_type} has completed!!!- {current_date}"
        subject = f"scraping failed for {scrape_type} - {current_date}"
    send_email(
        to="amaldasdxm@gmail.com",
        subject=subject,
        html_content= html
    )

############################################################
# Main DAG
#############################################################
with DAG(DAG_NAME,
    start_date=DAG_START_DATE, 
    schedule_interval=DAG_SCHEDULE_INTERVAL, 
    catchup=DAG_CATCHUP,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    concurrency=DAG_MAX_ACTIVE_RUNS
) as dag:

    # Start
    start_scrape = DummyOperator(
        task_id="start_scrape")

    scrape_apartment_task = PythonOperator(
        task_id='scrape_apartment_task',
        python_callable=scrape_apartment_listings,
        op_kwargs={"start_page": 1, "end_page": 20},
    )

    with TaskGroup("scrape_apartment_pages") as scrape_group:
        scrape_pages = []
        for i in range(2, 6):
            scrape_pages.append(
                PythonOperator(
                    task_id=f"scrape_pages_{i}",
                    python_callable=scrape_apartment_listings,
                    op_kwargs={"start_page": (i-1)*20+1, "end_page": i*20},
                )
            )
    
    scrape_house_task = PythonOperator(
        task_id='scrape_house_task',
        python_callable=scrape_house_listings,
        op_kwargs={"start_page": 1, "end_page": 20},
    )

    with TaskGroup("scrape_house_pages") as scrape_group2:
        scrape_pages = []
        for i in range(2, 6):
            scrape_pages.append(
                PythonOperator(
                    task_id=f"scrape_pages_{i}_2",
                    python_callable=scrape_house_listings,
                    op_kwargs={"start_page": (i-1)*20+1, "end_page": i*20},
                )
            )

    apartment_notification = PythonOperator(
        task_id='apartment_completion_notification',
        python_callable=send_email_fun,
        provide_context=True,
        op_kwargs={'scrape_type': 'scrape_apartment_pages'}
    )

    house_notification = PythonOperator(
        task_id='house_completion_notification',
        python_callable=send_email_fun,
        provide_context=True,
        op_kwargs={'scrape_type': 'scrape_house_pages'}
    )

    scraping_ends = DummyOperator(
        task_id="scraping_ends"
    )

    
    start_scrape >> scrape_apartment_task >> scrape_group >> apartment_notification >> scraping_ends
    start_scrape >> scrape_house_task >> scrape_group2 >> house_notification >> scraping_ends
