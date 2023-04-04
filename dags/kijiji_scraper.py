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
import sys
sys.path.append('/opt/airflow/src')
from kijiji import parse_html, parse_date_string, generate_df

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
scrape_status = True
working_dir = "/path/to/container/folder/"

def scrape_apartment_listings(start_page, end_page,**context):
    """
        A function to scrape apartment/condo listings posted across GTA!
        NB: public has only access to first 100 pages, so we will be scraping 100 pages only.
    """
    
    data = parse_html("apartment",start_page, end_page)
    
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
    data = parse_html("house",start_page, end_page)
    if len(data) > 1:
        df = generate_df(data)
        now = datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        file_name = working_dir+"house/kijiji_gta_"+str(start_page)+"_"+current_date+".xlsx"
        df.to_excel(file_name)
    else:
        print("scraping failed!")
        scrape_status = False

def send_email_fun(scrape_type, **context):
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    if scrape_status:
        html = f"Task group {scrape_type} has completed!!!- {current_date}"
        subject = f"scraping completed for {scrape_type} - {current_date}"
    else:
        html = f"Task group {scrape_type} has failed!!!- {current_date}"
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
                    task_id=f"scrape_pages_from_{(i-1)*20+1}_{i*20}",
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
                    task_id=f"scrape_pages_from_{(i-1)*20+1}_{i*20}_2",
                    python_callable=scrape_house_listings,
                    op_kwargs={"start_page": (i-1)*20+1, "end_page": i*20},
                )
            )

    apartment_notification = PythonOperator(
        task_id='apt_scrape_status_email_notification',
        python_callable=send_email_fun,
        provide_context=True,
        trigger_rule='all_done',
        op_kwargs={'scrape_type': 'scrape_apartment_pages'}
    )

    house_notification = PythonOperator(
        task_id='house_scrape_status_email_notification',
        python_callable=send_email_fun,
        provide_context=True,
        trigger_rule='all_done',
        op_kwargs={'scrape_type': 'scrape_house_pages'}
    )

    scraping_ends = DummyOperator(
        task_id="scraping_ends"
    )

    
    start_scrape >> scrape_apartment_task >> scrape_group >> apartment_notification >> scraping_ends
    start_scrape >> scrape_house_task >> scrape_group2 >> house_notification >> scraping_ends