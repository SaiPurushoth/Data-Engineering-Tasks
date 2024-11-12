import itertools
import requests
import pandas as pd
import os
from enum import Enum
from bs4 import BeautifulSoup
import glob
from concurrent.futures import ThreadPoolExecutor
from logger import log_info,log_debug,log_error

class METADATA(Enum):
    URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    DIRECTORY = "Downloads"
    DATE = "2024-01-19 10:27"
    FORMAT = "CSV"


def create_dir(directory_name):
    try:
        os.mkdir(directory_name)
        log_info(f"Directory '{directory_name}' created successfully.")
        return os.path.join(os.getcwd(), directory_name)
    except FileExistsError:
        log_error(f"Directory '{directory_name}' already exists.")
        return os.path.join(os.getcwd(), directory_name)
    except PermissionError:
        raise Exception(f"Permission denied: Unable to create '{directory_name}'.")
    except Exception as e:
        raise Exception(f"An error occurred: {e}")



def parse_html(content):
    try:
        file_names = []
        soup = BeautifulSoup(content, 'html.parser')
        items = soup.find_all('tr')
        for tr_items in items:
            if METADATA.DATE.value in tr_items.text:
                file_names.append(tr_items.text.split('.')[0])
        return file_names
    except Exception as e:
        log_error(f"Error parsing the HTML with Error")
        raise Exception(e)

def download_files(file_name):
        download_path = create_dir(METADATA.DIRECTORY.value)
        try:
            web_url = f"{METADATA.URL.value}{file_name}.csv"
            response = requests.get(web_url)
            csv_file = open(f'{download_path}/{file_name}.csv','wb')
            csv_file.write(response.content)
            csv_file.close()
        except Exception as e:
            log_error(f"Cannot Download File: {e}")
            raise Exception(e)


def get_whether_data(web_url):
    try:
        response = requests.get(web_url)
        file_names=parse_html(response.content)
        return file_names
    except Exception as e:
        log_error(f"Cannot Fetch Data from website with status code {response.status_code}")
        raise Exception(e)


def find_max_BulbTemperature():
    file_path=os.path.join(os.getcwd(), METADATA.DIRECTORY.value)
    csv_files = glob.glob(f"{file_path}/*.csv")
    main_dataframe = pd.DataFrame(pd.read_csv(csv_files[0],usecols = ['HourlyDewPointTemperature'])) # select only the column need to do operation to reduce in-memory size
    for i in range(1,len(csv_files)): 
        data = pd.read_csv(csv_files[i],usecols = ['HourlyDewPointTemperature']) 
        df = pd.DataFrame(data) 
        main_dataframe = pd.concat([main_dataframe,df],axis=1) 
    print(main_dataframe)
    max_val = df["HourlyDewPointTemperature"].max() 
    log_info(f"max value: {max_val}")

def scrape_and_download():
    try:
        file_names=get_whether_data(METADATA.URL.value)
        log_info(f"length of file_names: {len(file_names)}")
        log_debug(file_names)
        with ThreadPoolExecutor(10) as executor:
            executor.map(download_files, file_names)
    except Exception as e:
        raise Exception(e)

def main():
    try:
        # scrape_and_download()
        find_max_BulbTemperature()
    except Exception as e:
        log_error(f"System Error with exeception: {e}")

if __name__ == "__main__":
    main()
