import os
from io import BytesIO
from zipfile import ZipFile
from enum import Enum
import asyncio
import aiohttp
from logger import log_debug,log_info,log_error

class WEB(Enum):
    URLS = [
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
    ]
    DIRECTORY = "Downloads"


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


async def download_file(download_path, web_url, session):
    async with session.get(web_url) as response:
        try:
            status = response.status
            log_debug(status)
            if status == 200:
                data = await response.read()
                zipfile = ZipFile(BytesIO(data))
                zipfile.extractall(path=download_path)
            else:
                raise Exception(
                    f"Failed to get with status code: {response.status_code}"
                )
        except Exception as e:
            raise Exception(f"An error occurred: {e}")


async def main():
    try:
        download_path = create_dir(WEB.DIRECTORY.value)
        async with aiohttp.ClientSession() as session:
            tasks = [
                download_file(download_path, url, session) for url in WEB.URLS.value
            ]
            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )
    except Exception as e:
        log_error(f"The System failed with following error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
