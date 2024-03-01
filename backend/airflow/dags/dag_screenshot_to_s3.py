from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from PIL import Image
import time
import os
import json

from airflow.decorators import dag, task 

FUND_CAPTURE = [1320, 350, 2075, 770]
CHAIN_CAPTURE = [765, 245, 2070, 600] 
BLOCK_CAPTURE = [1320, 350, 2075, 770]

default_args = {
    'owner' : 'mseidl',
    'retries' : 2,
    'email' : ['matthias@orbal-analytics.com', 'mike@growthepie.xyz'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_screenshot_loader',
    description = 'Create and store screenshots in s3 bucket',
    start_date = datetime(2023,4,24),
    schedule = '02 02 * * *'
)

def etl():
    @task()
    def capture_screenshot(url, output_path, coords):
        options = Options()
        options.add_argument('--headless') 
        options.add_argument('--disable-gpu') 

        driver = webdriver.Chrome(options=options)

        try:
            driver.set_window_size(2560, 1440)
            driver.get(url)
            time.sleep(3)
            #Sleep allows page load.
            driver.save_screenshot(output_path)
                    
            im = Image.open(output_path)
            im_cropped = im.crop(coords)
            im_cropped.save(output_path)
        finally:
            driver.quit()
            
            
    def run_screenshots():
        if not os.path.exists("../Screenshots"):
            os.makedirs("../Screenshots")
            os.makedirs("../Screenshots/Fundamentals")
            os.makedirs("../Screenshots/Chains")
            os.makedirs("../Screenshots/Blockspace")
        with open('../../src/misc/navigation_data.json', 'r') as file:
            data = json.load(file)
        #Generate folders for image if not existing
        
        for i in data['Fundamentals']["options"]:
            url = "https://www.growthepie.xyz/fundamentals/"
            url = url + "" + i["urlKey"]
            path = "Screenshots/" + "Fundamentals/" + i["urlKey"] + ".png"
            capture_screenshot(url, path, i["coords"] if "coords" in i else FUND_CAPTURE)

            
        for i in data['Chains']["options"]:
            url = "https://www.growthepie.xyz/chains/"
            url = url + "" + i["urlKey"]
            path = "Screenshots/" + "Chains/" + i["urlKey"] + ".png"
            capture_screenshot(url, path, i["coords"] if "coords" in i else CHAIN_CAPTURE)
            
        for i in data['Blockspace']["options"]:
            url = "https://www.growthepie.xyz/blockspace/"
            url = url + "" + i["urlKey"]
            path = "Screenshots/" + "Blockspace/" + i["urlKey"] + ".png"
            capture_screenshot(url, path, i["coords"] if "coords" in i else BLOCK_CAPTURE)
        #Capture screenshot loop for each image group
    
    run_screenshots()

etl()

