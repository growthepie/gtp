from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from src.misc.helper_functions import upload_png_to_cf_s3
from PIL import Image
import time
import os

FUND_CAPTURE = [1320, 350, 2075, 775]
CHAIN_CAPTURE = [765, 245, 2070, 600] 
BLOCK_CAPTURE = [1320, 350, 2075, 770]

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

        return im_cropped
    finally:
        driver.quit()

def run_screenshots(s3_bucket, cf_distribution_id, api_version, user = None):
    if user == 'ubuntu':
        main_path = '../gtp/backend/src/api/screenshots'
    else:
        main_path = '../backend/src/api/screenshots'

    print(f"Running screenshots: storing them in {main_path} and uploading to {s3_bucket}")
    main_url = 'https://www.growthepie.xyz'

    #Generate folders for image if not existing
    if not os.path.exists(main_path):
        os.makedirs(main_path)
        os.makedirs(f"{main_path}/fundamentals")
        os.makedirs(f"{main_path}/chains")
        os.makedirs(f"{main_path}/blockspace")
 
    for key in screenshot_data:
        label = key.lower()
        for i in screenshot_data[key]["options"]:
            url = f"{main_url}/{label}/{i['urlKey']}"
            path = f"{main_path}/{label}/{i['urlKey']}.png"
            capture_screenshot(url, path, i["coords"] if "coords" in i else FUND_CAPTURE)
            
            s3_path = f'{api_version}/og_images/{label}/{i["urlKey"]}'
            upload_png_to_cf_s3(s3_bucket, s3_path, path, cf_distribution_id)

        
## This is the data structure for the screenshot data
screenshot_data = {
    "Fundamentals": {
      "label": "Fundamentals",
      "key": "metrics",
      "options": [
        {
          "label": "Daily Active Addresses",
          "urlKey": "daily-active-addresses"
        },
        {
          "label": "Transaction Count",
          "urlKey": "transaction-count"
        },
        {
          "label": "Stablecoin Market Cap",
          "urlKey": "stablecoin-market-cap"
        },
        {
          "label": "Total Value Locked",
          "urlKey": "total-value-locked"
        },
        {
          "label": "Fees Paid by Users",
          "urlKey": "fees-paid-by-users"
        },
        {
          "label": "Rent Paid to L1",
          "urlKey": "rent-paid"
        },
        {
          "label": "Onchain Profit",
          "urlKey": "profit"
        },
        {
          "label": "Fully Diluted Valuation",
          "urlKey": "fully-diluted-valuation",
          "coords": [1325, 380, 2075, 800]
        },
        {
          "label": "Market Cap",
          "urlKey": "market-cap",
          "coords": [1325, 380, 2075, 800]
        },
        
        {
          "label": "Transaction Costs",
          "urlKey": "transaction-costs",
          "coords": [1325, 370, 2075, 790]
        }
      ]
    },
    "Blockspace": {
        "label": "Blockspace", 
        "options": [
          {
            "label": "Chain Overview",
            "urlKey": "chain-overview",
            "coords" : [760, 310, 2075, 1040]
          },
          {
            "label": "Category Comparison",
            "urlKey": "category-comparison",
            "coords" : [765, 315, 2070, 1020] 
          }
        ]
      },
      "Chains": {
        "label": "Single Chain",
        "key": "chains",
        "options": [
          {
            "label": "Ethereum",
            "urlKey": "ethereum",
            "coords": [765, 320, 2070, 705] 
          },
          {
            "label": "Base",
            "urlKey": "base",
            "coords": [765, 295, 2070, 680]
          },
          {
            "label": "OP Mainnet",
            "urlKey": "optimism",
            "coords": [765, 345, 2070, 730]
          },
          {
            "label": "Public Goods Network",
            "urlKey": "public-goods-network",
            "coords": [765, 295, 2070, 680]
          },
          {
            "label": "Zora",
            "urlKey": "zora",
            "coords": [765, 295, 2070, 680] 
          },
          {
            "label": "Arbitrum",
            "urlKey": "arbitrum",
            "coords": [765, 320, 2070, 705] 
          },
          {
            "label": "Polygon zkEVM",
            "urlKey": "polygon-zkevm",
            "coords": [765, 320, 2070, 705]
          },
          {
            "label": "zkSync Era",
            "urlKey": "zksync-era",
            "coords": [765, 320, 2070, 705] 
          },
          {
            "label": "Linea",
            "urlKey": "linea",
            "coords": [765, 320, 2070, 705]
          },
          {
            "label": "Immutable X",
            "urlKey": "immutable-x",
            "coords": [765, 320, 2070, 705] 
          },
          {
            "label": "Blast",
            "urlKey": "blast",
            "coords": [765, 320, 2070, 705]
          },
          {
            "label": "Mode Network",
            "urlKey": "mode",
            "coords": [765, 295, 2070, 680]
          }
        ]
      }
  }