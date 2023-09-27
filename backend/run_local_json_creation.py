import os

from src.db_connector import DbConnector

from src.api.json_creation import JSONCreation
from src.api.blockspace_json_creation import BlockspaceJSONCreation

from dotenv import load_dotenv

# Load LOCAL environment variables
load_dotenv(".env.local")

# Get environment variables
db_user = os.getenv("DB_USERNAME")
db_passwd = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_DATABASE")

# Create DbConnector object with environment variables
db_connector = DbConnector(db_name=db_name, db_user=db_user, db_passwd=db_passwd, db_host=db_host)

# Create JSON creation objects — with None for s3_bucket and cf_distribution_id (will not upload to S3 or invalidate CloudFront and will create JSONs locally)
json_creation = JSONCreation(s3_bucket=None, cf_distribution_id=None, db_connector=db_connector, api_version='v1')
blockspace_json_creation = BlockspaceJSONCreation(s3_bucket=None, cf_distribution_id=None, db_connector=db_connector, api_version='v1')

# Create JSONs
print('Creating Master, Landing Page, Metrics, and Chains JSONs...')
json_creation.create_all_jsons()

# Create Blockspace JSONs
print('Creating Blockspace JSONs...')
blockspace_json_creation.create_all_jsons()
