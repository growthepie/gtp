import os
from src.db_connector import DbConnector
from src.misc.octant_v2 import OctantV2
import getpass
import dotenv
import getpass

sys_user = getpass.getuser()
dotenv.load_dotenv()

print("env vars loaded", os.getenv("S3_CF_BUCKET"), os.getenv(
    "CF_DISTRIBUTION_ID"), os.getenv("DB_HOST"))

api_version = "v1"
# use postgres for ubuntu user, sqlite for other users (local development)
# use_sqlite = False if sys_user == "ubuntu" else True
# db_connector = DbConnector(use_sqlite=True)
db_connector = DbConnector(use_sqlite=True)

octantv2 = OctantV2(os.getenv("S3_CF_BUCKET"), os.getenv(
    "CF_DISTRIBUTION_ID"), db_connector, api_version, user=sys_user)

# octantv2.load_user_data(4)
# octantv2.run()
octantv2.run_load_octant_data_for_all_epochs()
# octantv2.run_load_latest_epoch_data()
octantv2.run_create_all_octant_jsons()
# octantv2.create_project_metadata_json()
