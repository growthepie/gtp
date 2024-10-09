# helpers.py

import requests
import pandas as pd
from decimal import Decimal
import base64
import re
from eth_utils import to_checksum_address
from sgqlc.operation import Operation
from sgqlc.endpoint.requests import RequestsEndpoint
from src.misc.octant_lib.codegen.gql.schema import schema
import logging

# Constants
GLM_TOKEN_DECIMALS = 18
ETH_TOKEN_DECIMALS = 18
PAGE_SIZE = 100

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Column definitions for each table with data types for both SQLite and PostgreSQL
table_definitions = {
    'projects_metadata': {
        'table_name': 'octantv2_projects_metadata',
        'columns': [
            ('id', 'TEXT'),
            ('epoch', 'INTEGER'),
            ('address', 'TEXT'),
            ('project_key', 'TEXT'),
            ('cid', 'TEXT'),
            ('name', 'TEXT'),
            ('intro_description', 'TEXT'),
            ('description', 'TEXT'),
            ('profile_image_small', 'TEXT'),
            ('profile_image_medium', 'TEXT'),
            ('profile_image_large', 'TEXT'),
            ('website_label', 'TEXT'),
            ('website_url', 'TEXT')
        ],
        'primary_key': ['id']
    },
    'user_lock_history': {
        'table_name': 'octantv2_user_lock_history',
        'columns': [
            ('id', 'TEXT'),
            # Unix timestamp, can use INTEGER in both SQLite and PostgreSQL
            ('timestamp', 'INTEGER'),
            ('datetime', 'TIMESTAMP'),
            ('block_number', 'INTEGER'),
            ('user_address', 'TEXT'),
            # SQLite uses REAL for floating point numbers, PostgreSQL uses FLOAT
            ('amount', 'REAL'),
            ('current_total_locked', 'REAL'),
            ('current_total_locked_for_user', 'REAL')
        ],
        'primary_key': ['id']
    },
    'user_allocations': {
        'table_name': 'octantv2_user_allocations',
        'columns': [
            ('id', 'TEXT'),
            ('epoch', 'INTEGER'),
            ('donor', 'TEXT'),
            ('project', 'TEXT'),
            ('project_key', 'TEXT'),
            ('amount', 'REAL')
        ],
        'primary_key': ['id']
    },
    'user_budgets': {
        'table_name': 'octantv2_user_budgets',
        'columns': [
            ('id', 'TEXT'),
            ('epoch', 'INTEGER'),
            ('address', 'TEXT'),
            ('amount', 'REAL')
        ],
        'primary_key': ['id']
    },
    'project_allocations_and_matched_rewards': {
        'table_name': 'octantv2_project_allocations_and_matched_rewards',
        'columns': [
            ('id', 'TEXT'),
            ('epoch', 'INTEGER'),
            ('address', 'TEXT'),
            ('project_key', 'TEXT'),
            ('allocated', 'REAL'),
            ('matched', 'REAL'),
            ('total', 'REAL'),
            ('donor_count', 'INTEGER'),
            ('donor_list', 'JSON')
        ],
        'primary_key': ['id']
    },
    'user_data': {
        'table_name': 'octantv2_user_data',
        'columns': (
            ('id', 'TEXT'),
            ('epoch', 'INTEGER'),
            ('user_address', 'TEXT'),
            ('locked', 'REAL'),
            ('min', 'REAL'),
            ('max', 'REAL'),
            ('budget_amount', 'REAL'),
            ('allocation_amount', 'REAL'),
            ('allocated_to_project_count', 'INTEGER'),
            ('allocated_to_project_keys', 'JSON')
        ),
        'primary_key': ['id']
    },

}

# Create an endpoint for the Octant subgraph
endpoint = RequestsEndpoint(
    "https://graph.mainnet.octant.app/subgraphs/name/octant")


def generate_create_table_sql(table_key, db_type='sqlite'):
    """
    Generate the SQL CREATE TABLE statement for a given table based on its name and database type.

    :param table_name: Name of the table
    :param db_type: Type of the database ('sqlite' or 'postgres')
    :return: SQL CREATE TABLE statement as a string
    """

    if table_key not in table_definitions:
        raise ValueError(f"Table key '{table_key}' is not defined.")

    table_def = table_definitions[table_key]

    if 'table_name' not in table_def or 'columns' not in table_def:
        raise ValueError(f"Table definition for '{table_key}' is invalid.")

    table_name = table_def['table_name']
    columns_definitions = table_def['columns']
    primary_key = table_def.get('primary_key', [])

    # Define type mappings for different databases
    type_mappings = {
        'TEXT': 'TEXT',
        'JSON': 'JSON',
        'INTEGER': 'INTEGER',
        'REAL': 'REAL',
        'TIMESTAMP': 'TIMESTAMP' if db_type == 'postgres' else 'DATETIME'
    }

    # Build the CREATE TABLE statement
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    column_defs = [f"{col_name} {type_mappings[col_type]}" for col_name,
                   col_type in columns_definitions]

    # Add PRIMARY KEY constraint if specified
    if primary_key:
        pk = ", ".join(primary_key)
        column_defs.append(f"PRIMARY KEY ({pk})")

    sql += ", ".join(column_defs)
    sql += ");"

    return sql


def fetch_rest(url: str):
    """
    Fetches data from a given URL using the REST API.

    :param url: URL to fetch data from
    :return: JSON response from the URL
    """
    response = requests.get(
        url,
        headers={"Content-Type": "application/json"},
    )
    return response.json()


def get_epoch_start_end_times(epoch: int):
    """
    Gets the start and end times for a given epoch from the Octant subgraph.

    :param epoch: Epoch number
    :return: Dictionary containing the start and end times for the epoch
    """
    op = Operation(schema.Query)  # Reset the operation here
    op.epoches(where={"epoch": epoch})
    data = endpoint(op)
    epochStartEndTimes = data['data']['epoches']

    # add datetime fields to the epoch start and end times
    for epoch in epochStartEndTimes:
        epoch['fromDatetime'] = pd.to_datetime(int(epoch['fromTs']), unit='s')
        epoch['toDatetime'] = pd.to_datetime(int(epoch['toTs']), unit='s')

    logging.info(
        f"Epoch {epoch['epoch']} start and end times: {epochStartEndTimes}")

    return epochStartEndTimes[0]


def get_lockeds():
    """
    Gets all locked transactions from the Octant subgraph.

    :return: List of dictionaries containing the locked transactions
    """
    result = []
    skip = 0
    while True:
        op = Operation(schema.Query)  # Reset the operation here
        op.lockeds(first=PAGE_SIZE, skip=skip,
                   order_by="timestamp", order_direction='asc')
        data = endpoint(op)
        locked_transactions = data['data']['lockeds']
        for locked in locked_transactions:
            # checksum the user address
            locked['user'] = to_checksum_address(locked['user'].strip())
        result.extend(locked_transactions)
        skip += PAGE_SIZE

        if len(locked_transactions) < PAGE_SIZE:
            break

    return result


def get_unlockeds():
    """
    Gets all unlocked transactions from the Octant subgraph.

    :return: List of dictionaries containing the unlocked transactions
    """
    result = []
    skip = 0
    while True:
        op = Operation(schema.Query)  # Reset the operation here
        op.unlockeds(first=PAGE_SIZE, skip=skip,
                     order_by="timestamp", order_direction='asc')
        data = endpoint(op)
        unlocked_transactions = data['data']['unlockeds']
        for unlocked in unlocked_transactions:
            # checksum the user address
            unlocked['user'] = to_checksum_address(unlocked['user'].strip())
        result.extend(unlocked_transactions)
        skip += PAGE_SIZE

        if len(unlocked_transactions) < PAGE_SIZE:
            break

    return result


def get_last_epoch():
    """Gets the latest epoch number from the Octant backend."""
    resp = fetch_rest("https://backend.mainnet.octant.app/epochs/current")
    return resp["currentEpoch"] if "currentEpoch" in resp else None


def get_epoch_projects(epoch_info: dict):
    """
    Gets the projects for a given epoch from the Octant backend.

    :param epoch_info: Dictionary containing the epoch number
    :return: Dictionary containing the project addresses and CID
    """
    resp = fetch_rest(
        f"https://backend.mainnet.octant.app/projects/epoch/{epoch_info['epoch']}")

    return {
        "projectsAddresses": resp["projectsAddresses"] if "projectsAddresses" in resp else [],
        "projectsCid": resp["projectsCid"] if "projectsCid" in resp else None
    }


def get_project_metadata_from_ipfs(address: str, cid: str):
    """
    Gets the metadata for a project from IPFS using the project's address and CID

    :param address: Address of the project
    :param cid: CID of the project metadata
    :return: Dictionary containing the project metadata
    """
    resp = fetch_rest(
        f"https://turquoise-accused-gayal-88.mypinata.cloud/ipfs/{cid}/{address}")
    return {
        "name": resp["name"] if "name" in resp else None,
        "introDescription": resp["introDescription"] if "introDescription" in resp else None,
        "description": resp["description"] if "description" in resp else None,
        "profileImageSmall": resp["profileImageSmall"] if "profileImageSmall" in resp else None,
        "profileImageMedium": resp["profileImageMedium"] if "profileImageMedium" in resp else None,
        "profileImageLarge": resp["profileImageLarge"] if "profileImageLarge" in resp else None,
        "websiteLabel": resp["website"]["label"] if "website" in resp and "label" in resp["website"] else None,
        "websiteUrl": resp["website"]["url"] if "website" in resp and "url" in resp["website"] else None
    }


def process_possible_base64(s: str):
    """
    Decode a string if it is a valid Base64 string.

    :param s: Input string
    :return: Decoded string if it is a valid Base64 string, otherwise the input string
    """
    # Check if the string length is a multiple of 4 and contains only valid Base64 characters
    if len(s) % 4 != 0 or not re.match('^[A-Za-z0-9+/=]*$', s):
        return s

    # Check if the string is a valid Base64 string
    try:
        return base64.b64decode(s).decode("utf-8")
    except:
        return s


def get_epoch_projects_metadata(epoch_info: dict):
    """
    Gets the metadata for all projects in a given epoch.

    :param epoch_info: Dictionary containing the epoch number
    :return: List of dictionaries containing the project metadata
    """
    projects_metadata = []
    epoch_projects = get_epoch_projects(epoch_info)
    project_addresses = epoch_projects["projectsAddresses"]
    projects_cid = epoch_projects["projectsCid"]

    for index, address in enumerate(project_addresses):
        project_metadata = get_project_metadata_from_ipfs(
            address, projects_cid)
        project_metadata["address"] = address
        project_metadata["cid"] = projects_cid
        # check if the description field is base64 string, if so, decode it
        project_metadata["description"] = process_possible_base64(
            project_metadata["description"])
        projects_metadata.append(project_metadata)

        # add an id to each project row so we can upsert if needed, the id should be the epoch+address
        projects_metadata[index]["id"] = f"{epoch_info['epoch']}_{address}"
        # add the epoch number to each project row
        projects_metadata[index]["epoch"] = epoch_info["epoch"]
        projects_metadata[index]["project_key"] = project_metadata["websiteLabel"].strip(
        ) if project_metadata["websiteLabel"] else project_metadata["name"].strip()

    return projects_metadata


def get_budgets(epoch_info: dict, users_with_locked_balances: list):
    """
    Gets the budgets for a given epoch from the Octant backend.

    :param epoch_info: Dictionary containing the epoch number
    :param users_with_locked_balances: List of user addresses with locked balances

    :return: List of dictionaries containing the budgets
    """
    if epoch_info["has_allocation_started"]:
        resp = fetch_rest(
            f"https://backend.mainnet.octant.app/rewards/budgets/epoch/{epoch_info['epoch']}")

        budgets = resp["budgets"] if "budgets" in resp else []

    else:
        budgets = []
        for user in users_with_locked_balances:
            ''' trying to get the upcoming budget for the user is very slow so skipping it for now
            upcoming_budget = get_upcoming_budget_for_user(user)
            if upcoming_budget:
                budgets.append({
                    "id": f"{epoch_info['epoch']}_{user}",
                    "address": user,
                    "amount": upcoming_budget
                })
            '''

            budgets.append({
                "address": user,
                "amount": None
            })

    for budget in budgets:
        # add an id to each budget row so we can upsert if needed, the id should be the epoch+address
        budget["id"] = f"{epoch_info['epoch']}_{budget['address']}"
        # add the epoch number to each budget row
        budget["epoch"] = epoch_info["epoch"]
        # convert the amount to a Decimal if it is not None and divide by the token decimals
        budget["amount"] = Decimal(
            budget["amount"]) / (10 ** ETH_TOKEN_DECIMALS) if budget["amount"] else None
        # checksum the address
        budget["address"] = to_checksum_address(budget["address"].strip())

    return budgets


def get_upcoming_budget_for_user(user_address: str):
    """
    Gets the upcoming budget for a given user from the Octant backend.

    :param user_address: Address of the user
    :return: Upcoming budget for the user
    """
    resp = fetch_rest(
        f"https://backend.mainnet.octant.app/rewards/budget/{user_address}/upcoming")

    upcoming_budget = resp["upcomingBudget"] if "upcomingBudget" in resp else None

    return upcoming_budget


def get_allocations(epoch_info: dict):
    """
    Gets the allocations for a given epoch from the Octant backend.

    :param epoch_info: Dictionary containing the epoch number
    :return: List of dictionaries containing the allocations
    """
    resp = fetch_rest(
        f"https://backend.mainnet.octant.app/allocations/epoch/{epoch_info['epoch']}")
    allocations = resp["allocations"] if "allocations" in resp else []

    for allocation in allocations:
        # add an id to each allocation value so we can upsert if needed, the id should be the epoch+donor+project
        allocation["id"] = f"{epoch_info['epoch']}_{allocation['donor']}_{allocation['project']}"
        # add the epoch number to each allocation row
        allocation["epoch"] = epoch_info["epoch"]
        # convert the amount to a Decimal and divide by the token decimals
        allocation["amount"] = Decimal(
            allocation["amount"]) / (10 ** ETH_TOKEN_DECIMALS) if allocation["amount"] else None
        # checksum the donor address
        allocation["donor"] = to_checksum_address(allocation["donor"].strip())

    return allocations


def get_rewards(epoch_info: dict):
    """
    Gets the rewards for a given epoch from the Octant backend.

    :param epoch_info: Dictionary containing the epoch number
    :return: List of dictionaries containing the rewards
    """
    if epoch_info["has_allocation_ended"]:
        resp = fetch_rest(
            f"https://backend.mainnet.octant.app/rewards/projects/epoch/{epoch_info['epoch']}")
        rewards = resp["rewards"] if "rewards" in resp else []
    else:
        resp = fetch_rest(
            f"https://backend.mainnet.octant.app/rewards/projects/estimated")
        rewards = resp["rewards"] if "rewards" in resp else []

    for reward in rewards:
        # add an id to each reward value so we can upsert if needed, the id should be the epoch+address
        reward["id"] = f"{epoch_info['epoch']}_{reward['address']}"
        # add the epoch number to each reward row
        reward["epoch"] = epoch_info["epoch"]
        # convert the allocated and matched amounts to Decimals and divide by the token decimals
        reward["allocated"] = Decimal(
            reward["allocated"]) / (10 ** ETH_TOKEN_DECIMALS) if reward["allocated"] else None
        reward["matched"] = Decimal(
            reward["matched"]) / (10 ** ETH_TOKEN_DECIMALS) if reward["matched"] else None
        # add a total field to each allocation row which is the sum of the allocated and matched amounts
        reward["total"] = reward["allocated"] + reward["matched"]
        # checksum the address
        reward["address"] = to_checksum_address(reward["address"].strip())

    return rewards
