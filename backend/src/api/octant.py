import requests
import time
import json


def fetch_graph_ql(url: str, query: str, variables: dict):
    response = requests.post(
        url,
        json={"query": query, "variables": variables},
        headers={"Content-Type": "application/json"},
    )
    return response.json()


def fetch_rest(url: str):
    print(url)
    response = requests.get(
        url,
        headers={"Content-Type": "application/json"},
    )
    return response.json()


rest_url = "https://backend.mainnet.octant.app"
gql_url = "https://graph.mainnet.octant.app/subgraphs/name/octant"

# get the last epoch
last_epoch = fetch_rest(rest_url + "/epochs/current")
last_epoch = last_epoch["currentEpoch"]

print(last_epoch)

# get epoch start and end times
query = """
query GetEpochsStartEndTime($lastEpoch: Int) {
  epoches(first: $lastEpoch) {
    epoch
    toTs
    fromTs
    decisionWindow
  }
}
"""


def get_project_metadata(address: str, cid: str):
    return fetch_rest(f"https://turquoise-accused-gayal-88.mypinata.cloud/ipfs/{cid}/{address}")


epochStartEndTimes = fetch_graph_ql(
    gql_url, query, {"lastEpoch": last_epoch})

epochStartEndTimes = epochStartEndTimes["data"]["epoches"]

print(epochStartEndTimes)


epochs_data = []
# loop through the epochs and get the data
for epoch in epochStartEndTimes:
    # get the epoch number
    epoch_number = int(epoch["epoch"])

    # get the epoch stats
    epoch_stats = fetch_rest(rest_url + f"/epochs/info/{epoch['epoch']}")
    # print("epoch_stats", epoch_stats)

    epoch_projects_metadata = fetch_rest(
        rest_url + f"/projects/epoch/{epoch['epoch']}")
    print("epoch_projects_metadata", epoch_projects_metadata)

    # get the project addresses and the projects cid
    project_addresses = epoch_projects_metadata["projectsAddresses"]
    projects_cid = epoch_projects_metadata["projectsCid"]

    epoch_budgets = fetch_rest(
        rest_url + f"/rewards/budgets/epoch/{epoch['epoch']}")
    epoch_budgets = epoch_budgets["budgets"] if "budgets" in epoch_budgets else [
    ]
    # print("epoch_budgets", epoch_budgets)

    epoch_estimated_rewards = fetch_rest(
        rest_url + f"/rewards/projects/estimated")
    epoch_estimated_rewards = epoch_estimated_rewards["rewards"] if "rewards" in epoch_estimated_rewards else [
    ]
    # print("epoch_estimated_rewards", epoch_estimated_rewards)

    epoch_finalized_rewards = fetch_rest(
        rest_url + f"/rewards/projects/epoch/{epoch['epoch']}")
    epoch_finalized_rewards = epoch_finalized_rewards["rewards"] if "rewards" in epoch_finalized_rewards else [
    ]
    # print("epoch_finalized_rewards", epoch_finalized_rewards)

    epoch_rewards_threshold = fetch_rest(
        rest_url + f"/rewards/threshold/{epoch['epoch']}")
    epoch_rewards_threshold = int(
        epoch_rewards_threshold["threshold"]) if "threshold" in epoch_rewards_threshold and epoch_rewards_threshold["threshold"] else None
    # print("epoch_rewards_threshold", epoch_rewards_threshold)

    epoch_allocations = fetch_rest(
        rest_url + f"/allocations/epoch/{epoch['epoch']}")
    epoch_allocations = epoch_allocations["allocations"] if "allocations" in epoch_allocations else [
    ]
    # print("epoch_allocations", epoch_allocations)

    # get this epoch's start and end time info
    epoch_start_end = epoch

    budgets_by_project = {}  # dict of projects where the value is a list of budgets
    for budget in epoch_budgets:
        if budget["address"] not in budgets_by_project:
            budgets_by_project[budget["address"]] = []
        budgets_by_project[budget["address"]].append(budget)

    # dict of projects where the value is a list of allocations
    allocations_by_project = {}
    for allocation in epoch_allocations:
        if allocation["project"] not in allocations_by_project:
            allocations_by_project[allocation["project"]] = []
        allocations_by_project[allocation["project"]].append(allocation)

    total_allocated_by_project = {}
    if epoch_allocations and len(epoch_allocations) > 0:
        for allocation in epoch_allocations:
            if allocation["project"] not in total_allocated_by_project:
                total_allocated_by_project[allocation["project"]] = 0
            total_allocated_by_project[
                allocation["project"]
            ] += int(allocation["amount"])

    # get the highest project allocation
    highest_allocation = max(
        total_allocated_by_project.items(), key=lambda x: x[1]) if total_allocated_by_project and len(total_allocated_by_project) > 0 else 0

    finalized_rewards_by_project = {}

    if epoch_finalized_rewards and len(epoch_finalized_rewards) > 0:
        for reward in epoch_finalized_rewards:
            if reward["address"] not in finalized_rewards_by_project:
                finalized_rewards_by_project[reward["address"]] = {
                    "allocated": 0,
                    "matched": 0,
                    "total": 0,
                }
            finalized_rewards_by_project[reward["address"]]["allocated"] += int(
                reward["allocated"]
            )

            finalized_rewards_by_project[reward["address"]]["matched"] += int(
                reward["matched"]
            )

            finalized_rewards_by_project[reward["address"]]["total"] += int(
                reward["allocated"]) + int(reward["matched"])

    estimated_rewards_by_project = {}

    if epoch_estimated_rewards and len(epoch_estimated_rewards) > 0:
        for reward in epoch_estimated_rewards:
            if reward["address"] not in estimated_rewards_by_project:
                estimated_rewards_by_project[reward["address"]] = {
                    "allocated": 0,
                    "matched": 0,
                    "total": 0,
                }
            estimated_rewards_by_project[reward["address"]]["allocated"] += int(
                reward["allocated"]
            )

            estimated_rewards_by_project[reward["address"]]["matched"] += int(
                reward["matched"]
            )

            estimated_rewards_by_project[reward["address"]]["total"] += int(
                reward["allocated"]) + int(reward["matched"])

    # if epoch_number >= 4, do a simple calculation to get the matched rewards
    if epoch_number >= 4:
        for address in estimated_rewards_by_project:
            total_to_subtract = total_allocated_by_project[
                address] if address in total_allocated_by_project else 0
            estimated_rewards_by_project[address]["matched"] = estimated_rewards_by_project[address]["total"] - total_to_subtract

    total_rewards_rank_by_project = finalized_rewards_by_project if finalized_rewards_by_project and len(
        finalized_rewards_by_project) > 0 else estimated_rewards_by_project

    # sort the projects by total rewards
    total_rewards_rank_by_project = dict(sorted(total_rewards_rank_by_project.items(
    ), key=lambda item: item[1]["total"], reverse=True))

    # default to PENDING state
    epoch_state = "PENDING"

    if epoch_start_end:
        # get the current time
        now = int(time.time())

        # get the epoch start and end times
        epoch_start_time = int(epoch_start_end["fromTs"])
        epoch_end_time = int(epoch_start_end["toTs"])

        # get the decision window
        decision_window = int(epoch_start_end["decisionWindow"])

        # if the current time is greater than the epoch end time, the epoch is CLOSED
        if now >= epoch_start_time and now < epoch_end_time:
            epoch_state = "ACTIVE"
        elif now >= epoch_end_time and now < epoch_end_time + decision_window:
            epoch_state = "REWARD_ALLOCATION"
        elif now >= epoch_end_time + decision_window:
            epoch_state = "FINALIZED"

    projects_data = []
    for address in project_addresses:

        # check if the threshold is reached
        threshold_reached = False
        if epoch_number >= 4:
            if address in allocations_by_project:
                unique_donors = set(
                    allocation['donor'] for allocation in allocations_by_project[address])
                threshold_reached = len(unique_donors) > 0
            else:
                threshold_reached = False
        else:
            threshold_reached = total_allocated_by_project.get(
                address, 0) >= epoch_rewards_threshold

        # get the project metadata
        project_metadata = get_project_metadata(address, projects_cid)

        # add the project data to the projects_data list
        projects_data.append({
            "address": address,
            "name": project_metadata["name"] if "name" in project_metadata else None,
            "profileImageMedium": project_metadata["profileImageMedium"] if "profileImageMedium" in project_metadata else None,
            "website": project_metadata["website"] if "website" in project_metadata else None,
            "budgets": budgets_by_project[address] if address in budgets_by_project else [],
            "allocations": allocations_by_project[address] if address in allocations_by_project else [],
            "donors": len(set([allocation["donor"] for allocation in allocations_by_project[address]])) if address in allocations_by_project else 0,
            "totalAllocated": total_allocated_by_project[address] if address in total_allocated_by_project else 0,
            "percentageThresholdOfTotalAllocated": epoch_rewards_threshold / total_allocated_by_project[address] if address in total_allocated_by_project and total_allocated_by_project[address] > 0 and epoch_rewards_threshold else 0,
            # for rank, find the index where the address is in the sorted list of projects
            "rank": list(total_rewards_rank_by_project.keys()).index(address) if address in total_rewards_rank_by_project else None,
            "rewardsTotal": finalized_rewards_by_project[address]["total"] if address in finalized_rewards_by_project else estimated_rewards_by_project[address]["total"] if address in estimated_rewards_by_project else 0,
            "rewardsMatched": finalized_rewards_by_project[address]["matched"] if address in finalized_rewards_by_project else estimated_rewards_by_project[address]["matched"] if address in estimated_rewards_by_project else 0,
            "rewards": finalized_rewards_by_project[address] if address in finalized_rewards_by_project else estimated_rewards_by_project[address] if address in estimated_rewards_by_project else {
                "allocated": 0,
                "matched": 0,
                "total": 0
            },
            "thresholdReached": threshold_reached,
        })

    epochs_data.append({
        "stats": epoch_stats,
        "state": epoch_state,
        "epoch": epoch_number,
        "fromTimestamp": int(epoch_start_end["fromTs"]) * 1000 if epoch_start_end else None,
        "toTimestamp": int(epoch_start_end["toTs"]) * 1000 if epoch_start_end else None,
        "decisionWindow": int(epoch_start_end["toTs"]) * 1000 + int(epoch_start_end["decisionWindow"]) * 1000 if epoch_start_end else None,
        "estimatedRewards": estimated_rewards_by_project,
        "finalizedRewards": finalized_rewards_by_project,
        "rewardsThreshold": epoch_rewards_threshold if epoch_rewards_threshold else 0,
        "highestProjectAllocation": highest_allocation,
        "projects": projects_data,
    })

print(json.dumps(epochs_data, indent=2))
