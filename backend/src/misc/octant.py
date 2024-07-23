import requests
import time
import json
import copy

def fetch_graph_ql(url: str, query: str, variables: dict):
    response = requests.post(
        url,
        json={"query": query, "variables": variables},
        headers={"Content-Type": "application/json"},
    )
    return response.json()

def fetch_rest(url: str):
    response = requests.get(
        url,
        headers={"Content-Type": "application/json"},
    )
    return response.json()


class Octant():
    def __init__(self):
        self.rest_url = "https://backend.mainnet.octant.app"
        self.gql_url = "https://graph.mainnet.octant.app/subgraphs/name/octant"

    # get the project metadata
    def get_project_metadata(self, address: str, cid: str):
        return fetch_rest(f"https://turquoise-accused-gayal-88.mypinata.cloud/ipfs/{cid}/{address}")

    # get the last epoch
    def get_last_epoch(self):
        last_epoch = fetch_rest(self.rest_url + "/epochs/current")
        last_epoch = last_epoch["currentEpoch"]

        print(f"last epoch retrieved: {last_epoch}")
        return last_epoch

    # get epoch start and end times
    def get_epoch_start_end_times(self, last_epoch: int):
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
        epochStartEndTimes = fetch_graph_ql(
            self.gql_url, query, {"lastEpoch": last_epoch})

        epochStartEndTimes = epochStartEndTimes["data"]["epoches"]

        print(f"epoch start and end times retrieved: {epochStartEndTimes}")
        return epochStartEndTimes

    def run_epoch_data_retrieval(self, epoch):
        print(f"fetching data for epoch: {epoch['epoch']}")

        # get the epoch number
        epoch_number = int(epoch["epoch"])

        # get the epoch stats
        epoch_stats = fetch_rest(self.rest_url + f"/epochs/info/{epoch['epoch']}")

        epoch_projects_metadata = fetch_rest(
            self.rest_url + f"/projects/epoch/{epoch['epoch']}")
        print("epoch_projects_metadata", epoch_projects_metadata)

        # get the project addresses and the projects cid
        project_addresses = epoch_projects_metadata["projectsAddresses"]
        projects_cid = epoch_projects_metadata["projectsCid"]

        epoch_budgets = fetch_rest(
            self.rest_url + f"/rewards/budgets/epoch/{epoch['epoch']}")
        epoch_budgets = epoch_budgets["budgets"] if "budgets" in epoch_budgets else [
        ]

        epoch_estimated_rewards = fetch_rest(
            self.rest_url + f"/rewards/projects/estimated")
        epoch_estimated_rewards = epoch_estimated_rewards["rewards"] if "rewards" in epoch_estimated_rewards else [
        ]

        epoch_finalized_rewards = fetch_rest(
            self.rest_url + f"/rewards/projects/epoch/{epoch['epoch']}")
        epoch_finalized_rewards = epoch_finalized_rewards["rewards"] if "rewards" in epoch_finalized_rewards else [
        ]

        epoch_rewards_threshold = fetch_rest(
            self.rest_url + f"/rewards/threshold/{epoch['epoch']}")
        epoch_rewards_threshold = int(
            epoch_rewards_threshold["threshold"]) if "threshold" in epoch_rewards_threshold and epoch_rewards_threshold["threshold"] else None

        epoch_allocations = fetch_rest(
            self.rest_url + f"/allocations/epoch/{epoch['epoch']}")
        epoch_allocations = epoch_allocations["allocations"] if "allocations" in epoch_allocations else [
        ]

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
        highest_allocation = max(total_allocated_by_project.items(), key=lambda x: x[1])[1] if total_allocated_by_project and len(total_allocated_by_project) > 0 else None

        finalized_rewards_by_project = []
        if epoch_finalized_rewards and len(epoch_finalized_rewards) > 0:
            for reward in epoch_finalized_rewards:
                finalized_rewards_by_project.append({
                    "address": reward["address"],
                    "allocated": int(reward["allocated"]),
                    "matched": int(reward["matched"]),
                    "total": int(reward["allocated"]) + int(reward["matched"]),
                })

        estimated_rewards_by_project = []
        if epoch_estimated_rewards and len(epoch_estimated_rewards) > 0:
            for reward in epoch_estimated_rewards:
                estimated_rewards_by_project.append({
                    "address": reward["address"],
                    "allocated": int(reward["allocated"]),
                    "matched": int(reward["matched"]),
                    "total": int(reward["allocated"]) + int(reward["matched"]),
                })

        # if epoch_number >= 4, do a simple calculation to get the matched rewards
        if epoch_number >= 4:
            for index, project in enumerate(estimated_rewards_by_project):
                address = project["address"]
                total_to_subtract = total_allocated_by_project[address] if address in total_allocated_by_project else 0
                estimated_rewards_by_project[index]["matched"] = estimated_rewards_by_project[index]["total"] - total_to_subtract

        print(f"length of finalized_rewards_by_project: {len(finalized_rewards_by_project)}")
        total_rewards_rank_by_project = finalized_rewards_by_project if finalized_rewards_by_project and len(finalized_rewards_by_project) > 0 else estimated_rewards_by_project
        total_rewards = copy.deepcopy(total_rewards_rank_by_project)

        # sort the projects by total rewards
        total_rewards_rank_by_project = sorted(total_rewards_rank_by_project, key=lambda x: x["total"], reverse=True)

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

        ## drop "total" key from estimated_rewards_by_project and finalized_rewards_by_project and cast allocated and matched to string
        estimated_rewards_by_project_clean = copy.deepcopy(estimated_rewards_by_project) ## necessary to avoid modifying the original list that is used later
        for project in estimated_rewards_by_project_clean:
            project.pop("total", None)
            project["allocated"] = str(project["allocated"])
            project["matched"] = str(project["matched"])

        finalized_rewards_by_project_clean = copy.deepcopy(finalized_rewards_by_project) ## necessary to avoid modifying the original list that is used later
        for project in finalized_rewards_by_project_clean:
            project.pop("total", None)
            project["allocated"] = str(project["allocated"])
            project["matched"] = str(project["matched"])

        projects_data = []
        for address in project_addresses:
            # check if the threshold is reached
            threshold_reached = False
            if epoch_number >= 4:
                if address in allocations_by_project:
                    unique_donors = set(allocation['donor'] for allocation in allocations_by_project[address])
                    threshold_reached = len(unique_donors) > 0
                else:
                    threshold_reached = False
            else:
                threshold_reached = total_allocated_by_project.get(address, 0) >= epoch_rewards_threshold

            # get the project metadata
            project_metadata = self.get_project_metadata(address, projects_cid)

            if address in [x['address'] for x in total_rewards_rank_by_project]:
                rank = [i for i, x in enumerate(total_rewards_rank_by_project) if x['address'] == address][0]
            else:
                rank = None

            total_rewards_clean = copy.deepcopy(total_rewards) ## necessary to avoid modifying the original list that is used later (in this case because of address pop)
            if address in [x['address'] for x in total_rewards_clean]:
                rewards_total = [x["total"] for x in total_rewards_clean if x['address'] == address][0]
                rewards_matched = [x["matched"] for x in total_rewards_clean if x['address'] == address][0]
                rewards = [x for x in total_rewards_clean if x['address'] == address][0]
                rewards.pop("address", None)
            else:
                rewards_total = 0
                rewards_matched = 0
                rewards = {
                    "allocated": 0,
                    "matched": 0,
                    "total": 0
                }

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
                "percentageThresholdOfTotalAllocated": epoch_rewards_threshold / total_allocated_by_project[address] if address in total_allocated_by_project and total_allocated_by_project[address] > 0 and epoch_rewards_threshold else None,
                
                # for rank, find the index where the address is in the sorted list of projects
                "rank": rank,
                "rewardsTotal": rewards_total,
                "rewardsMatched": rewards_matched,
                "rewards": rewards,
                "thresholdReached": threshold_reached,
            })

        ## from_timestamp in format YYYY-MM-DDTHH:MM:SSZ based on epoch_start_end["fromTs"]
        from_timestamp = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(int(epoch_start_end["fromTs"])))
        to_timestamp = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(int(epoch_start_end["toTs"])))
        decision_window_timestamp = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(int(epoch_start_end["toTs"]) + int(epoch_start_end["decisionWindow"])))

        if len(finalized_rewards_by_project) == 0:
            fin_rewards = None
        else:
            fin_rewards = finalized_rewards_by_project_clean

        epoch_data = {
            "stats": epoch_stats,
            "state": epoch_state,
            "epoch": epoch_number,
            "fromTimestamp": from_timestamp,
            "toTimestamp": to_timestamp,
            "decisionWindow": decision_window_timestamp,
            "estimatedRewards": estimated_rewards_by_project_clean,
            "finalizedRewards": fin_rewards,
            "rewardsThreshold": epoch_rewards_threshold if epoch_rewards_threshold else 0,
            "highestProjectAllocation": highest_allocation,
            "projects": projects_data,
        }
    
        return epoch_data

    def get_epochs_data(self, epochStartEndTimes: list):
        epochs_data = []
        # loop through the epochs and get the data
        for epoch in epochStartEndTimes:
            epoch_data = self.run_epoch_data_retrieval(epoch)
            epochs_data.append(epoch_data)

        return json.dumps(epochs_data, indent=2)