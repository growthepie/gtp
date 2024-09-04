import os
import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from dotenv import load_dotenv
load_dotenv()

from datetime import datetime, timedelta
from src.adapters.rpc_funcs.gtp_ai import GTPAI, convert_timestamps
import pandas as pd
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook
from pendulum import timezone

CET = timezone("Europe/Paris")

@dag(
    default_args={
        'owner': 'nader',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook,
    },
    dag_id='gtp_ai',
    description='Generate AI Insights',
    tags=['ai', 'milestones', 'metrics'],
    start_date=CET.convert(datetime(2023, 9, 1, 8, 0)),
    schedule_interval='30 6 * * *',
    catchup=False  # Ensures only future runs are scheduled, not backfilled
)

def gtp_ai():
    @task(execution_timeout=timedelta(minutes=45))
    def run_ai():
        # Load from environment variables
        print("Loading environment variables...")
        url = os.getenv("GTP_URL")
        local_filename = os.getenv("GTP_AI_LOCAL_FILENAME")
        webhook_url = os.getenv("GTP_AI_WEBHOOK_URL")

        if not url or not local_filename or not webhook_url:
            raise ValueError("Environment variables for URL, local filename, or webhook URL are not set.")
        print("Environment variables loaded successfully!")

        analytics = GTPAI()

        # Fetch and process data
        print(f"Fetching data from URL: {url} and saving to {local_filename}...")
        analytics.fetch_json_data(url, local_filename)
        print("Data fetched successfully! Data saved to:", local_filename)

        metrics = ["daa", "txcount", "market_cap_usd", "gas_per_second", "tvl"]
        print(f"Filtering data for metrics: {metrics}...")
        filtered_data = analytics.filter_data(file_path=local_filename, metrics=metrics)
        print(f"Data filtered successfully! Filtered data length: {len(filtered_data)}")

        print("Organizing filtered data...")
        organized_data = analytics.organize_data(filtered_data)
        print(f"Data organized successfully! Organized data length: {len(organized_data)}")

        print("Converting organized data to dataframe...")
        df = analytics.json_to_dataframe(organized_data)
        df['date'] = pd.to_datetime(df['date'])
        df.sort_values(by='date', ascending=False, inplace=True)
        print(f"Dataframe created and sorted by date! Dataframe length: {len(df)}")

        print("Ranking origins by TVL...")
        df = analytics.rank_origins_by_tvl(df)
        print("Origins ranked by TVL! Preview of ranked dataframe:\n", df.head())

        # Detect and analyze milestones
        print("Detecting chain milestones...")
        chain_milestones = analytics.detect_chain_milestones(df, analytics.metric_milestones)
        print(f"Detected {len(chain_milestones)} chain milestones. First milestone preview:\n", chain_milestones[0] if chain_milestones else "No milestones detected")

        print("Getting latest milestones...")
        latest_milestones = analytics.get_latest_milestones(chain_milestones, n=3, day_interval=1)
        latest_milestones = sorted(latest_milestones, key=lambda x: (x['date'], -x['total_importance']))[:10]
        print(f"Selected top {len(latest_milestones)} latest milestones. First latest milestone preview:\n", latest_milestones[0] if latest_milestones else "No latest milestones selected")

        print("Analyzing cross-chain milestones...")
        cross_chain_milestones = analytics.analyze_cross_chain_milestones(df, analytics.cross_chain_milestones)
        cross_chain_milestones = [milestone for milestone in cross_chain_milestones if pd.to_datetime(milestone['date'], format='%d.%m.%Y') >= pd.Timestamp.now() - pd.Timedelta(days=3)]
        print(f"Analyzed {len(cross_chain_milestones)} cross-chain milestones. First cross-chain milestone preview:\n", cross_chain_milestones[0] if cross_chain_milestones else "No cross-chain milestones detected")

        # Organize single-chain milestones based on metric and include date
        print("Organizing single-chain milestone data...")
        chain_data = {}
        for milestone in latest_milestones:
            origin = milestone['origin']
            date = milestone['date']
            if isinstance(date, pd.Timestamp):
                date = date.strftime('%d.%m.%Y')
            metric = milestone['metric']
            if origin not in chain_data:
                chain_data[origin] = {}
            if date not in chain_data[origin]:
                chain_data[origin][date] = {}
            if metric not in chain_data[origin][date]:
                chain_data[origin][date][metric] = []
            chain_data[origin][date][metric].append(milestone)
        print(f"Single-chain milestone data organized. Number of origins: {len(chain_data)}")

        # Organize cross-chain milestones based on metric and include date
        print("Organizing cross-chain milestone data...")
        cross_chain_data = {}
        for milestone in cross_chain_milestones:
            origin = milestone['origin']
            date = milestone['date']
            if isinstance(date, pd.Timestamp):
                date = date.strftime('%d.%m.%Y')
            metric = milestone['metric']
            if origin not in cross_chain_data:
                cross_chain_data[origin] = {}
            if date not in cross_chain_data[origin]:
                cross_chain_data[origin][date] = {}
            if metric not in cross_chain_data[origin][date]:
                cross_chain_data[origin][date][metric] = []
            cross_chain_data[origin][date][metric].append(milestone)
        print(f"Cross-chain milestone data organized. Number of origins: {len(cross_chain_data)}")

        # Organize data for JSON agent
        print("Combining single-chain and cross-chain data...")
        combined_data = {
            "single_chain_milestones": chain_data,
            "cross_chain_milestones": cross_chain_data
        }

        # Convert any remaining Timestamps to strings
        print("Converting timestamps to strings...")
        combined_data = convert_timestamps(combined_data)
        print("Timestamps converted. Combined data preview:", {k: v for k, v in combined_data.items() if v})  # Print only non-empty data

        # Analyze Layer 2 milestones
        print("Generating milestone responses...")
        responses = analytics.generate_milestone_responses(combined_data)
        print(f"Milestone responses generated. Number of responses: {len(responses)}")

        # Send the response as a Discord embed message
        print("Crafting and sending Discord embed message...")
        title = "Layer 2 Blockchain Milestone Update"
        footer = f"Analysis as of {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data compiled from latest metrics"
        analytics.craft_and_send_discord_embeds(webhook_url, responses, title, footer)
        print("Discord embed message sent successfully.")

    run_ai()
gtp_ai()
    