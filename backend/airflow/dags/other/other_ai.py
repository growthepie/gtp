import os
import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from dotenv import load_dotenv
load_dotenv()

from datetime import datetime, timedelta
from src.adapters.rpc_funcs.gtp_ai import GTPAI
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
    schedule_interval='0 8 * * *',
    catchup=False  # Ensures only future runs are scheduled, not backfilled
)

def gtp_ai():
    @task(execution_timeout=timedelta(minutes=45))
    def run_ai():
        # Load from environment variables
        url = os.getenv("GTP_URL")
        local_filename = os.getenv("GTP_AI_LOCAL_FILENAME")
        webhook_url = os.getenv("GTP_AI_WEBHOOK_URL")

        if not url or not local_filename or not webhook_url:
            raise ValueError("Environment variables for URL, local filename, or webhook URL are not set.")

        analytics = GTPAI()
        
        # Fetch and process data
        analytics.fetch_json_data(url, local_filename)
        filtered_data = analytics.filter_data(file_path=local_filename, metrics=["daa", "txcount", "market_cap_usd"])
        print("Filtered data!")
        organized_data = analytics.organize_data(filtered_data)
        print("Organized data!")
        df = analytics.json_to_dataframe(organized_data)
        df['date'] = pd.to_datetime(df['date'])
        df.sort_values(by='date', ascending=False , inplace=True)
        print("Converted to dataframe!")
            
        # Detect and analyze milestones
        chain_milestones = analytics.detect_chain_milestones(df, analytics.metric_milestones)
        chain_milestones = [milestone for milestone in chain_milestones if pd.to_datetime(milestone['date'], format='%d.%m.%Y') >= pd.Timestamp.now() - pd.Timedelta(days=7)]

        cross_chain_milestones = analytics.analyze_cross_chain_milestones(df, analytics.cross_chain_milestones)
        cross_chain_milestones = [milestone for milestone in cross_chain_milestones if pd.to_datetime(milestone['date'], format='%d.%m.%Y') >= pd.Timestamp.now() - pd.Timedelta(days=7)]

        chain_data = {}
        for milestone in chain_milestones:
            origin = milestone['origin']
            date = milestone['date']
            metric = milestone['metric']
            if origin not in chain_data:
                chain_data[origin] = {}
            if date not in chain_data[origin]:
                chain_data[origin][date] = {}
            if metric not in chain_data[origin][date]:
                chain_data[origin][date][metric] = []
            chain_data[origin][date][metric].append(milestone)

        cross_chain_data = {}
        for milestone in cross_chain_milestones:
            origin = milestone['origin']
            date = milestone['date']
            metric = milestone['metric']
            if origin not in cross_chain_data:
                cross_chain_data[origin] = {}
            if date not in cross_chain_data[origin]:
                cross_chain_data[origin][date] = {}
            if metric not in cross_chain_data[origin][date]:
                cross_chain_data[origin][date][metric] = []
            cross_chain_data[origin][date][metric].append(milestone)

        # Organize data for JSON agent
        combined_data = {
            "single_chain_milestones": chain_data,
            "cross_chain_milestones": cross_chain_data
        }

        # Analyze Layer 2 milestones using a JSON agent
        response = analytics.analyze_layer2_milestones(combined_data)
        
        # Check if the response is a string and handle accordingly
        if isinstance(response, str):
            response = analytics.clean_response(response)
            print(response)
        elif isinstance(response, dict):
            # Handle response as dictionary if applicable
            output = response.get('output', 'No output available')
            print(output)


        # Send the response as a Discord embed message
        analytics.send_discord_embed_message(
            webhook_url=webhook_url,
            title="Layer 2 Blockchain Milestone Update",
            description=output,
            color=0x7289da,
            footer=f"Analysis as of {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data compiled from latest metrics",
            timestamp=True,
            author="GTP-AI"
        )


    run_ai()
gtp_ai()
    