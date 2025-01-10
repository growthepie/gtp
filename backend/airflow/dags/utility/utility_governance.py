from datetime import datetime,timedelta
import getpass
import os
sys_user = getpass.getuser()

import dotenv
dotenv.load_dotenv()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")
from airflow.decorators import dag, task 

from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(seconds=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='utility_governance',
    description='Sends notifications about new governance proposals',
    tags=['utility'],
    start_date=datetime(2025,1,1),
    schedule_interval='0 8 * * *'
)

def etl():
    @task()
    def check_for_new_proposals():
        from src.misc.helper_functions import send_discord_message, prompt_chatgpt
        from src.misc.tally import TallyAPI
        from src.misc.agora import AgoraAPI

        # checking Arbitrum and Zksync
        t = TallyAPI(os.getenv("Tally_API_KEY"))
        for governance in t.organisation_id:
            proposals = t.get_proposals(t.organisation_id[governance])

            for i, proposal in proposals.iterrows():
                if proposal['startTime'].split('T')[0] == datetime.now().isoformat().split('T')[0]:
                    url = f"https://www.tally.xyz/gov/{governance}/proposal/{proposal['onchainId']}"
                    message = f"""
                    📢 **New Proposal for {governance}** <@{898167530202464278}> <@{874921624720257037}>
                    
                    🔗 [Read the full proposal]({url})
                    
                    📝 **Description:**
                    {prompt_chatgpt("Please provide a short description for the proposal: " + proposal["description"], os.getenv('OPENAI_API_KEY'))}
                    """
                    message = message.replace("                    ", "")
                    send_discord_message(message, os.getenv('DISCORD_GOV'))
        
        # checking Optimism and Scroll
        a = AgoraAPI(os.getenv("Agora_API_KEY"))
        for governance in a.base_url:
            print(governance)
            proposals = a.get_proposals(a.base_url[governance])
            for i, proposal in proposals.iterrows():
                if proposal['startTime'].split('T')[0] == datetime.now().isoformat().split('T')[0]: # only post about proposals that are starting voting today
                    proposal_url = a.base_url[governance].replace("api/v1/", "") + "proposals/" + proposal['id']
                    message = f"""
                    📢 **New Proposal for {governance}** <@{898167530202464278}> <@{874921624720257037}>
                    
                    🔗 [Read the full proposal]({proposal_url})
                    
                    📝 **Description:**
                    {prompt_chatgpt("Please provide a short description for the proposal: " + proposal["description"], os.getenv('OPENAI_API_KEY'))}
                    """
                    message = message.replace("                    ", "")
                    print(message)
                    send_discord_message(message, os.getenv('DISCORD_GOV'))
        
    
    check_for_new_proposals()
etl()