import os
from src.misc.helper_functions import send_discord_message

def alert_via_webhook(context):
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    exception = context.get('exception')
    webhook_url = os.getenv("DISCORD_ALERTS")

    message = f"Alert: A failure occurred in {dag_run.dag_id} on task {task_instance.task_id}. Exception: {exception}"
    send_discord_message(message, webhook_url)