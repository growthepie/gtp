import os
from src.misc.helper_functions import send_discord_message
from airflow.models import Variable

def alert_via_webhook(context, user='mseidl'):
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    exception = context.get('exception')
    webhook_url = Variable.get("DISCORD_ALERTS")

    if user == 'mseidl':
        user_id = '693484083895992393'
    elif user == 'lorenz':
        user_id = '790276642660548619'
    elif user == 'nader':
        user_id = '326358477335298050'
    elif user == 'mike':
        user_id = '253618927572221962'

    message = f"<@{user_id}> -- A failure occurred in {dag_run.dag_id} on task {task_instance.task_id}. Might just be a transient issue -- Exception: {exception}"
    send_discord_message(message[:499], webhook_url)