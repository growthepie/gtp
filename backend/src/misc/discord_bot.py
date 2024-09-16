import asyncio
import nextcord
import json
import boto3
from datetime import datetime

def load_config():
    try:
        with open('config.json', 'r') as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        print("Configuration file not found. Please check the path.")
        exit(1)
    except json.JSONDecodeError:
        print("Error parsing the configuration file. Please check its format.")
        exit(1)

intents = nextcord.Intents.default()
intents.members = True
bot = nextcord.Client(intents=nextcord.Intents.all())

config = load_config()
bot_offline_alert_task = None
BOT_TOKEN = config['discord']['token']

# Load AWS Config
AWS_ACCESS_KEY = config['aws']['access_key_id']
AWS_SECRET_KEY = config['aws']['secret_access_key']
SECURITY_GROUP_ID = config['aws']['security_group_id']

# Initialize EC2 Resource with AWS Config
ec2 = boto3.resource(
    'ec2',
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

def split_messages(tasks, max_length=2000):
    # If the entire message is shorter than the max length, return it as is.
    full_message = "".join(tasks)
    if len(full_message) <= max_length:
        return [full_message]

    messages = []
    current_message = ""
    for task in tasks:
        if len(current_message) + len(task) > max_length:
            messages.append(current_message)
            current_message = task
        else:
            current_message += task
    messages.append(current_message)
    return messages

async def alert_offline():
    await asyncio.sleep(1)  # Simulate some delay or work
    print(f"The bot has gone offline at {datetime.now()}. Why?")

@bot.event
async def on_disconnect():
    global bot_offline_alert_task
    bot_offline_alert_task = asyncio.create_task(alert_offline())

@bot.event
async def on_ready():
    global bot_offline_alert_task
    if bot_offline_alert_task:
        bot_offline_alert_task.cancel()
    print(f'{bot.user.name} is Ready!')

@bot.slash_command()
async def ping(interaction: nextcord.Interaction):
    await interaction.response.send_message(f"Pong! {round(bot.latency * 1000)}ms")

@bot.slash_command(description="Whitelist an IP address")
async def whitelist(interaction: nextcord.Interaction, ip_address: str, port: int = 8080):
    await interaction.response.defer()

    try:
        security_group = ec2.SecurityGroup(SECURITY_GROUP_ID)
        ip_permission = {
            'IpProtocol': 'tcp',
            'FromPort': port,
            'ToPort': port,
            'IpRanges': [{'CidrIp': f'{ip_address}/32'}]
        }
        security_group.authorize_ingress(IpPermissions=[ip_permission])
        print(f"IP successfully whitelisted:{ip_address}")
        await interaction.followup.send(f"Your IP has been whitelisted on port {port}.")
    except Exception as e:
        await interaction.followup.send(f"Failed to whitelist: {str(e)}")

bot.run(BOT_TOKEN)