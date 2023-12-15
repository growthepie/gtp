import requests

def send_discord_message(message, webhook_url):
    data = {"content": message}
    response = requests.post(webhook_url, json=data)

    # Check the response status code
    if response.status_code == 204:
        print("Message sent successfully")
    else:
        print("Error sending message")