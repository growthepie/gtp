import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.agents import create_json_agent
from langchain.agents.agent_toolkits import JsonToolkit
from langchain_community.tools.json.tool import JsonSpec
from collections import defaultdict
from datetime import datetime, timedelta

class GTPAI:
    def __init__(self):
        load_dotenv()
        self.OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
        if not self.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY environment variable is not set.")
        
        self.metric_milestones = {
            "daa": [
                {"type": "ATH", "milestone": "Chain ATH", "importance_score": 9},
                {"type": "Multiples", "milestone": "Multiple of 1M", "importance_score": 6, "threshold": 1_000_000},
                {"type": "Multiples", "milestone": "Multiple of 500k", "importance_score": 4, "threshold": 500_000},
                {"type": "Multiples", "milestone": "Multiple of 100k", "importance_score": 1, "threshold": 100_000},
                {"type": "Up %", "milestone": "24h Up 10%+", "importance_score": 2, "threshold": 10},
                {"type": "Up %", "milestone": "24h Up 50%+", "importance_score": 1, "threshold": 50},
                {"type": "Up %", "milestone": "7 days Up 30%+", "importance_score": 2, "threshold": 30},
                {"type": "Up %", "milestone": "7 days Up 50%+", "importance_score": 1, "threshold": 50},
                {"type": "Up %", "milestone": "30 days Up 50%+", "importance_score": 2, "threshold": 50},
                {"type": "Up %", "milestone": "30 days Up 100%+", "importance_score": 1, "threshold": 100},
                {"type": "Up %", "milestone": "1 year Up 500%+", "importance_score": 2, "threshold": 500},
                {"type": "Up %", "milestone": "1 year Up 1000%+", "importance_score": 1, "threshold": 1000},
            ],
            "txcount": [
                {"type": "ATH", "milestone": "Chain ATH", "importance_score": 9},
                {"type": "Multiples", "milestone": "Multiple of 1M", "importance_score": 6, "threshold": 1_000_000},
                {"type": "Multiples", "milestone": "Multiple of 500k", "importance_score": 4, "threshold": 500_000},
                {"type": "Multiples", "milestone": "Multiple of 100k", "importance_score": 1, "threshold": 100_000},
                {"type": "Up %", "milestone": "24h Up 10%+", "importance_score": 2, "threshold": 10},
                {"type": "Up %", "milestone": "24h Up 50%+", "importance_score": 1, "threshold": 50},
                {"type": "Up %", "milestone": "7 days Up 30%+", "importance_score": 2, "threshold": 30},
                {"type": "Up %", "milestone": "7 days Up 50%+", "importance_score": 1, "threshold": 50},
                {"type": "Up %", "milestone": "30 days Up 50%+", "importance_score": 2, "threshold": 50},
                {"type": "Up %", "milestone": "30 days Up 100%+", "importance_score": 1, "threshold": 100},
                {"type": "Up %", "milestone": "1 year Up 500%+", "importance_score": 2, "threshold": 500},
                {"type": "Up %", "milestone": "1 year Up 1000%+", "importance_score": 1, "threshold": 1000},
            ],
            "market_cap_usd": [
                {"type": "ATH", "milestone": "Chain ATH", "importance_score": 9},
                {"type": "Multiples", "milestone": "Multiple of 1B", "importance_score": 8, "threshold": 1_000_000_000},
                {"type": "Multiples", "milestone": "Multiple of 1M", "importance_score": 6, "threshold": 1_000_000},
                {"type": "Multiples", "milestone": "Multiple of 500k", "importance_score": 4, "threshold": 500_000},
                {"type": "Multiples", "milestone": "Multiple of 100k", "importance_score": 1, "threshold": 100_000},
                {"type": "Up %", "milestone": "24h Up 5%+", "importance_score": 2, "threshold": 5},
                {"type": "Up %", "milestone": "24h Up 10%+", "importance_score": 2, "threshold": 10},
                {"type": "Up %", "milestone": "24h Up 50%+", "importance_score": 1, "threshold": 50},
                {"type": "Up %", "milestone": "7 days Up 30%+", "importance_score": 2, "threshold": 30},
                {"type": "Up %", "milestone": "7 days Up 50%+", "importance_score": 1, "threshold": 50},
                {"type": "Up %", "milestone": "30 days Up 50%+", "importance_score": 2, "threshold": 50},
                {"type": "Up %", "milestone": "30 days Up 100%+", "importance_score": 1, "threshold": 100},
                {"type": "Up %", "milestone": "1 year Up 500%+", "importance_score": 2, "threshold": 500},
                {"type": "Up %", "milestone": "1 year Up 1000%+", "importance_score": 1, "threshold": 1000},
            ],
        }

        self.cross_chain_milestones = [
            {"type": "ATH", "milestone": "Metric ATH", "importance_score": 10},
        ]

    def fetch_json_data(self, url, local_filename):
        #if os.path.exists(local_filename):
        #    last_modified_time = datetime.fromtimestamp(os.path.getmtime(local_filename))
        #    current_time = datetime.now()
            
        #    if current_time - last_modified_time < timedelta(hours=24):
        #        print(f"{local_filename} was downloaded less than 24 hours ago. No need to download again.")
        #        return

        response = requests.get(url)
        data = response.json()

        with open(local_filename, "w") as file:
            json.dump(data, file, indent=4)

        print(f"{local_filename} downloaded successfully")

    def filter_data(self, file_path, chain=None, metrics=None, start_date=None):
        with open(file_path, "r") as file:
            data = json.load(file)
        
        if start_date:
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            end_datetime = start_datetime + timedelta(days=7)
        else:
            start_datetime = None
            end_datetime = None
        
        filtered_data = [
            item for item in data if (
                (chain is None or item['origin_key'] == chain) and
                (metrics is None or item['metric_key'] in metrics) and
                '_eth' not in item['metric_key'] and
                (start_datetime is None or (
                    datetime.strptime(item['date'], "%Y-%m-%d") >= start_datetime and
                    datetime.strptime(item['date'], "%Y-%m-%d") < end_datetime
                ))
            )
        ]
        
        return filtered_data

    def organize_data(self, filtered_data):
        combined_dict = defaultdict(lambda: defaultdict(dict))
        for item in filtered_data:
            origin = item['origin_key']
            metric = item['metric_key']
            date = item['date']
            combined_dict[origin][metric][date] = item['value']
        return combined_dict

    def json_to_dataframe(self, data):
        records = []
        for origin, metrics in data.items():
            for metric, values in metrics.items():
                for date, value in values.items():
                    records.append({'origin': origin, 'metric': metric, 'date': date, 'value': value})
        return pd.DataFrame(records)

    def detect_chain_milestones(self, data, metric_milestones):
        data['date'] = pd.to_datetime(data['date']).dt.date
        data = data.sort_values(by=['metric', 'date'])
        
        results = []
        
        for metric in data['metric'].unique():
            metric_data = data[data['metric'] == metric].copy()
            
            metric_data.loc[:, '1d_pct_change'] = metric_data.groupby('origin')['value'].pct_change(periods=1) * 100
            metric_data.loc[:, '7d_pct_change'] = metric_data.groupby('origin')['value'].pct_change(periods=7) * 100
            metric_data.loc[:, '30d_pct_change'] = metric_data.groupby('origin')['value'].pct_change(periods=30) * 100
            metric_data.loc[:, '365d_pct_change'] = metric_data.groupby('origin')['value'].pct_change(periods=365) * 100
            
            metric_data.loc[:, 'ath'] = metric_data.groupby('origin')['value'].expanding().max().reset_index(level=0, drop=True)
            
            milestones = metric_milestones.get(metric, [])
            
            for index, row in metric_data.iterrows():
                origin = row['origin']
                formatted_date = row['date'].strftime('%d.%m.%Y')
                
                if row['value'] == row['ath']:
                    results.append({
                        "origin": origin,
                        "date": formatted_date, 
                        "metric": row['metric'], 
                        "milestone": "Chain ATH", 
                        "importance_score": 9, 
                        "new_ath": row['ath']
                    })
                
                for milestone in milestones:
                    if milestone['type'] == 'Multiples' and 'threshold' in milestone and row['value'] >= milestone['threshold'] and row['value'] % milestone['threshold'] == 0:
                        results.append({
                            "origin": origin,
                            "date": formatted_date, 
                            "metric": row['metric'], 
                            "milestone": milestone['milestone'], 
                            "importance_score": milestone['importance_score'], 
                            "exact_increase": None
                        })
                
                pct_fields = {'1d_pct_change': '24h Up', '7d_pct_change': '7 days Up', '30d_pct_change': '30 days Up', '365d_pct_change': '1 year Up'}
                for key, label in pct_fields.items():
                    for milestone in milestones:
                        if milestone['type'] == 'Up %' and row[key] >= milestone['threshold']:
                            results.append({
                                "origin": origin,
                                "date": formatted_date, 
                                "metric": row['metric'], 
                                "milestone": f"{label} {milestone['threshold']}%+", 
                                "importance_score": milestone['importance_score'], 
                                "exact_increase": f"{row[key]:.2f}%"
                            })
        
        results.sort(key=lambda x: (-pd.to_datetime(x['date'], format='%d.%m.%Y').timestamp(), -x['importance_score']))
        
        return results

    def analyze_cross_chain_milestones(self, data, milestones):
        cross_chain_results = []

        data['date'] = pd.to_datetime(data['date']).dt.date
        data = data.sort_values(by=['metric', 'date'])

        for metric in data['metric'].unique():
            metric_data = data[data['metric'] == metric]
            global_ath = None

            for date in metric_data['date'].unique():
                daily_data = metric_data[metric_data['date'] == date]
                max_value_row = daily_data.loc[daily_data['value'].idxmax()]

                if global_ath is None or max_value_row['value'] > global_ath:
                    global_ath = max_value_row['value']
                    cross_chain_results.append({
                        "origin": max_value_row['origin'],
                        "date": date.strftime('%d.%m.%Y'),
                        "metric": max_value_row['metric'],
                        "milestone": milestones[0]['milestone'].format(metric=max_value_row['metric']),
                        "importance_score": milestones[0]['importance_score'],
                        "global_ath": global_ath
                    })

        cross_chain_results.sort(key=lambda x: (-pd.to_datetime(x['date'], format='%d.%m.%Y').timestamp(), -x['importance_score']))

        return cross_chain_results


    def send_discord_embed_message(self, webhook_url, title, description, color=0x00ff00, footer=None, timestamp=True, author=None):
        embed = {
            "title": title,
            "description": description,
            "color": color
        }
        
        if timestamp:
            embed["timestamp"] = datetime.utcnow().isoformat()
        
        if footer:
            embed["footer"] = {
                "text": footer
            }
        
        if author:
            embed["author"] = {
                "name": author
            }
        
        data = {
            "embeds": [embed]
        }
        
        response = requests.post(webhook_url, json=data)
        if response.status_code == 204:
            print("Embedded message sent successfully!")
        else:
            print(f"Failed to send embedded message. Status code: {response.status_code}, Response: {response.text}")
            
    def analyze_layer2_milestones(self, combined_data):
        spec = JsonSpec(dict_=dict(combined_data), max_value_length=4000)
        toolkit = JsonToolkit(spec=spec)
        try:
            agent = create_json_agent(
                llm=ChatOpenAI(temperature=0.7, model="gpt-4-turbo", openai_api_key=self.OPENAI_API_KEY),
                toolkit=toolkit,
                max_iterations=2000,
                verbose=True,
                handle_parsing_errors=True
            )
            
            query = (
                "You are a Layer 2 blockchain analyst. Analyze the latest significant milestones achieved across different metrics, "
                "including single-chain and cross-chain milestones for different metrics. "
                "Craft engaging and concise messages that reflect the importance of each milestone, tailored for Discord embeds. "
                "For single-chain milestones, highlight the chain's growth, metric-specific achievements, and broader implications. "
                "For cross-chain milestones, emphasize the broader impact and comparisons across chains. "
                "Include relevant market insights to provide a comprehensive analysis. Structure the response as follows:"
                "\n\n🔥 **Single-Chain Milestones:**"
                "\n> 📅 On {date}, **{origin}** achieved a new milestone for **{metric}**:\n> 🚀 **{milestone}** with a growth of **{exact_increase}**. This marks a significant achievement in {impact_statement}."
                "\n\n🌐 **Cross-Chain Milestones:**"
                "\n> 🌍 On {date}, **{origin}** set a new record for **{metric}** with **{global_ath}** across all chains. This represents a key moment in {contextual_impact}."
                "\n\n📊 **Market Insights:**"
                "\n> 💡 **{market_context}** These achievements suggest {prediction_or_insight}, leading to {future_outlook} in the Layer 2 ecosystem."
            )

            
            response = agent.invoke(query)
            return response

        except Exception as e:
            return f"An error occurred: {e}"

    def clean_response(self, response_str):
        start_marker = "🔥 **Single-Chain Milestones:**"
        
        start_index = response_str.find(start_marker)
        
        if start_index == -1:
            return "Error: Single-chain milestones section not found in response."

        relevant_content = response_str[start_index:]
        
        end_marker = "These crafted messages should effectively communicate"
        end_index = relevant_content.find(end_marker)
        
        if end_index != -1:
            relevant_content = relevant_content[:end_index].strip()

        return relevant_content
