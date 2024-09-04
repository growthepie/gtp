import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime, timedelta

class GTPAI:
    def __init__(self):
        load_dotenv()
        self.OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
        self.CHAIN_WEIGHT = 0.5
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
                "gas_per_second": [
                {"type": "ATH", "milestone": "Metric ATH", "importance_score": 10},
                {"type": "ATH", "milestone": "Chain ATH", "importance_score": 9},
                {"type": "Multiples", "milestone": "Multiple of 1Mgas", "importance_score": 6, "threshold": 1_000_000},
                {"type": "Multiples", "milestone": "Multiple of 0.5Mgas", "importance_score": 4, "threshold": 500_000},
                {"type": "Multiples", "milestone": "Multiple of 0.1Mgas", "importance_score": 1, "threshold": 100_000},
                {"type": "Up %", "milestone": "24h Up 10%+", "importance_score": 2, "threshold": 10},
                {"type": "Up %", "milestone": "24h Up 25%+", "importance_score": 1, "threshold": 25},
                {"type": "Up %", "milestone": "7 days Up 50%+", "importance_score": 2, "threshold": 50},
                {"type": "Up %", "milestone": "7 days Up 100%+", "importance_score": 1, "threshold": 100},
                {"type": "Up %", "milestone": "30 days Up 50%+", "importance_score": 2, "threshold": 50},
                {"type": "Up %", "milestone": "30 days Up 100%+", "importance_score": 1, "threshold": 100},
                {"type": "Up %", "milestone": "1 year Up 100%+", "importance_score": 2, "threshold": 100},
                {"type": "Up %", "milestone": "1 year Up 500%+", "importance_score": 1, "threshold": 500},
            ],
        }


        self.cross_chain_milestones = [
            {"type": "ATH", "milestone": "Metric ATH", "importance_score": 10},
        ]

    def fetch_json_data(self, url, local_filename):

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

    def rank_origins_by_tvl(self, df):
        tvl_df = df[df['metric'] == 'tvl']

        latest_tvl_df = tvl_df.loc[tvl_df.groupby('origin')['date'].idxmax()]

        latest_tvl_df = latest_tvl_df.sort_values(by='value', ascending=False).reset_index(drop=True)

        latest_tvl_df['rank'] = latest_tvl_df.index + 1  # Assign ranks from 1 to n

        latest_tvl_df = latest_tvl_df.sort_values(by=['rank', 'value'], ascending=[False, False]).reset_index(drop=True)
        latest_tvl_df['rank'] = range(1, len(latest_tvl_df) + 1)

        # Scale the ranks to use the full range from 1 to 10
        latest_tvl_df['rank'] = (latest_tvl_df['rank'] / latest_tvl_df['rank'].max()) * 9 + 1
        latest_tvl_df['rank'] = latest_tvl_df['rank'].round().astype(int)

        if 'ethereum' not in latest_tvl_df['origin'].values:
            ethereum_row = pd.DataFrame({
                'origin': ['ethereum'],
                'rank': [10]
            })
            latest_tvl_df = pd.concat([latest_tvl_df, ethereum_row], ignore_index=True)
        else:
            latest_tvl_df.loc[latest_tvl_df['origin'] == 'ethereum', 'rank'] = 10

        unique_ranks = latest_tvl_df['rank'].unique()
        if len(unique_ranks) < 10:
            missing_ranks = set(range(1, 11)) - set(unique_ranks)
            for rank in sorted(missing_ranks):
                closest_lower_rank = latest_tvl_df['rank'][latest_tvl_df['rank'] < rank].max()
                latest_tvl_df.loc[latest_tvl_df['rank'] == closest_lower_rank, 'rank'] = rank

        if 'rank' in df.columns:
            df.drop(columns='rank', inplace=True)

        df = df.merge(latest_tvl_df[['origin', 'rank']], on='origin', how='left')

        df = df[df['metric'] != 'tvl']

        return df
    
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
                formatted_date = row['date'].strftime('%d.%m.%Y')
                
                if row['value'] == row['ath']:
                    total_importance = 9 + (row['rank'] * self.CHAIN_WEIGHT)
                    results.append({
                        "origin": row['origin'],
                        "rank": row['rank'],
                        "date": formatted_date, 
                        "metric": row['metric'], 
                        "milestone": "Chain ATH", 
                        "importance_score": 9, 
                        "exact_value": f"{row['ath']:,.2f}",
                        "total_importance": total_importance
                    })
                
                for milestone in milestones:
                    if milestone['type'] == 'Multiples' and row['value'] >= milestone['threshold']:
                        total_importance = milestone['importance_score'] + (row['rank'] * self.CHAIN_WEIGHT)
                        results.append({
                            "origin": row['origin'],
                            "rank": row['rank'],
                            "date": formatted_date, 
                            "metric": row['metric'], 
                            "milestone": milestone['milestone'], 
                            "importance_score": milestone['importance_score'],
                            "exact_value": f"{row['value']:,.2f}",
                            "total_importance": total_importance
                        })
                    
                pct_fields = {'1d_pct_change': '24h Up', '7d_pct_change': '7 days Up', '30d_pct_change': '30 days Up', '365d_pct_change': '1 year Up'}
                for key, label in pct_fields.items():
                    for milestone in milestones:
                        if milestone['type'] == 'Up %' and row[key] >= milestone['threshold']:
                            total_importance = milestone['importance_score'] + (row['rank'] * self.CHAIN_WEIGHT)
                            results.append({
                                "origin": row['origin'],
                                "rank": row['rank'],
                                "date": formatted_date, 
                                "metric": row['metric'], 
                                "milestone": f"{label} {milestone['threshold']}%+", 
                                "importance_score": milestone['importance_score'], 
                                "exact_value": f"{row[key]:,.2f}%",
                                "total_importance": total_importance
                            })
        
        # Sort results by date (latest first), then by chain rank, and within each date by importance score (highest first)
            results.sort(key=lambda x: (pd.to_datetime(x['date'], format='%d.%m.%Y'), -x['total_importance']))
        
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

    def get_latest_milestones(self, chain_milestones, n=1, day_interval=1):
        for milestone in chain_milestones:
            milestone['date'] = pd.to_datetime(milestone['date'], format='%d.%m.%Y')
        
        # Keep only the milestones within the specified day interval
        recent_milestones = [
            milestone for milestone in chain_milestones
            if (pd.Timestamp.now() - milestone['date']).days <= day_interval
        ]
        
        # Group by origin and metric, and keep the one with the highest importance score
        grouped_milestones = {}
        for milestone in recent_milestones:
            key = (milestone['origin'], milestone['metric'])
            if key not in grouped_milestones or milestone['importance_score'] > grouped_milestones[key]['importance_score']:
                grouped_milestones[key] = milestone
        
        grouped_milestones_list = list(grouped_milestones.values())
        
        grouped_milestones_list.sort(key=lambda x: (-x['date'].timestamp(), x['rank'], -x['importance_score']))

        # Pick n latest milestones per origin
        final_milestones = []
        origins_seen = {}
        
        for milestone in grouped_milestones_list:
            origin = milestone['origin']
            if origin not in origins_seen:
                origins_seen[origin] = 0
            if origins_seen[origin] < n:
                milestone['date'] = milestone['date'].strftime('%d.%m.%Y')
                final_milestones.append(milestone)
                origins_seen[origin] += 1
        
        return final_milestones
    
    def send_discord_embed_message(self, webhook_url, embeds):
        data = {
            "embeds": embeds
        }
        
        response = requests.post(webhook_url, json=data)
        if response.status_code == 204:
            print("Embedded message sent successfully!")
        else:
            error_embed = {
                "title": "Error: Failed to Send Embedded Message",
                "description": f"Status code: {response.status_code}\nResponse: {response.text}",
                "color": 0xFF0000,  # Red color for error
            }
            
            error_data = {
                "embeds": [error_embed]
            }
            
            error_response = requests.post(webhook_url, json=error_data)
            if error_response.status_code == 204:
                print("Error embed sent successfully!")
            else:
                print(f"Failed to send error embed. Status code: {error_response.status_code}, Response: {error_response.text}")

    def craft_and_send_discord_embeds(self, webhook_url, responses, title, footer, color=0x7289da, author="GTP-AI"):
        embed_messages = []

        for idx, (key, value) in enumerate(responses.items()):
            if 'output' in value:
                description = value['output']

                embed = {
                    "description": description,
                    "color": color,
                }

                if idx == 0:
                    embed["author"] = {"name": author}
                    embed["title"] = title
                
                if idx == len(responses) - 1:
                    embed["footer"] = {"text": footer}

                embed_messages.append(embed)

        # Send all embeds in a single Discord message
        self.send_discord_embed_message(webhook_url, embed_messages)
        
    def generate_milestone_responses(self, combined_data):
        template = (
            "\n\n🔥 **{origin} Milestone (Importance: {importance_score}/10, Rank: {rank}):**"
            "\n> 📅 On {date}, **{origin}** reached a milestone in **{metric}**:\n> 🚀 **{milestone}** "
            "with an increase of **{exact_value}**.\n> 💡 **Total Importance:** {total_importance}"
        )

        responses = {}

        # Process single-chain milestones
        single_chain_milestones = combined_data.get("single_chain_milestones", {})
        if single_chain_milestones:
            for chain, chain_data in single_chain_milestones.items():
                response = ""
                for date, metrics in chain_data.items():
                    for metric_name, milestones in metrics.items():
                        for milestone in milestones:
                            response += template.format(
                                origin=milestone["origin"].upper(),
                                importance_score=milestone["importance_score"],
                                rank=milestone["rank"],
                                date=milestone["date"],
                                metric=milestone["metric"],
                                milestone=milestone["milestone"],
                                exact_value=milestone.get("exact_value", "N/A"),
                                total_importance=milestone["total_importance"]
                            )
                responses[f"single_chain_{chain}"] = {"output": response}

        # Process cross-chain milestones
        cross_chain_milestones = combined_data.get("cross_chain_milestones", {})
        if cross_chain_milestones:
            for chain, chain_data in cross_chain_milestones.items():
                response = ""
                for date, metrics in chain_data.items():
                    for metric_name, milestones in metrics.items():
                        for milestone in milestones:
                            response += template.format(
                                origin=milestone["origin"].upper(),
                                importance_score=milestone["importance_score"],
                                rank=milestone["rank"],
                                date=milestone["date"],
                                metric=milestone["metric"],
                                milestone=milestone["milestone"],
                                exact_value=milestone.get("exact_value", "N/A"),
                                total_importance=milestone["total_importance"]
                            )
                responses[f"cross_chain_{chain}"] = {"output": response}

        return responses
    
def convert_timestamps(data):
    if isinstance(data, dict):
        return {key: convert_timestamps(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_timestamps(element) for element in data]
    elif isinstance(data, pd.Timestamp):
        return data.strftime('%d.%m.%Y')
    return data