import os
import json
import requests
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta

class GTPAnalyst:
    def __init__(self):
        self.CHAIN_WEIGHT = 0.2
        self.SIGNIFICANCE_MULTIPLIER = 1.5
        self.MIN_TOTAL_IMPORTANCE = 5
        
        # Load milestones configuration from JSON file
        config_path = os.path.join(os.path.dirname(__file__), 'milestones_config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
            self.metric_milestones = config['metric_milestones']
            self.cross_chain_milestones = config['cross_chain_milestones']

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
        # Ensure 'date' column is datetime.date and sort the data
        data['date'] = pd.to_datetime(data['date']).dt.date
        data = data.sort_values(by=['metric', 'date'])

        results = []

        # Process each metric separately
        for metric in data['metric'].unique():
            metric_data = data[data['metric'] == metric].copy()

            # Calculate percentage changes and highs
            self._calculate_percentage_changes(metric_data)
            self._calculate_highs(metric_data)

            # Get milestones for the current metric
            milestones = metric_milestones.get(metric, [])

            # Evaluate milestones for each row
            metric_results = self._evaluate_milestones(metric_data, milestones)
            results.extend(metric_results)

        # Sort results by date (latest first) and total importance (highest first)
        results.sort(key=lambda x: (-pd.to_datetime(x['date'], format='%d.%m.%Y').timestamp(), -x['total_importance']))

        return results

    def _calculate_percentage_changes(self, df):
        periods = {'1d_pct_change': 1, '7d_pct_change': 7, '30d_pct_change': 30, '365d_pct_change': 365}
        for col_name, period in periods.items():
            df[col_name] = df.groupby('origin')['value'].pct_change(periods=period) * 100

    def _calculate_highs(self, df):
        df['ath'] = df.groupby('origin')['value'].cummax()
        df['1y_high'] = df.groupby('origin')['value'].rolling(window=365, min_periods=1).max().reset_index(level=0, drop=True)

    def _evaluate_milestones(self, df, milestones):
        results = []

        for index, row in df.iterrows():
            formatted_date = row['date'].strftime('%d.%m.%Y')
            origin = row['origin']
            metric = row['metric']
            rank = row['rank']
            value = row['value']

            # Check for ATH
            if value == row['ath']:
                # Create milestone result for ATH
                ath_result = self._create_milestone_result(
                    row,
                    milestone_name="Chain ATH",
                    importance_score=9,
                    date=formatted_date
                )
                results.append(ath_result)

                # Check for combined milestones with multiples
                for milestone in milestones:
                    if milestone['type'] == 'Multiples' and value >= milestone['threshold']:
                        combined_importance = round(
                            (milestone['importance_score'] + (rank * self.CHAIN_WEIGHT)) * self.SIGNIFICANCE_MULTIPLIER, 2
                        )
                        combined_result = self._create_milestone_result(
                            row,
                            milestone_name=f"Chain ATH and {milestone['milestone']}",
                            importance_score=milestone['importance_score'],
                            date=formatted_date,
                            total_importance=combined_importance
                        )
                        results.append(combined_result)

            # Check for 1-year high
            if value == row['1y_high']:
                # Create milestone result for 1-Year High
                y_high_result = self._create_milestone_result(
                    row,
                    milestone_name="1-Year High",
                    importance_score=8,
                    date=formatted_date
                )
                results.append(y_high_result)

                # Check for combined milestones with multiples
                for milestone in milestones:
                    if milestone['type'] == 'Multiples' and value >= milestone['threshold']:
                        combined_importance = round(
                            (milestone['importance_score'] + (rank * self.CHAIN_WEIGHT)) * self.SIGNIFICANCE_MULTIPLIER, 2
                        )
                        combined_result = self._create_milestone_result(
                            row,
                            milestone_name=f"1-Year High and {milestone['milestone']}",
                            importance_score=milestone['importance_score'],
                            date=formatted_date,
                            total_importance=combined_importance
                        )
                        results.append(combined_result)

            # Check percentage increases
            pct_fields = {
                '1d_pct_change': '24h Up',
                '7d_pct_change': '7 days Up',
                '30d_pct_change': '30 days Up',
                '365d_pct_change': '1 year Up'
            }
            for pct_col, label in pct_fields.items():
                pct_change = row.get(pct_col)
                if pd.notnull(pct_change):
                    for milestone in milestones:
                        if milestone['type'] == 'Up %' and pct_change >= milestone['threshold']:
                            total_importance = round(milestone['importance_score'] + (rank * self.CHAIN_WEIGHT), 2)
                            pct_result = self._create_milestone_result(
                                row,
                                milestone_name=f"{label} {milestone['threshold']}%+",
                                importance_score=milestone['importance_score'],
                                date=formatted_date,
                                exact_value=f"{pct_change:,.2f}%",
                                total_importance=total_importance
                            )
                            results.append(pct_result)

        return results

    def _create_milestone_result(self, row, milestone_name, importance_score, date, exact_value=None, total_importance=None):
        rank = row['rank']
        if total_importance is None:
            total_importance = round(importance_score + (rank * self.CHAIN_WEIGHT), 2)
        if exact_value is None:
            exact_value = f"{row['value']:,.2f}"

        result = {
            "origin": row['origin'],
            "rank": rank,
            "date": date,
            "metric": row['metric'],
            "milestone": milestone_name,
            "importance_score": importance_score,
            "exact_value": exact_value,
            "total_importance": total_importance
        }
        return result

    def analyze_cross_chain_milestones(self, data, milestones):
        cross_chain_results = []
        CHAIN_WEIGHT = 0.1

        data['date'] = pd.to_datetime(data['date']).dt.date
        data = data.sort_values(by=['metric', 'date'])

        if not milestones:
            return cross_chain_results

        for metric in data['metric'].unique():
            metric_data = data[data['metric'] == metric]
            global_ath = None

            for date in metric_data['date'].unique():
                daily_data = metric_data[metric_data['date'] == date]
                
                max_value_row = daily_data.loc[daily_data['value'].idxmax()]

                # If there's no global ATH yet or the current max value exceeds the global ATH
                if global_ath is None or max_value_row['value'] > float(global_ath):
                    global_ath = max_value_row['value']
                    
                    total_importance = round(milestones[0]['importance_score'] + (max_value_row['rank'] * CHAIN_WEIGHT), 2)
                    
                    formatted_global_ath = f"{global_ath:,.2f}"
                    
                    cross_chain_results.append({
                        "origin": max_value_row['origin'],
                        "rank": max_value_row['rank'],
                        "date": date.strftime('%d.%m.%Y'),
                        "metric": max_value_row['metric'],
                        "milestone": milestones[0]['milestone'].format(metric=max_value_row['metric']),
                        "importance_score": milestones[0]['importance_score'],
                        "exact_value": formatted_global_ath,
                        "total_importance": total_importance
                    })
                    
                    # Check if the ATH also meets any multiple thresholds
                    for milestone in milestones:
                        if 'type' in milestone and milestone['type'] == 'Multiples' and max_value_row['value'] >= milestone['threshold']:
                            combined_importance = round((milestone['importance_score'] + (max_value_row['rank'] * CHAIN_WEIGHT)) * self.SIGNIFICANCE_MULTIPLIER, 2)
                            cross_chain_results.append({
                                "origin": max_value_row['origin'],
                                "rank": max_value_row['rank'],
                                "date": date.strftime('%d.%m.%Y'),
                                "metric": max_value_row['metric'],
                                "milestone": f"ATH and {milestone['milestone']}",
                                "importance_score": milestone['importance_score'],
                                "exact_value": formatted_global_ath,
                                "total_importance": combined_importance
                            })

        cross_chain_results.sort(key=lambda x: (-pd.to_datetime(x['date'], format='%d.%m.%Y').timestamp(), -x['total_importance']))

        return cross_chain_results

    def get_latest_milestones(self, chain_milestones, n=1, day_interval=1):
        for milestone in chain_milestones:
            milestone['date'] = pd.to_datetime(milestone['date'], format='%d.%m.%Y')
        
        # Keep only the milestones within the specified day interval
        recent_milestones = [
            milestone for milestone in chain_milestones
            if (pd.Timestamp.now() - milestone['date']).days <= day_interval
        ]
        
        if not recent_milestones:
            print(f"No milestones found within the last {day_interval} day(s).")
            return []
        
        # Group by origin and metric, and keep the one with the highest importance score
        grouped_milestones = {}
        for milestone in recent_milestones:
            if milestone['total_importance'] < self.MIN_TOTAL_IMPORTANCE:
                continue
            
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

        if not responses:
            # Send an embed indicating no milestones were found
            embed_messages.append({
                "title": "No Latest Milestones",
                "description": "No milestones detected in the specified time range.",
                "color": 0xFF0000,  # Red color for no milestones
                "footer": {"text": footer}
            })
        else:
            total_responses = len(responses)
            for idx, (key, value) in enumerate(responses.items()):
                description = value.get('output', '')
                if not description:
                    continue  # Skip if there is no output

                embed = {
                    "description": description,
                    "color": color,
                }

                if idx == 0:
                    embed["author"] = {"name": author}
                    embed["title"] = title

                # Add footer to the last embed
                if idx == total_responses - 1:
                    embed["footer"] = {"text": footer}

                embed_messages.append(embed)

        # Send all embeds in a single Discord message
        try:
            self.send_discord_embed_message(webhook_url, embed_messages)
            print("Discord embeds sent successfully.")
        except Exception as e:
            print(f"Failed to send Discord embeds: {e}")

    def generate_milestone_responses(self, combined_data):
        # Define templates
        templates = {
            "pct_increase": (
                "\n\n**{origin}** - **{metric}**: **{milestone}**"
                "\n> with an increase of {exact_value}."
                "\n> **Total Importance:** {total_importance}  (Milestone: {importance_score}/10, Chain Rank: {rank})."
            ),
            "absolute_value": (
                "\n\n**{origin}** - **{metric}**: **{milestone}**"
                "\n> with a current value of {exact_value}."
                "\n> **Total Importance:** {total_importance}  (Milestone: {importance_score}/10, Chain Rank: {rank})."
            ),
            "fire_pct_increase": (
                "\n\n🔥 **{origin}** - **{metric}**: **{milestone}**"
                "\n> with an increase of {exact_value}."
                "\n> **Total Importance:** {total_importance}  (Milestone: {importance_score}/10, Chain Rank: {rank})."
            ),
            "fire_absolute_value": (
                "\n\n🔥 **{origin}** - **{metric}**: **{milestone}**"
                "\n> with a current value of {exact_value}."
                "\n> **Total Importance:** {total_importance}  (Milestone: {importance_score}/10, Chain Rank: {rank})."
            ),
        }

        responses = {}

        # Process single-chain milestones
        single_chain_milestones = combined_data.get("single_chain_milestones", {})
        if single_chain_milestones:
            for chain, chain_data in single_chain_milestones.items():
                response = self.process_milestones(chain_data, templates)
                responses[f"single_chain_{chain}"] = {"output": response}

        # Process cross-chain milestones
        cross_chain_milestones = combined_data.get("cross_chain_milestones", {})
        if cross_chain_milestones:
            for chain, chain_data in cross_chain_milestones.items():
                response = self.process_milestones(chain_data, templates)
                responses[f"cross_chain_{chain}"] = {"output": response}

        return responses

    def process_milestones(self, chain_data, templates):
        response = ""
        for date, metrics in chain_data.items():
            for metric_name, milestones_list in metrics.items():
                for milestone in milestones_list:
                    # Determine the template to use
                    is_percentage_increase = "%" in milestone.get("exact_value", "")
                    high_importance = milestone["total_importance"] >= 10

                    if is_percentage_increase and high_importance:
                        template_key = "fire_pct_increase"
                    elif is_percentage_increase:
                        template_key = "pct_increase"
                    elif high_importance:
                        template_key = "fire_absolute_value"
                    else:
                        template_key = "absolute_value"

                    template = templates[template_key]

                    # Format the template with milestone data
                    formatted_response = template.format(
                        origin=milestone["origin"].upper(),
                        importance_score=milestone["importance_score"],
                        rank=milestone["rank"],
                        date=milestone["date"],
                        metric=milestone["metric"],
                        milestone=milestone["milestone"],
                        exact_value=milestone.get("exact_value", "N/A"),
                        total_importance=milestone["total_importance"]
                    )

                    response += formatted_response
        return response

def convert_timestamps(data):
    if isinstance(data, dict):
        return {key: convert_timestamps(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_timestamps(element) for element in data]
    elif isinstance(data, pd.Timestamp):
        return data.strftime('%d.%m.%Y')
    return data