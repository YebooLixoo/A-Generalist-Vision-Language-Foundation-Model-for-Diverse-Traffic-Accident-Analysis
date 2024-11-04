from apify_client import ApifyClient
import csv
import pandas as pd
from datetime import datetime, timedelta

# Initialize the ApifyClient with API token
client = ApifyClient("apify_api_MdGvE68vHMl96m49cNohmJbgsgigAe1QkN2h")

# Read the CSV file using pandas
data_2022 = pd.read_csv("/Users/boyu/Desktop/LLMs for Traffic Accident/Coding/Data Sraping/2022_california.csv")

# Extract the required columns from the dataframe
Initial_date = data_2022.apply(lambda row: f"{row['YEAR']}-{row['MONTH']}-{row['DAY']}", axis=1).tolist()
End_date = [(datetime.strptime(date, "%Y-%m-%d") + timedelta(days=3)).strftime("%Y-%m-%d") for date in Initial_date]
Tway_id = data_2022['TWAY_ID'].tolist()
Tway_id2 = data_2022['TWAY_ID2'].tolist()
State = data_2022['STATE'].tolist()
County_name = data_2022['COUNTYNAME'].tolist()
State_case = data_2022['ST_CASE'].tolist()

# Prepare the Actor input

run_input = {
    "customMapFunction": "(object) => { return {...object} }",
    "end": End_date,
    "maxItems": 1000,
    "minimumFavorites": 0,
    "minimumReplies": 0,
    "minimumRetweets": 0,
    "placeObjectId": "96683cc9126741d1",
    "searchTerms": [
        f"{tway_id} {tway_id2 if tway_id2 else ''} {state if state else ''} {county_name if county_name else ''}".strip() for tway_id, tway_id2, state, county_name in zip(Tway_id, Tway_id2, State, County_name)
    ],
    "sort": "Latest",
    "start": Initial_date,
    "tweetLanguage": "en"
}

# Run the Actor and wait for it to finish
run = client.actor("61RPP7dywgiy0JPD0").call(run_input=run_input) 

# Define the CSV file path 
csv_file_path = "/Users/boyu/Desktop/LLMs for Traffic Accident/Coding/Data Sraping/twitter_data.csv"
# Fetch Actor results and save them to a CSV file
with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
    fieldnames = ['State_case', 'id', 'text', 'createdAt', 'retweetCount', 'favoriteCount', 'replyCount', 'user', 'url', 'image']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        writer.writerow({
            "State_case": State_case.pop(0) if State_case else '',
            "id": item.get("id"),
            "text": item.get("text"),
            "createdAt": item.get("createdAt"),
            "retweetCount": item.get("retweetCount"),
            "favoriteCount": item.get("favoriteCount"),
            "replyCount": item.get("replyCount"),
            "user": item.get("user", {}).get("username"),
            "url": item.get("url"),
            "image": 1 if item.get("media") else 0
        })