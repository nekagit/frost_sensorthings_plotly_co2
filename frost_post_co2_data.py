import pandas as pd
import requests
import json

# Read CSV
df = pd.read_csv('CO2sensors_.csv', delimiter=';', parse_dates=['Server time'])

# Convert DataFrame to FROST-compatible JSON format
def convert_to_frost_format(row):
    return {
        "phenomenonTime": row['Server time'].isoformat(),
        "resultTime": row['Server time'].isoformat(),
        "result": {
            "CO2 concentration": row['CO2 concentration'],
            "Temperature": row['Temperature'],
            "Humidity": row['Humidity']
        }
    }

frost_data = [convert_to_frost_format(row) for _, row in df.iterrows()]

# FROST API endpoint (replace with your actual endpoint)
frost_url = "https://your-frost-api-endpoint/Observations"

# POST data to FROST API
headers = {
    'Content-Type': 'application/json'
}

for observation in frost_data:
    response = requests.post(frost_url, 
                             headers=headers, 
                             data=json.dumps(observation))
    
    if response.status_code == 201:
        print(f"Successfully uploaded observation: {observation}")
    else:
        print(f"Failed to upload: {response.text}")