import requests
import pandas as pd
import json
from datetime import datetime

def read_csv_debug(csv_file):
    """Debug CSV reading process"""
    try:
        with open(csv_file, 'r') as file:
            print("Raw CSV content:")
            print(file.read())
    except Exception as e:
        print(f"Error reading file: {e}")

def create_datastream(base_url, thing_id, observed_property_ids):
    """Create datastreams for different measurements"""
    datastreams = {}
    
    datastream_configs = {
        'CO2 concentration': {
            'name': 'CO2 Concentration Datastream',
            'description': 'Measures CO2 concentration',
            'unit_of_measurement': 'ppm'
        },
        'Temperature': {
            'name': 'Temperature Datastream',
            'description': 'Measures ambient temperature',
            'unit_of_measurement': 'degree Celsius'
        },
        'Humidity': {
            'name': 'Humidity Datastream',
            'description': 'Measures relative humidity',
            'unit_of_measurement': 'percentage'
        }
    }
    
    for measurement, config in datastream_configs.items():
        datastream_payload = {
            'name': config['name'],
            'description': config['description'],
            'unitOfMeasurement': {
                'name': config['unit_of_measurement'],
                'symbol': config['unit_of_measurement'],
                'definition': 'http://example.org/unit_definition'
            },
            'Thing': {'@iot.id': thing_id},
            'ObservedProperty': {'@iot.id': observed_property_ids[measurement]},
            'Sensor': {'@iot.id': 1}
        }
        
        try:
            response = requests.post(
                f"{base_url}/Datastreams", 
                json=datastream_payload,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [200, 201]:
                datastream = response.json()
                datastreams[measurement] = datastream['@iot.id']
                print(f"Created Datastream for {measurement}")
            else:
                print(f"Failed to create Datastream for {measurement}: {response.text}")
        except Exception as e:
            print(f"Error creating Datastream for {measurement}: {e}")
    
    return datastreams

def upload_to_sensorthings(csv_file, base_url):
    """Upload CSV data to FROST-Server"""
    # Debug raw CSV content
    read_csv_debug(csv_file)
    
    # Attempt to read CSV with careful parsing
    try:
        # Skip the second row (units/format description) and use the first row as column headers
        df = pd.read_csv(csv_file, sep=';', header=0, skiprows=[1])
        
        print("DataFrame columns:", list(df.columns))
        print("DataFrame first few rows:")
        print(df.head())
        
        # Convert 'Server time' to datetime with explicit format
        df['Server time'] = pd.to_datetime(df['Server time'], format='%Y-%m-%d %H:%M:%S')
        
        # Create Thing
        thing_payload = {
            'name': 'CO2 Monitoring Station',
            'description': 'Automated CO2, Temperature, and Humidity Monitoring Device'
        }
    
        print("Creating Thing...")
        thing_response = requests.post(
            f"{base_url}/Things", 
            json=thing_payload,
            headers={'Content-Type': 'application/json'}
        )

        print("Response Status Code:", thing_response.status_code)
        print("Response Text:", thing_response.text)

        if thing_response.status_code in [200, 201]:
            try:
                thing = thing_response.json()
                thing_id = thing.get('@iot.id', 'Unknown')
                print(f"Created Thing with ID: {thing_id}")
            except json.JSONDecodeError:
                print("Server response is not valid JSON. Response body:", thing_response.text)
                return
        
        # Create Observed Properties
        observed_properties = {
            'CO2 concentration': None,
            'Temperature': None,
            'Humidity': None
        }
        
        for measurement in observed_properties.keys():
            op_payload = {
                'name': f'{measurement} Observed Property',
                'description': f'Measuring {measurement}',
                'definition': 'http://example.org/property_definition'
            }
            
            op_response = requests.post(
                f"{base_url}/ObservedProperties", 
                json=op_payload,
                headers={'Content-Type': 'application/json'}
            )
            
            if op_response.status_code in [200, 201]:
                op = op_response.json()
                observed_properties[measurement] = op['@iot.id']
                print(f"Created Observed Property for {measurement}")
            else:
                print(f"Failed to create Observed Property for {measurement}: {op_response.text}")
        
        # Create Datastreams
        datastreams = create_datastream(base_url, thing_id, observed_properties)
        
        # Upload Observations
        for _, row in df.iterrows():
            for measurement, datastream_id in datastreams.items():
                observation_payload = {
                    'phenomenonTime': row['Server time'].isoformat(),
                    'resultTime': row['Server time'].isoformat(),
                    'result': row[measurement],
                    'Datastream': {'@iot.id': datastream_id}
                }
                
                try:
                    response = requests.post(
                        f"{base_url}/Observations",
                        json=observation_payload,
                        headers={'Content-Type': 'application/json'}
                    )
                    
                    if response.status_code not in [200, 201]:
                        print(f"Failed to upload {measurement} observation: {response.text}")
                
                except Exception as e:
                    print(f"Error uploading {measurement} observation: {e}")
        
        print("Data upload completed successfully!")
    
    except Exception as e:
        print(f"Error in data upload process: {e}")

def fetch_datastreams(base_url, thing_id):
    """Fetch datastreams for a specific Thing."""
    try:
        url = f"{base_url}/Things({thing_id})/Datastreams"
        response = requests.get(url, headers={'Accept': 'application/json'})
        if response.status_code == 200:
            datastreams = response.json().get('value', [])
            print(f"Fetched {len(datastreams)} datastreams for Thing {thing_id}")
            return datastreams
        else:
            print(f"Failed to fetch Datastreams for Thing {thing_id}: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"Error fetching Datastreams for Thing {thing_id}: {e}")
        return []

def fetch_observations(base_url, datastream_id):
    """Fetch observations for a specific Datastream."""
    try:
        url = f"{base_url}/Datastreams({datastream_id})/Observations"
        observations = []
        while url:
            print(f"Fetching from: {url}")
            response = requests.get(url, headers={'Accept': 'application/json'})
            if response.status_code == 200:
                data = response.json()
                observations.extend(data.get('value', []))
                print(f"Fetched {len(data.get('value', []))} observations from {url}")
                url = data.get('@iot.nextLink', None)
            else:
                print(f"Failed to fetch Observations for Datastream {datastream_id}: {response.status_code} - {response.text}")
                break
        return observations
    except Exception as e:
        print(f"Error fetching Observations for Datastream {datastream_id}: {e}")
        return []

def save_observations_to_json(observations, file_name):
    """Save the fetched observations to a JSON file."""
    try:
        with open(file_name, 'w') as json_file:
            json.dump(observations, json_file, indent=4)
        print(f"Saved {len(observations)} observations to {file_name}")
    except Exception as e:
        print(f"Error saving observations to JSON: {e}")

def fetch_and_save_data(base_url, thing_ids, file_name):
    """Fetch and save the observations for multiple Things."""
    all_observations = []
    
    for thing_id in thing_ids:
        datastreams = fetch_datastreams(base_url, thing_id)
        
        for datastream in datastreams:
            datastream_id = datastream['@iot.id']
            observations = fetch_observations(base_url, datastream_id)
            all_observations.extend(observations)
    
    save_observations_to_json(all_observations, file_name)

# Configuration - Replace with your actual values
base_url = "http://localhost:8080/FROST-Server/v1.1"
thing_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]  # List of Thing IDs to fetch
file_name = "observations_data.json"  # File to save the observations data

upload_to_sensorthings(csv_file_path, base_url)

# Fetch and save the observations data
fetch_and_save_data(base_url, thing_ids, file_name)
csv_file_path = "CO2sensors_.csv"

# Execute the upload
