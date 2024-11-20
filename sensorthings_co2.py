import requests
import pandas as pd
import json
from datetime import datetime

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
    # Read the CSV file with specific parsing
    df = pd.read_csv(csv_file, sep=';', parse_dates=['Server time'])
    
    # Create Thing
    thing_payload = {
        'name': 'CO2 Monitoring Station',
        'description': 'Automated CO2, Temperature, and Humidity Monitoring Device'
    }
    
    try:
        # Create Thing
        thing_response = requests.post(
            f"{base_url}/Things", 
            json=thing_payload,
            headers={'Content-Type': 'application/json'}
        )
        
        if thing_response.status_code in [200, 201]:
            thing = thing_response.json()
            thing_id = thing['@iot.id']
            print(f"Created Thing with ID: {thing_id}")
        else:
            print(f"Failed to create Thing: {thing_response.text}")
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

# Configuration - Replace with your actual values
base_url = "http://localhost:8080/FROST-Server/v1.1"
csv_file_path = "CO2sensors_.csv"

# Execute the upload
upload_to_sensorthings(csv_file_path, base_url)