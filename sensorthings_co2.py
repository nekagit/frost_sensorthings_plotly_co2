import requests
import pandas as pd
import json
import logging
import traceback
from datetime import datetime

class SensorThingsManager:
    def __init__(self, base_url):
        """Initialize SensorThings Manager"""
        self.base_url = base_url
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler('sensorthings_upload.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def read_csv_debug(self, csv_file):
        """Debug CSV reading process"""
        try:
            with open(csv_file, 'r') as file:
                self.logger.info("Raw CSV content:")
                self.logger.info(file.read())
        except Exception as e:
            self.logger.error(f"Error reading file: {e}")

    def create_datastream(self, thing_id, observed_property_ids):
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
                    f"{self.base_url}/Datastreams", 
                    json=datastream_payload,
                    headers={'Content-Type': 'application/json'}
                )
                
                if response.status_code in [200, 201]:
                    datastream = response.json()
                    datastreams[measurement] = datastream['@iot.id']
                    self.logger.info(f"Created Datastream for {measurement}")
                else:
                    self.logger.error(f"Failed to create Datastream for {measurement}: {response.text}")
            except Exception as e:
                self.logger.error(f"Error creating Datastream for {measurement}: {e}")
        
        return datastreams

    def upload_to_sensorthings(self, csv_file):
        """Upload CSV data to FROST-Server"""
        # Debug raw CSV content
        self.read_csv_debug(csv_file)
        
        try:
            # Skip the second row (units/format description) and use the first row as column headers
            df = pd.read_csv(csv_file, sep=';', header=0, skiprows=[1])
            
            self.logger.info(f"DataFrame columns: {list(df.columns)}")
            self.logger.info("DataFrame first few rows:")
            self.logger.info(str(df.head()))
            
            # Convert 'Server time' to datetime with explicit format
            df['Server time'] = pd.to_datetime(df['Server time'], format='%Y-%m-%d %H:%M:%S')
            
            # Create Thing
            thing_payload = {
                'name': 'CO2 Monitoring Station',
                'description': 'Automated CO2, Temperature, and Humidity Monitoring Device'
            }
        
            self.logger.info("Creating Thing...")
            thing_response = requests.post(
                f"{self.base_url}/Things", 
                json=thing_payload,
                headers={'Content-Type': 'application/json'}
            )

            self.logger.info(f"Response Status Code: {thing_response.status_code}")
            self.logger.info(f"Response Text: {thing_response.text}")

            if thing_response.status_code not in [200, 201]:
                self.logger.error("Failed to create Thing")
                return
            
            try:
                thing = thing_response.json()
                thing_id = thing.get('@iot.id', 'Unknown')
                self.logger.info(f"Created Thing with ID: {thing_id}")
            except json.JSONDecodeError:
                self.logger.error("Server response is not valid JSON")
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
                    f"{self.base_url}/ObservedProperties", 
                    json=op_payload,
                    headers={'Content-Type': 'application/json'}
                )
                
                if op_response.status_code in [200, 201]:
                    op = op_response.json()
                    observed_properties[measurement] = op['@iot.id']
                    self.logger.info(f"Created Observed Property for {measurement}")
                else:
                    self.logger.error(f"Failed to create Observed Property for {measurement}: {op_response.text}")
            
            # Create Datastreams
            datastreams = self.create_datastream(thing_id, observed_properties)
            
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
                            f"{self.base_url}/Observations",
                            json=observation_payload,
                            headers={'Content-Type': 'application/json'}
                        )
                        
                        if response.status_code not in [200, 201]:
                            self.logger.error(f"Failed to upload {measurement} observation: {response.text}")
                    
                    except Exception as e:
                        self.logger.error(f"Error uploading {measurement} observation: {e}")
            
            self.logger.info("Data upload completed successfully!")
        
        except Exception as e:
            self.logger.error(f"Error in data upload process: {e}")

    def fetch_datastreams(self, thing_id):
        """Fetch datastreams for a specific Thing."""
        try:
            url = f"{self.base_url}/Things({thing_id})/Datastreams"
            response = requests.get(url, headers={'Accept': 'application/json'})
            if response.status_code == 200:
                datastreams = response.json().get('value', [])
                self.logger.info(f"Fetched {len(datastreams)} datastreams for Thing {thing_id}")
                return datastreams
            else:
                self.logger.error(f"Failed to fetch Datastreams for Thing {thing_id}: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            self.logger.error(f"Error fetching Datastreams for Thing {thing_id}: {e}")
            return []

    def fetch_observations(self, datastream_id):
        """Fetch observations for a specific Datastream."""
        try:
            url = f"{self.base_url}/Datastreams({datastream_id})/Observations"
            observations = []
            while url:
                self.logger.info(f"Fetching from: {url}")
                response = requests.get(url, headers={'Accept': 'application/json'})
                if response.status_code == 200:
                    data = response.json()
                    observations.extend(data.get('value', []))
                    self.logger.info(f"Fetched {len(data.get('value', []))} observations from {url}")
                    url = data.get('@iot.nextLink', None)
                else:
                    self.logger.error(f"Failed to fetch Observations for Datastream {datastream_id}: {response.status_code} - {response.text}")
                    break
            return observations
        except Exception as e:
            self.logger.error(f"Error fetching Observations for Datastream {datastream_id}: {e}")
            return []

    def fetch_and_save_data(self, thing_ids, file_name):
        """Fetch and save the observations for multiple Things."""
        all_observations = []
        
        for thing_id in thing_ids:
            datastreams = self.fetch_datastreams(thing_id)
            
            for datastream in datastreams:
                datastream_id = datastream['@iot.id']
                observations = self.fetch_observations(datastream_id)
                all_observations.extend(observations)
        
        self.save_observations_to_json(all_observations, file_name)

    def save_observations_to_json(self, observations, file_name):
        """Save the fetched observations to a JSON file."""
        try:
            with open(file_name, 'w') as json_file:
                json.dump(observations, json_file, indent=4)
            self.logger.info(f"Saved {len(observations)} observations to {file_name}")
        except Exception as e:
            self.logger.error(f"Error saving observations to JSON: {e}")

# Usage example
if __name__ == "__main__":
    # Configuration - Replace with your actual values
    BASE_URL = "http://localhost:8080/FROST-Server/v1.1"
    CSV_FILE_PATH = "CO2sensors_.csv"
    THING_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    OUTPUT_FILE = "observations_data.json"

    # Create manager instance
    manager = SensorThingsManager(BASE_URL)

    # Upload CSV data
    manager.upload_to_sensorthings(CSV_FILE_PATH)

    # Fetch and save observations
    manager.fetch_and_save_data(THING_IDS, OUTPUT_FILE)