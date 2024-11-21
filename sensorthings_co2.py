import requests
import pandas as pd
import json
import logging
import traceback
from datetime import datetime
import pandas as pd

def debug_csv_read(file_path):
    # Read CSV with multi-level header
    df = pd.read_csv(file_path, sep=';', header=[0, 1], skiprows=[1])
    
    # Flatten column names
    df.columns = df.columns.get_level_values(0)
    
    # Convert Server time to datetime
    df['Server time'] = pd.to_datetime(df['Server time'], format='%Y-%m-%d %H:%M:%S')
    
    print("DataFrame Columns:", list(df.columns))
    print("\nDataFrame Head:")
    print(df.head())
    print("\nDataFrame Info:")
    print(df.info())

# Replace with your actual CSV path
class SensorThingsManager:
    def __init__(self, base_url):
        """Initialize SensorThings Manager with robust logging"""
        self.base_url = base_url
        
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler('sensorthings_upload.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def create_sensor(self):
        """Create or fetch a generic sensor, ensuring unique identification"""
        try:
            # First, try to find an existing sensor
            sensors_response = requests.get(
                f"{self.base_url}/Sensors?$filter=name eq 'Generic Environmental Sensor'&$top=1",
                headers={'Accept': 'application/json'}
            )
            
            if sensors_response.status_code == 200:
                sensors_data = sensors_response.json()
                if sensors_data.get('value'):
                    sensor_id = sensors_data['value'][0]['@iot.id']
                    self.logger.info(f"Existing sensor found with ID: {sensor_id}")
                    return sensor_id
            
            # If no existing sensor, create a new one
            sensor_payload = {
                'name': 'Generic Environmental Sensor',
                'description': 'Multi-parameter sensor for CO2, Temperature, and Humidity',
                'encodingType': 'application/pdf',
                'metadata': 'https://example.com/sensor_specification.pdf'
            }
            
            sensor_response = requests.post(
                f"{self.base_url}/Sensors", 
                json=sensor_payload,
                headers={'Content-Type': 'application/json'}
            )
            
            if sensor_response.status_code in [200, 201]:
                sensor = sensor_response.json()
                self.logger.info(f"Created new sensor with ID: {sensor['@iot.id']}")
                return sensor['@iot.id']
            
            self.logger.error(f"Failed to create Sensor: {sensor_response.text}")
            return None
        
        except Exception as e:
            self.logger.error(f"Error in create_sensor: {e}")
            return None

    def upload_to_sensorthings(self, csv_file):
        """Optimized upload process with improved CSV handling"""
        try:
            # Read CSV with specific handling for the header rows
            df = pd.read_csv(csv_file, sep=';', header=[0, 1], skiprows=[1])
            
            # Flatten multi-level column names
            df.columns = df.columns.get_level_values(0)
            
            # Ensure proper datetime conversion
            df['Server time'] = pd.to_datetime(df['Server time'], format='%Y-%m-%d %H:%M:%S')
            
            # Get or create sensor
            sensor_id = self.create_sensor()
            if not sensor_id:
                self.logger.error("Cannot proceed without a valid Sensor")
                return
            
            # Create Thing with enhanced error checking
            thing_payload = {
                'name': 'CO2 Monitoring Station',
                'description': 'Automated CO2, Temperature, and Humidity Monitoring Device',
                'properties': {
                    'location': 'Monitoring Site',
                    'organization': 'Your Organization'
                }
            }
            
            thing_response = requests.post(
                f"{self.base_url}/Things", 
                json=thing_payload,
                headers={'Content-Type': 'application/json'}
            )

            if thing_response.status_code not in [200, 201]:
                self.logger.error(f"Thing creation failed: {thing_response.text}")
                return
            
            # Fetch the most recently created Thing
            things_response = requests.get(
                f"{self.base_url}/Things?$filter=name eq 'CO2 Monitoring Station'&$orderby=@iot.id desc&$top=1",
                headers={'Accept': 'application/json'}
            )
            
            if things_response.status_code != 200 or not things_response.json().get('value'):
                self.logger.error("Could not retrieve created Thing")
                return
            
            thing_id = things_response.json()['value'][0]['@iot.id']
            
            # Create Observed Properties with centralized error handling
            measurement_properties = {
                'CO2 concentration': {
                    'name': 'CO2 Concentration Observed Property',
                    'description': 'Measuring CO2 concentration',
                    'definition': 'http://example.org/co2_property'
                },
                'Temperature': {
                    'name': 'Temperature Observed Property',
                    'description': 'Measuring ambient temperature',
                    'definition': 'http://example.org/temperature_property'
                },
                'Humidity': {
                    'name': 'Humidity Observed Property',
                    'description': 'Measuring relative humidity',
                    'definition': 'http://example.org/humidity_property'
                }
            }
            
            observed_properties = {}
            for measurement, config in measurement_properties.items():
                op_payload = {
                    'name': config['name'],
                    'description': config['description'],
                    'definition': config['definition']
                }
                
                op_response = requests.post(
                    f"{self.base_url}/ObservedProperties", 
                    json=op_payload,
                    headers={'Content-Type': 'application/json'}
                )
                
                if op_response.status_code in [200, 201]:
                    op_data = op_response.json()
                    observed_properties[measurement] = op_data['@iot.id']
                else:
                    self.logger.error(f"Failed to create Observed Property for {measurement}")
            
            # Create Datastreams
            datastreams = {}
            for measurement, op_id in observed_properties.items():
                datastream_payload = {
                    'name': f'{measurement} Datastream',
                    'description': f'Measures {measurement}',
                    'unitOfMeasurement': {
                        'name': measurement,
                        'symbol': measurement,
                        'definition': 'http://example.org/unit_definition'
                    },
                    'Thing': {'@iot.id': thing_id},
                    'ObservedProperty': {'@iot.id': op_id},
                    'Sensor': {'@iot.id': sensor_id}
                }
                
                ds_response = requests.post(
                    f"{self.base_url}/Datastreams", 
                    json=datastream_payload,
                    headers={'Content-Type': 'application/json'}
                )
                
                if ds_response.status_code in [200, 201]:
                    ds_data = ds_response.json()
                    datastreams[measurement] = ds_data['@iot.id']
                else:
                    self.logger.error(f"Failed to create Datastream for {measurement}")
            
            # Upload Observations with proper handling
            for _, row in df.iterrows():
                for measurement in ['CO2 concentration', 'Temperature', 'Humidity']:
                    if measurement in datastreams:
                        observation_payload = {
                            'phenomenonTime': row['Server time'].isoformat(),
                            'resultTime': row['Server time'].isoformat(),
                            'result': row[measurement],
                            'Datastream': {'@iot.id': datastreams[measurement]}
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
            self.logger.error(f"Critical error in data upload: {e}")
            self.logger.error(traceback.format_exc())

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
    debug_csv_read('CO2sensors_.csv')

    # Create manager instance
    manager = SensorThingsManager(BASE_URL)

    # Upload CSV data
    manager.upload_to_sensorthings(CSV_FILE_PATH)

    # Fetch and save observations
    manager.fetch_and_save_data(THING_IDS, OUTPUT_FILE)
    
    
    