import requests
import pandas as pd
import json
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Union

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

    def create_thing(self, name: str, description: str, properties: Dict) -> Optional[int]:
        """Create a new Thing in the SensorThings API."""
        try:
            # First check if Thing already exists
            things_response = requests.get(
                f"{self.base_url}/Things?$filter=name eq '{name}'",
                headers={'Accept': 'application/json'}
            )
            
            if things_response.status_code == 200:
                things_data = things_response.json()
                if things_data.get('value'):
                    thing_id = things_data['value'][0]['@iot.id']
                    self.logger.info(f"Existing Thing found with ID: {thing_id}")
                    return thing_id
            
            thing_payload = {
                'name': name,
                'description': description,
                'properties': properties
            }
            
            response = requests.post(
                f"{self.base_url}/Things",
                json=thing_payload,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [200, 201]:
                thing_data = response.json()
                self.logger.info(f"Created Thing with ID: {thing_data['@iot.id']}")
                return thing_data['@iot.id']
            else:
                self.logger.error(f"Failed to create Thing: {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating Thing: {str(e)}")
            return None

    def fetch_things(self, filter_query: Optional[str] = None) -> List[Dict]:
        """Fetch Things from the SensorThings API with optional filtering."""
        try:
            url = f"{self.base_url}/Things"
            if filter_query:
                url += f"?$filter={filter_query}"
                
            response = requests.get(
                url,
                headers={'Accept': 'application/json'}
            )
            
            if response.status_code == 200:
                things = response.json().get('value', [])
                self.logger.info(f"Fetched {len(things)} Things")
                return things
            else:
                self.logger.error(f"Failed to fetch Things: {response.text}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error fetching Things: {str(e)}")
            return []

    def create_observed_property(self, name: str, description: str, definition: str) -> Optional[int]:
        """Create or fetch an ObservedProperty."""
        try:
            # Check if property already exists
            response = requests.get(
                f"{self.base_url}/ObservedProperties?$filter=name eq '{name}'",
                headers={'Accept': 'application/json'}
            )
            self.logger.info('asdfasdfasdf')
            self.logger.info(response.json()['value'][0]['@iot.id'])
            self.logger.info('asdfasdfasdf')
            if response.status_code == 200:
                data = response.json()
                if data.get('value'):
                    return data['value'][0]['@iot.id']
            
            # Create new property if it doesn't exist
            payload = {
                'name': name,
                'description': description,
                'definition': definition
            }
            
            response = requests.post(
                f"{self.base_url}/ObservedProperties",
                json=payload,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [200, 201]:
                return response.json()['@iot.id']
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error creating ObservedProperty: {str(e)}")
            return None

    def create_datastream(self, name: str, description: str, 
                         thing_id: int, observed_property_id: int, 
                         sensor_id: int, unit_of_measurement: Dict) -> Optional[int]:
        """Create a new Datastream in the SensorThings API."""
        try:
            # Check if datastream already exists
            response = requests.get(
                f"{self.base_url}/Datastreams?$filter=name eq '{name}'",
                headers={'Accept': 'application/json'}
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('value'):
                    return data['value'][0]['@iot.id']
            
            datastream_payload = {
                'name': name,
                'description': description,
                'unitOfMeasurement': unit_of_measurement,
                'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                'Thing': {'@iot.id': thing_id},
                'ObservedProperty': {'@iot.id': observed_property_id},
                'Sensor': {'@iot.id': sensor_id}
            }
            
            response = requests.post(
                f"{self.base_url}/Datastreams",
                json=datastream_payload,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [200, 201]:
                datastream_data = response.json()
                self.logger.info(f"Created Datastream with ID: {datastream_data['@iot.id']}")
                return datastream_data['@iot.id']
            else:
                self.logger.error(f"Failed to create Datastream: {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating Datastream: {str(e)}")
            return None


    def process_csv(self, csv_path: str) -> pd.DataFrame:
        """Process the environmental CSV file with specific format handling."""
        try:
            # Read CSV with semicolon delimiter, skipping the units row
            df = pd.read_csv(csv_path, delimiter=';', skiprows=[1])
            
            # Rename columns to match our needs
            df.columns = ['server_time', 'sensor_time', 'co2', 'temperature', 'humidity']
            
            # Convert time columns to datetime
            df['server_time'] = pd.to_datetime(df['server_time'])
            # Remove timezone info from sensor_time for consistency
            df['sensor_time'] = pd.to_datetime(df['sensor_time'].str.split('+').str[0])
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing CSV: {str(e)}")
            raise
        
    def create_feature_of_interest(self, name: str, description: str, location: Dict) -> Optional[int]:
        """Create a FeatureOfInterest in the SensorThings API."""
        try:
            # Check if FeatureOfInterest already exists
            response = requests.get(
                f"{self.base_url}/FeaturesOfInterest?$filter=name eq '{name}'",
                headers={'Accept': 'application/json'}
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('value'):
                    self.logger.info(f"Found existing FeatureOfInterest with ID: {data['value'][0]['@iot.id']}")
                    return data['value'][0]['@iot.id']
            
            # Create new FeatureOfInterest if it doesn't exist
            foi_payload = {
                'name': name,
                'description': description,
                'encodingType': 'application/geo+json',
                'feature': {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [location['coordinates'][0], location['coordinates'][1]]
                    },
                    'properties': {
                        'name': name
                    }
                }
            }
            
            response = requests.post(
                f"{self.base_url}/FeaturesOfInterest",
                json=foi_payload,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [200, 201]:
                foi_data = response.json()
                self.logger.info(f"Created new FeatureOfInterest with ID: {foi_data['@iot.id']}")
                return foi_data['@iot.id']
            else:
                self.logger.error(f"Failed to create FeatureOfInterest: {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating FeatureOfInterest: {str(e)}")
            return None

    def create_observation(self, datastream_id: int, result: Union[float, int, str],
                    phenomenon_time: str, feature_of_interest_id: int,
                    result_time: Optional[str] = None) -> Optional[int]:
        """Create a new Observation in the SensorThings API."""
        try:
            # Format the timestamp as an ISO 8601 interval
            formatted_time = f"{phenomenon_time}Z/{phenomenon_time}Z"
            
            observation_payload = {
                'result': result,
                'phenomenonTime': formatted_time,
                'resultTime': f"{result_time or phenomenon_time}Z",
                'Datastream': {'@iot.id': datastream_id},
                'FeatureOfInterest': {'@iot.id': feature_of_interest_id}
            }
            
            response = requests.post(
                f"{self.base_url}/Observations",
                json=observation_payload,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [200, 201]:
                observation_data = response.json()
                self.logger.info(f"Created observation with result {result} at time {formatted_time}")
                return observation_data['@iot.id']
            else:
                self.logger.error(f"Failed to create Observation: {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating Observation: {str(e)}")
            return None

    def upload_environmental_data(self, csv_path: str, location_name: str = "Default Location",
                                latitude: float = 0.0, longitude: float = 0.0):
        """Upload environmental data from CSV to SensorThings API."""
        try:
            # Process CSV
            df = self.process_csv(csv_path)
            
            # Create or get sensor
            sensor_id = self.create_sensor()
            if not sensor_id:
                raise Exception("Failed to create/fetch sensor")
            
            # Create Thing for the location
            thing_properties = {
                'application': 'Environmental Monitoring',
                'deployment_date': datetime.now().isoformat(),
                'location_name': location_name
            }
            
            thing_id = self.create_thing(
                name=f"Environmental Station - {location_name}",
                description=f"Environmental monitoring station at {location_name}",
                properties=thing_properties
            )
            
            if not thing_id:
                raise Exception("Failed to create Thing")

            # Create FeatureOfInterest
            foi_id = self.create_feature_of_interest(
                name=f"Location - {location_name}",
                description=f"Monitoring location at {location_name}",
                location={
                    'coordinates': [longitude, latitude],
                    'type': 'Point'
                }
            )
            
            if not foi_id:
                raise Exception("Failed to create FeatureOfInterest")
            
            # Rest of the method remains the same until the observation creation part
            
            # Upload observations
            for _, row in df.iterrows():
                # Format the timestamp properly
                timestamp = row['sensor_time'].strftime('%Y-%m-%dT%H:%M:%S')
                
                # Create observations for each measurement
                self.create_observation(
                    datastream_id=datastreams['CO2'],
                    result=float(row['co2']),
                    phenomenon_time=timestamp,
                    feature_of_interest_id=foi_id
                )
                
                self.create_observation(
                    datastream_id=datastreams['Temperature'],
                    result=float(row['temperature']),
                    phenomenon_time=timestamp,
                    feature_of_interest_id=foi_id
                )
                
                self.create_observation(
                    datastream_id=datastreams['Humidity'],
                    result=float(row['humidity']),
                    phenomenon_time=timestamp,
                    feature_of_interest_id=foi_id
                )
            
            self.logger.info(f"Successfully uploaded data for {location_name}")
            return thing_id
            
        except Exception as e:
            self.logger.error(f"Error uploading environmental data: {str(e)}")
            raise

if __name__ == "__main__":
    # Configuration
    BASE_URL = "http://localhost:8080/FROST-Server/v1.1"
    CSV_FILE_PATH = "CO2sensors_.csv"
    LOCATION_NAME = "Room A1"
    LATITUDE = 48.7904  # Example latitude
    LONGITUDE = 9.1917  # Example longitude
    
    # Create manager instance and upload data
    manager = SensorThingsManager(BASE_URL)
    manager.upload_environmental_data(CSV_FILE_PATH, LOCATION_NAME, LATITUDE, LONGITUDE)