import requests
import pandas as pd
import json
import logging
import traceback
from datetime import datetime
import pandas as pd
import sys

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
            print(sensors_response)
            if sensors_response.status_code == 200:
                sensors_data = sensors_response.json()
                if sensors_data.get('value'):
                    sensor_id = sensors_data['value'][0]['@iot.id']
                    # self.logger.info(f"Existing sensor found with ID: {sensor_id}")
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
            print(sensors_response)
            
            if sensor_response.status_code in [200, 201]:
                sensor = sensor_response.json()
                # self.logger.info(f"Created new sensor with ID: {sensor['@iot.id']}")
                return sensor['@iot.id']
            
            self.logger.error(f"Failed to create Sensor: {sensor_response.text}")
            return None
        
        except Exception as e:
            self.logger.error(f"Error in create_sensor: {e}")
            return None

    def create_thing(self, name: str, description: str, properties: Dict) -> Optional[int]:
        """
        Create a new Thing in the SensorThings API.
        
        Args:
            name: Name of the Thing
            description: Description of the Thing
            properties: Dictionary of additional properties
            
        Returns:
            Thing ID if successful, None if failed
        """
        try:
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
        """
        Fetch Things from the SensorThings API with optional filtering.
        
        Args:
            filter_query: Optional OData filter query
            
        Returns:
            List of Thing objects
        """
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

    def create_datastream(self, name: str, description: str, 
                         thing_id: int, observed_property_id: int, 
                         sensor_id: int, unit_of_measurement: Dict) -> Optional[int]:
        """
        Create a new Datastream in the SensorThings API.
        
        Args:
            name: Name of the Datastream
            description: Description of the Datastream
            thing_id: ID of the associated Thing
            observed_property_id: ID of the associated ObservedProperty
            sensor_id: ID of the associated Sensor
            unit_of_measurement: Dictionary containing unit details
            
        Returns:
            Datastream ID if successful, None if failed
        """
        try:
            datastream_payload = {
                'name': name,
                'description': description,
                'unitOfMeasurement': unit_of_measurement,
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

    def fetch_datastreams(self, thing_id: Optional[int] = None) -> List[Dict]:
        """
        Fetch Datastreams from the SensorThings API.
        
        Args:
            thing_id: Optional Thing ID to filter Datastreams
            
        Returns:
            List of Datastream objects
        """
        try:
            url = f"{self.base_url}/Things({thing_id})/Datastreams" if thing_id else f"{self.base_url}/Datastreams"
            
            response = requests.get(
                url,
                headers={'Accept': 'application/json'}
            )
            
            if response.status_code == 200:
                datastreams = response.json().get('value', [])
                self.logger.info(f"Fetched {len(datastreams)} Datastreams")
                return datastreams
            else:
                self.logger.error(f"Failed to fetch Datastreams: {response.text}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error fetching Datastreams: {str(e)}")
            return []

    def create_observation(self, datastream_id: int, result: Union[float, int, str],
                         phenomenon_time: str, result_time: Optional[str] = None) -> Optional[int]:
        """
        Create a new Observation in the SensorThings API.
        
        Args:
            datastream_id: ID of the associated Datastream
            result: The observation result value
            phenomenon_time: Time when the observation was made
            result_time: Optional time when the result was generated
            
        Returns:
            Observation ID if successful, None if failed
        """
        try:
            observation_payload = {
                'result': result,
                'phenomenonTime': phenomenon_time,
                'resultTime': result_time or phenomenon_time,
                'Datastream': {'@iot.id': datastream_id}
            }
            
            response = requests.post(
                f"{self.base_url}/Observations",
                json=observation_payload,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [200, 201]:
                observation_data = response.json()
                self.logger.info(f"Created Observation with ID: {observation_data['@iot.id']}")
                return observation_data['@iot.id']
            else:
                self.logger.error(f"Failed to create Observation: {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating Observation: {str(e)}")
            return None

    def fetch_observations(self, datastream_id: Optional[int] = None, 
                         time_filter: Optional[str] = None) -> List[Dict]:
        """
        Fetch Observations from the SensorThings API.
        
        Args:
            datastream_id: Optional Datastream ID to filter observations
            time_filter: Optional time filter string (e.g., "phenomenonTime gt 2024-01-01T00:00:00Z")
            
        Returns:
            List of Observation objects
        """
        try:
            if datastream_id:
                url = f"{self.base_url}/Datastreams({datastream_id})/Observations"
            else:
                url = f"{self.base_url}/Observations"
                
            if time_filter:
                url += f"?$filter={time_filter}"
            
            observations = []
            while url:
                response = requests.get(
                    url,
                    headers={'Accept': 'application/json'}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    observations.extend(data.get('value', []))
                    url = data.get('@iot.nextLink')  # Handle pagination
                else:
                    self.logger.error(f"Failed to fetch Observations: {response.text}")
                    break
            
            self.logger.info(f"Fetched {len(observations)} Observations")
            return observations
                
        except Exception as e:
            self.logger.error(f"Error fetching Observations: {str(e)}")
            return []

# Usage example
if __name__ == "__main__":
    # Configuration - Replace with your actual values
    BASE_URL = "http://localhost:8080/FROST-Server/v1.1"
    CSV_FILE_PATH = "CO2sensors_.csv"
    THING_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    OUTPUT_FILE = "observations_data.json"
    # helper.debug_csv_read('CO2sensors_.csv')
    # Create manager instance
    manager = SensorThingsManager(BASE_URL)
    
    # Upload CSV data
    manager.upload_to_sensorthings(CSV_FILE_PATH)
    print('finished upload')

    # # Fetch and save observations
    # manager.fetch_and_save_data(THING_IDS, OUTPUT_FILE)
    
    
    