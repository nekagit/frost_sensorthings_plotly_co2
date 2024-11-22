import requests
import json
from typing import List, Dict
import logging

class SensorThingsMapDataFetcher:
    def __init__(self, base_url: str):
        """
        Initialize SensorThings Map Data Fetcher
        
        :param base_url: Base URL of the FROST server
        """
        self.base_url = base_url
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_things_with_locations(self) -> List[Dict]:
        """
        Fetch Things with their geographic locations
        
        :return: List of Things with their location details
        """
        try:
            # Fetch Things with their Locations
            url = f"{self.base_url}/Things?$expand=Locations"
            response = requests.get(url, headers={'Accept': 'application/json'})
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Things: {response.text}")
                return []
            
            things_data = response.json().get('value', [])
            
            # Filter and process Things with valid locations
            sensor_locations = []
            for thing in things_data:
                locations = thing.get('Locations', [])
                for location in locations:
                    if 'location' in location:
                        sensor_location = {
                            'thing_id': thing.get('@iot.id'),
                            'thing_name': thing.get('name', 'Unnamed Sensor'),
                            'description': thing.get('description', 'No description'),
                            'latitude': location['location']['coordinates'][1],
                            'longitude': location['location']['coordinates'][0]
                        }
                        
                        # Fetch latest observations for this Thing
                        latest_observations = self.get_latest_observations(thing.get('@iot.id'))
                        sensor_location.update(latest_observations)
                        
                        sensor_locations.append(sensor_location)
            
            return sensor_locations
        
        except Exception as e:
            self.logger.error(f"Error fetching Things locations: {e}")
            return []

    def get_latest_observations(self, thing_id: int) -> Dict:
        """
        Fetch the latest observations for a specific Thing
        
        :param thing_id: ID of the Thing
        :return: Dictionary of latest observations
        """
        try:
            # Fetch Datastreams for the Thing
            datastreams_url = f"{self.base_url}/Things({thing_id})/Datastreams"
            datastreams_response = requests.get(datastreams_url, headers={'Accept': 'application/json'})
            
            if datastreams_response.status_code != 200:
                return {}
            
            datastreams = datastreams_response.json().get('value', [])
            
            latest_observations = {}
            for datastream in datastreams:
                # Fetch latest observation for each datastream
                obs_url = f"{self.base_url}/Datastreams({datastream['@iot.id']})/Observations?$orderby=phenomenonTime desc&$top=1"
                obs_response = requests.get(obs_url, headers={'Accept': 'application/json'})
                
                if obs_response.status_code == 200:
                    observations = obs_response.json().get('value', [])
                    if observations:
                        latest_obs = observations[0]
                        obs_name = datastream.get('name', 'Unknown Measurement')
                        latest_observations[obs_name] = {
                            'value': latest_obs.get('result'),
                            'time': latest_obs.get('phenomenonTime')
                        }
            
            return latest_observations
        
        except Exception as e:
            self.logger.error(f"Error fetching observations for Thing {thing_id}: {e}")
            return {}

    def export_sensor_locations(self, output_file: str = 'sensor_locations.json'):
        """
        Export sensor locations to a JSON file
        
        :param output_file: Path to output JSON file
        """
        sensor_locations = self.get_things_with_locations()
        
        with open(output_file, 'w') as f:
            json.dump(sensor_locations, f, indent=4)
        
        self.logger.info(f"Exported {len(sensor_locations)} sensor locations to {output_file}")
        return sensor_locations

# Example usage
if __name__ == "__main__":
    BASE_URL = "http://localhost:8080/FROST-Server/v1.1"
    
    # Create fetcher instance
    fetcher = SensorThingsMapDataFetcher(BASE_URL)
    
    # Export and print sensor locations
    sensor_locations = fetcher.export_sensor_locations()
    print(json.dumps(sensor_locations, indent=2))