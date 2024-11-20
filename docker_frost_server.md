## Setting Up FROST-Server with Docker

This guide provides a step-by-step tutorial to set up the FROST-Server and its database using Docker. You will also learn how to test the setup using demo entities.

---
https://github.com/FraunhoferIOSB/FROST-Server?tab=readme-ov-file
https://fraunhoferiosb.github.io/FROST-Server/deployment/docker.html
### Prerequisites
- **Docker**: Ensure Docker is installed and running on your system. [Install Docker](https://docs.docker.com/get-docker/).
- **Docker Compose**: Ensure Docker Compose is installed. [Install Docker Compose](https://docs.docker.com/compose/install/).

---

### Quick Start Guide

#### Step 1: Download the Docker Compose File
Run the following command to download the `docker-compose.yaml` file for setting up FROST-Server:
```bash
wget https://raw.githubusercontent.com/FraunhoferIOSB/FROST-Server/v2.x/scripts/docker-compose.yaml
```

---

#### Step 2: Start the Server
Start the server and database containers using Docker Compose:
```bash
docker-compose up
```
This will start the FROST-Server and its database, making them available at `http://localhost:8080`.

---

#### Step 3: Fetch the Demo Entities
Download a JSON file containing demo entities to test the server:
```bash
wget https://gist.githubusercontent.com/hylkevds/4ffba774fe0128305047b7bcbcd2672e/raw/demoEntities.json
```

---

#### Step 4: Post Demo Entities to the Server
Use the following `curl` command to upload the demo entities to the FROST-Server:
```bash
curl -X POST -H "Content-Type: application/json" -d @demoEntities.json http://localhost:8080/FROST-Server/v1.1/Things
```

---

#### Step 5: Check the Database Status
Visit the following URL to check the status of your FROST-Server database:
[http://localhost:8080/FROST/DatabaseStatus](http://localhost:8080/FROST/DatabaseStatus)

Click the **Upgrade** button to complete the setup if required.

---

#### Step 6: Access the API
Browse to the SensorThings API endpoint:
[http://localhost:8080/FROST-Server/v1.0](http://localhost:8080/FROST-Server/v1.0)

---

### Notes
- The FROST-Server is now ready, and you can start working with SensorThings API entities.
- Customize the `docker-compose.yaml` file as needed for advanced configurations.

Enjoy! 🎉