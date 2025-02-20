# Project Goal
Get the emissions reports for maritime transport, do ETL, store the data in the cloud, and build a dashboard to visualise the data. Most likely, the project goal will change at the some point.

# Data Acquisition
A scrapper uses Selenium to get the reports from the following website https://mrv.emsa.europa.eu/#public/emission-report

# Development
## Formatting
You can run ruff to format the codebase after python requirements have be installed:

```
ruff format
```


# Create a systemd service to run docker compose on startup
Create a new systemd service file, e.g., docker-compose.service, in the /etc/systemd/system/ directory. You can use a text editor like nano or vim to create the file:
```
sudo nano /etc/systemd/system/docker-compose.service
```

Add the following content to the docker-compose.service file, replacing /path/to/your/docker-compose.yml with the actual path to your Docker Compose file:
```
[Unit]
Description=Docker Compose Service
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/path/to/your/docker-compose/directory
ExecStart=/usr/local/bin/docker-compose up -d
ExecStop=/usr/local/bin/docker-compose down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
```

Reload the systemd daemon to make it aware of the new service file:
```
sudo systemctl daemon-reload
```

Enable the service to start automatically on system boot:
```
systemctl enable docker-compose.service
```

Start the service:
```
sudo systemctl start docker-compose.service
```

After completing these steps, your Docker Compose services should start automatically when the EC2 instance boots up or restarts.
You can check the status of the service using the following command:
```
sudo systemctl status docker-compose.service
```