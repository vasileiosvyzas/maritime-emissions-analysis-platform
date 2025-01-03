<p align="center">
  <img src="frontend/public/icon_for_the_name_shipcarbontracker_blue%20(1).png" width="200" height="200">
</p>

# Maritime Emissions Analysis Platform
A full-stack data analytics platform built on AWS, featuring automated data pipelines, interactive dashboards, and API integration, all orchestrated with Docker containers.

## Project Overview
This project is a comprehensive data analytics platform that:
- Collects CO2 emissions reports from <a href="https://mrv.emsa.europa.eu/#public/emission-report" target="_blank">EU-MRV system</a> through Python scripts and stores it in AWS S3
- Processes raw data through an ETL pipeline using AWS Glue
- Provides data access through a REST API (AWS API Gateway)
- Visualizes data through an Apache Superset dashboard
- Features a React-based landing page

## Architecture
- **Frontend**: React.js landing page
- **Backend**: 
  - AWS API Gateway for REST endpoints
  - Python ETL scripts for data processing
  - Apache Superset for data visualization
- **Infrastructure**:
  - AWS EC2 for hosting
  - Docker containers for service orchestration
  - Nginx as reverse proxy
  - AWS S3 for data storage
  - AWS Route 53 for domain management

## Technologies Used
- **Cloud & Infrastructure**:
  - AWS (EC2, S3, API Gateway, Route 53)
  - Docker & Container Orchestration
  - Nginx
- **Backend & Data**:
  - Python
  - Apache Superset
  - ETL Pipeline
  - API Development
- **Frontend**:
  - React.js
  - HTML/CSS
  - JavaScript

## Setup and Installation

### Prerequisites
- AWS Account
- Docker and Docker Compose
- Node.js
- Python 3.x

### Local Development Setup
1. Clone the repository:
```bash
git clone [repository-url]
cd [repository-name]
```

2. Start the frontend application:
```bash
cd frontend
npm install
npm start
```

3. Run the Docker containers:
```bash
docker-compose up -d
```

4. Initialize Superset (first time only):
```bash
docker-compose -f superset-docker-compose.yml exec superset superset-init
```

### Production Deployment
![deployment](https://github.com/user-attachments/assets/10ff226b-9d2e-4d63-8b7d-70c19fb821ca)

1. Configure AWS services:
   - Set up EC2 instance
   - Configure S3 bucket
   - Set up API Gateway
   - Configure Route 53 for domain management

2. Deploy application:
```bash
docker-compose -f docker-compose.prod.yml up -d
```

3. Set up SSL certificates:
```bash
sudo certbot --nginx -d yourdomain.com -d www.yourdomain.com -d dashboard.yourdomain.com
```

## Project Structure
```
project/
├── docker-compose.yml           # Main application composition
├── superset-docker-compose.yml  # Superset setup
├── frontend/                    # React application
│   ├── public/
│   ├── src/
│   └── Dockerfile
├── backend/                    # Backend
│   ├── src/
│   ├── app/
│   └── Dockerfile
│   └── compose.yml
├── nginx/                       # Nginx configuration
│   └── conf.d/
└── superset/                   # Superset configuration
    └── superset_config.py
```

## Future Development / Roadmap
### Infrastructure Improvements
- [ ] Move containers to AWS ECS
- [ ] Automate infrastructure creation and deletion with Terraform
- [ ] Set up monitoring and alerting with CloudWatch

### Data Pipeline Enhancements
- [ ] Add data quality checks
- [ ] Implement error handling and retry mechanisms
- [ ] Implement an automated testing suite with Pytests
- [ ] Create automated data backup system

## License
[Add license information here]
