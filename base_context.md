
## Problem: [Basic ETL pipeline](./docs/data_side_project.pdf)

## Solution:
 
### Data source: https://openweathermap.org/api
  - API docs: https://openweathermap.org/api/current?collection=current_forecast#one
  - Public API key see  [.env](.env)
### High level Design:
1. ETL in three main modules
  a. Ingestor
    - Ingest data from data source to database - PostgreSQL as requirement from problem
    - This module include multiples specific ingestor for each kind of data. The data ingestion logic for the data source we choose is consider as a sub-module inside the ingestor. (e.g: ingestor/pkg/open_weather)

  b. Extractor
    - Extractor data from the database to local storage under `JSON` format (e.g., `data/open_weather/raw/year=2026/month=10/day=20/20261020.json`)
    - This module include multiples specific sxtractor for each kind of data. The data extraction logic for the data source we choose is consider as a sub-module inside the extractor. (e.g: extractor/pkg/open_weather)

  c Transformer
    - Process data produce by extractor to normalize data: structure, format, date time standard, etc
    - Save processed data to the file. (e.g., `data/open_weather/v1/year=2026/month=10/day=20/20261020.json`)
    - Implement by using Python 

2. Data storage
 - Local simulation of a Data Lake
 - In term of OA we do storage data in local storage

3. Orchestrator
 - Ingestor running as a service so keep in running as docker container - because the requirement request ingestor fetch data every 30s. So running a long live service is make more sense than a cron job.
 - Extractor and Tranformeer have no specific on how frequently. So, consider as batch job. E.g hourly. Introduce `Prefect` as an orchestrator for deploy/running/manage pipeline of extract and transform data.

4. Other requirements follow the PDF file



### Implementation
 - setup and run everything with docker compose


