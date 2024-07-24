# mage-project-etl
This project is a test project given by PT Modular Kuliner Indonesia (hangry!) during the recruitment process. This project uses mage.ai as orchestration.

## Problem Statement
This project is designed to provide valuable sales insights that drive business growth and progress. I extract data from Google Spreadsheet and perform ETL (Extract, Transform, Load) operations using Mage. The transformed data is then exported to PostgreSQL, allowing for advanced analysis and visualization in Google Looker Studio. Through this streamlined process, I aim to uncover actionable insights that can significantly impact business strategies.

## Tech stack Used:
1. Python
2. SQL
3. Docker
4. Mage -AI for orchestration
5. Looker Studio

## Data Pipeline Design
This data pipeline uses the ETL concept with Mage.ai
![data_pipeline](assets/designs_pipeline.png)

## Setup Mage.ai With Docker compose
Now, let's build the container
```
docker compose build
```
To finish, start up the Docker container:
```
docker compose up
```
Next, open your browser and go to http://localhost:6789. You're all set to begin the course.

## Create Data Pipeline
Once you open the Mage UI, you can quickly create a Data Pipeline for either Batch or Streaming data processing.
![mage_ui](assets/create_pipeline.png)

Once the data pipeline is established, you can define various processes and stages. In this project, I set up three key components: Data Loader, Transformer, and Data Exporter.
![date_pipeline_mage](assets/Edit_pipeline.PNG)
To script the data loader in Python, click on [loader.py](scripts/loader.py). For the Python Transformer scripts, open [transform.py](scripts/transform.py), and for the Data Exporter scripts, select [exporter.py](scripts/exporter.py).

## Sales Report
visualization created using looker studio. you can see it [here](https://lookerstudio.google.com/s/utn72bqtyLM).