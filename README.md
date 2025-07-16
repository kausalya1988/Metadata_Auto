## 🌟 Overview

This is a comprehensive data engineering project aimed at building a robust data pipeline. This pipeline extracts data, processes it using AWS Glue and dbt, and finally loads the transformed data into Snowflake. The entire workflow is orchestrated by Apache Airflow using DAGs.

## 🗂 Project Structure

source/  <br />
│  <br />
├── dags/  <br />
│   └── <Airflow DAGs for orchestration>  <br />
│  <br />
├── data-dynamodb/  <br />
│   └── <Scripts and configurations for extracting data from DynamoDB>  <br />
│  <br />
├── dbt/  <br />
│   └── <DBT project for data transformation>  <br />
│  <br />
├── ddls-snowflake/  <br />
│   └── <Snowflake DDLs (Data Definition Language scripts)>  <br />
│  <br />
├── glue/  <br />
│   └── <AWS Glue jobs for data processing>  <br />
│  <br />
└── init_script/  <br />
    └── <Initialization scripts to set up the environment and prerequisites>  <br />

📂 Directory Breakdown

dags/: Contains Airflow DAGs (Directed Acyclic Graphs) that define the workflow for orchestrating the data pipeline.

data-dynamodb/: Houses scripts and configurations necessary to extract data from DynamoDB.

dbt/: Contains the DBT (Data Build Tool) project responsible for transforming data within the pipeline.

ddls-snowflake/: Holds Snowflake DDL scripts used to define tables, schemas, and other database objects.

glue/: Includes AWS Glue jobs responsible for processing and transforming data before it reaches Snowflake.

init_script/: Contains initialization scripts to set up the environment, including dependencies, connections, and infrastructure.

## 🚀 Getting Started

### Prerequisites

Ensure the libraries in the file requirements.txt are installed and configured on your system:

python3 -m venv venv  <br />
source venv/bin/activate  <br />
pip install -r requirements.txt  <br />

## Setup

## 1-Initialize the Project

### First create a project configuration YAML file based on the example in cookiecutter_init_example.yaml. 

default_context:  <br />
    jv: "country, ex:fr"  <br />
    source: "project source, ex: gifp"  <br />
    environment: "project environnement, ex: dev"  <br />


### Then, generate the project directly from github toolkit:    

cookiecutter https://github.psa-cloud.com/lak00/tools-script/toolkit/cookiecutter-dbt.git --no-input --config-file cookiecutter_init_example.yaml

### Or, clone the toolkit in your local, then generate the project:

git clone https://github.psa-cloud.com/lak00/tools-script/toolkit/cookiecutter-dbt.git
cookiecutter cookiecutter-dbt --no-input --config-file cookiecutter_init_example.yaml

### Following this, copy the generated files to the project's GitHub repository.

cd country/source  <br />
cp -r path/project_name/* .

### Then, add the needed environment variables
export SNOWFLAKE_ACCOUNT=  <br />
export AWS_REGION=  <br />
export KMS_KEY_ID=  <br />
export STORAGE_INTEGRATION=  <br />
export SNOWSQL_TRANSFORM_USER=  <br />
export SNOWSQL_TRANSFORM_PWD=  <br />
export ENVIRONMENT=  <br />

### Finally, Run the initialization scripts in the init_script/ directory from the root directory to set up your environment:

bash init_script/setup_project.sh
