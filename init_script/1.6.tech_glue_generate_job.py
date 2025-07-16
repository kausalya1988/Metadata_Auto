import json
import yaml
import os
from pathlib import Path


def create_json_file(table_input, json_path, job_name):
    """Save the table_input in a JSON file for versioning."""
    json_file_path = os.path.join(json_path, f"{job_name}.json")
    txt_file_path = os.path.join(json_path, 'json_import_glue_job.txt')
    try:
        with open(json_file_path, "w") as file:
            json.dump(table_input, file)
        print(f"Glue job configuration for {job_name} saved to {json_file_path}.")
        # Ajouter le nom du fichier JSON dans le fichier texte (fichier_liste)
        with open (txt_file_path,'a') as fichier_txt: 
        # 'a' pour append (ajouter à la fin)
                fichier_txt.write(f"{job_name}.json" +'\n')
    except Exception as e:
        print(f"Error saving JSON file for {job_name}: {e}")


def create_resource(config, python_script, job_name):
    project = config["project"]
    source = config["source"]

    resource_descriptor = {
        "resource": {
            "aws_glue_job": {
            "my_glue_job": {
                "name": job_name,
                "role_arn": "iam_role_arn",
                "command": {
                "name": "glueetl",
                "script_location": f"s3://s3b-dlz-environment-src-core-el/scripts/{python_script}.py",
                "python_version": "3"
                },
                "default_arguments": {
                "--job-language": "python",
                "--notification": "",
                "--additional-python-modules": "pandas,faker"
                },
                "max_retries": 0,
                "timeout": 2880,
                "number_of_workers": 5,
                "worker_type": "G.1X",
                "glue_version": "4.0",
                "max_concurrent_runs": 1,
                "tags": {                
                "source": source
                }
            }
            }
        }
        }
    return resource_descriptor

def create_glue_job(config):
    project = config["project"]
    project_path = config["project_path"]
    source = config["source"]
    layout= config["multi_layout"]
    jv = config["jv"]
    glue_job_path = config["glue_job_path"]

    # Create an json_import_glue_job.txt
    with open(os.path.join(config["json_path"], 'json_import_glue_job.txt'), 'w') as file:
        pass

    # step 1 : check structure
    python_script='int-s3-structure-check'
    job_name= f"{project.lower()}-{project_path.split('-')[0]}-{jv.lower()}-s3sc"
    if layout.lower() == 'yes':
            python_script='int-s3-structure-check-layout'
            job_name= f"{project.lower()}-{project_path.split('-')[0]}-{jv.lower()}-s3scl"

    resource_descriptor=create_resource(config, python_script, job_name)
    create_json_file(resource_descriptor, glue_job_path, job_name)

    # step 2 : csv to parquet
    python_script='int-s3-csv-to-parquet'
    job_name= f"{project.lower()}-{project_path.split('-')[0]}-{jv.lower()}-ctop"


    resource_descriptor=create_resource(config, python_script, job_name)
    create_json_file(resource_descriptor, glue_job_path, job_name)

def load_configuration(config_path):
    """Load configuration from the YAML file."""
    try:
        with open(config_path, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Configuration file {config_path} not found.")
        return None
    except yaml.YAMLError as e:
        print(f"Error loading configuration file: {e}")
        return None

def main():
    root_folder = os.path.abspath('.')  # Adjust the path as neede
    config_folder = os.path.join(root_folder, 'config')

    # Load configuration from YAML file
    config_tech = load_configuration(os.path.join(config_folder, 'config_tech.yaml'))
    if not config_tech:
        exit()
    for file_cfg in config_tech["yaml_cfg_files_path"]:
        config_file = os.path.join(config_folder, file_cfg)
        config = load_configuration(config_file)
        if not config:
            exit()

        # generate glue job json
        create_glue_job(config)

if __name__ == "__main__":
    main()
