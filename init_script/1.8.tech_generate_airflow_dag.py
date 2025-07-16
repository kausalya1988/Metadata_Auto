import yaml
from ruamel.yaml import YAML
from string import Template
import os
import pandas as pd

def create_airflow_dag(project, layout, source, jv, period):
    
    with open("init_script/airflow-template.py", "r") as template_file:
        template_content = template_file.read()
    # Create a Template object
    template = Template(template_content)
    output_text = template.substitute({"project": project.lower(), "layout": layout.lower(), "source_split": source.lower().split('-')[0], "source_file": source.lower(), "source": source.lower().replace('-', '_'), "jv": jv.lower(), "period": period})

    file_path = f"dags/{jv.lower()}_el_{source.lower().replace('-', '_')}_{period.lower()}.py"
    with open(file_path, "w") as output_file:
        output_file.write(output_text)

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

def extract_periodicities(file_path):
    # Initialiser un ensemble pour stocker les périodicités distinctes
    periodicities = set()
 
    # Liste des périodicités connues
    known_periodicities = {'weekly', 'daily', 'monthly', 'yearly', 'quarterly'}
 
    # Ouvrir le fichier en mode lecture
    with open(file_path, 'r') as file:
        # Lire chaque ligne du fichier
        for line in file:
            # Diviser la ligne en mots
            words = line.split('_')
            # Parcourir les mots pour trouver les périodicités
            for word in words:
                if word.lower() in known_periodicities:
                    periodicities.add(word.lower())
 
    # Convertir l'ensemble en liste et retourner
    return list(periodicities)

if __name__ == "__main__":
    root_folder = os.path.abspath('.')  # Adjust the path as neede
    config_folder = os.path.join(root_folder, 'config')
    config_tech = load_configuration(os.path.join(config_folder, 'config_tech.yaml'))

    if not config_tech:
        exit()
    for file_cfg in config_tech["yaml_cfg_files_path"]:
        config_file = os.path.join(config_folder, file_cfg)
        config = load_configuration(config_file)
        if not config:
            exit()

        multi_layout = config["multi_layout"]
        project_path = config["project_path"]
        project = config["project"]
        jv = config["jv"]
        layout = ''
        #folder for periodicity
        glue_folder = os.path.join(root_folder, f'glue/ddl/{project}')
        list_perdiod = extract_periodicities(os.path.join(glue_folder, 'json_import_gdc.txt'))

        if multi_layout.lower() == 'yes':
            layout = 'l'
        for period in list_perdiod:
            create_airflow_dag(project.lower(), layout, project_path.lower(), jv,period)
