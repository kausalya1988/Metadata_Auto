import os
import yaml


def generate_yaml_for_excel_files(root_folder):
    # Path to the datacontract folder
    data_contract_folder = os.path.join(root_folder, 'datacontract')
    
    if not os.path.isdir(data_contract_folder):
        raise FileNotFoundError(f"Data contract folder {data_contract_folder} does not exist.")
    
    # Extract `jv` and `environment` from the root folder name
    #jv, source = '{{cookiecutter.jv}}', '{{cookiecutter.source.split('-')[0]}}'
    
    jv = 'INDIA'#{{ cookiecutter.jv }}'
    source = 'EKIP'#'{{ cookiecutter.source.split("-")[0] }}'


    # Create the config directory if it does not exist
    config_folder = os.path.join(root_folder, 'config')
    os.makedirs(config_folder, exist_ok=True)

    # List to hold names of generated YAML files
    yaml_file_names = []

    # Iterate over each file in the datacontract folder
    for file_name in os.listdir(data_contract_folder):
        if file_name.endswith('.xlsm'):
            # Extract relevant information from the file name
            base_name = os.path.splitext(file_name)[0]  # Extract 'source_project' from 'source_project.xlsm'
            parts = base_name.split('_')
            if len(parts) < 2:
                continue
            
            print(base_name)
            project = base_name.split('_', 1)[1].lower().replace('_', '-')
            print('project')
            print(project)
            project_path = base_name.lower().replace('_', '-')

            # Generate the YAML file name and path
            yaml_file_name = f"config_{source.upper()}_{project.upper()}.yaml"
            yaml_file_path = os.path.join(config_folder, yaml_file_name)
            
            # Define the YAML content
            yaml_content = {
                'excel_file_path': f'datacontract/{file_name}',
                'source': source,
                'jv': jv,
                'project': project,
                'project_path': project_path,
                'yaml_path': f'glue/config/{project.lower()}',  # Assumes this is the source; adjust as needed
                'json_path': f'glue/ddl/{project.lower()}',
                'ddl_path': f'ddls-snowflake/{project.lower()}',
                'multi_layout': '{{cookiecutter.multi_layout}}',
                'glue_job_path': f'glue/ddl/{project.lower()}',
            }
            
            # Write YAML content to the file
            with open(yaml_file_path, 'w') as yaml_file:
                yaml.dump(yaml_content, yaml_file, default_flow_style=False)

            print(f"Generated YAML file: {yaml_file_path}")

            # Add the generated YAML file name to the list
            yaml_file_names.append(yaml_file_name)

    # Generate the config_tech.yaml file listing all generated YAML files
    tech_yaml_file_path = os.path.join(config_folder, 'config_tech.yaml')
    tech_yaml_content = {
        'yaml_cfg_files_path': yaml_file_names
    }

    with open(tech_yaml_file_path, 'w') as tech_yaml_file:
        yaml.dump(tech_yaml_content, tech_yaml_file, default_flow_style=False)

    print(f"Generated tech YAML file: {tech_yaml_file_path}")

if __name__ == "__main__":
    root_folder = os.path.abspath('.')  # Adjust the path as neede
    print(root_folder)
    generate_yaml_for_excel_files(root_folder)
