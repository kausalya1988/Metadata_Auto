import os
import yaml
import json

def load_yaml(file_path):
    """Load a YAML file."""
    with open(file_path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(f"Error loading {file_path}: {exc}")
            return None

def create_dbt_source_table(yaml_data, table_name,project_path):
    """Create a dbt source table schema from YAML data."""
    columns = yaml_data.get('columns', [])
    project = project_path.split('-')[0].upper()
    nullable_columns = [col['name'] for col in columns if col['Mandatory'].lower() == 'non' and col['PrimaryKey'] == 'PK']
    not_nullable_columns = [col['name'] for col in columns if col['Mandatory'].lower() == 'oui' and col['PrimaryKey'] == 'PK']
    
    dbt_table = {
        'name': table_name,
        'description': f"This table contains data for {table_name}.",
        'columns': [],
        'tests': []
    }
    
    for col in columns:
        column_data = {
            'name': col['name'],
            'description': col.get('description', ''),
            'tests': []
        }
        not_null = col.get('Mandatory', [])
        
        lov = col.get('LOV', [])
        if lov:
            # Flatten the list if it's nested and ensure it is a single list
            flattened_lov = [item for sublist in lov for item in sublist] if isinstance(lov[0], list) else lov
            # Remove 'null' from the list
            flattened_lov = [item for item in flattened_lov if item != 'null' and item != 'Null' and item != 'NULL']

            if not_null.lower() == 'non':
                # Add a test for LOV values
                column_data['tests'].append({
                    'dbt_expectations.expect_column_values_to_be_in_set': {
                        'value_set': flattened_lov,
                        'config':{
                            'where': f"{col['name']} is not null and {col['name']} <> ''"
                        }
                    }
                })
            else:
                # Add a test for LOV values
                column_data['tests'].append({
                    'dbt_expectations.expect_column_values_to_be_in_set': {
                        'value_set': flattened_lov
                    }
                })

        if not_null.lower() == 'oui':
            # Add a test for not null values
            column_data['tests'].append({
                'dbt_expectations.expect_column_values_to_not_be_null': {
                    'row_condition': f"{col['name']} is not null"
                }
            })
        

        dbt_table['columns'].append(column_data)
        
    if not_nullable_columns:
        # Add a test for not PK
        dbt_table['tests'].append({
            'unique_combination': {
                'columns': not_nullable_columns,
                'nullable_columns': nullable_columns,
                'meta':{
                    'description': 'This test check the primary key unicity',
                    'tags': ['data_quality', 'primary_key']
                }
            }
        })
            
    
    return dbt_table

def write_dbt_source_file(output_directory, source_group, database, schema, tables):
    """Write the dbt source schema to a single YAML file."""
    dbt_source = {
        'version': 2,
        'sources': [
            {
                'name': source_group,
                'database': database,
                'schema': schema,
                
                'freshness':{
                    'warn_after':{
                        'count': 1, 
                        'period': 'day'
                    },
                    'error_after':{
                        'count': 2,
                        'period': 'day'
                    }
                },
                'loaded_at_field': 'DATE_BATCH_PARTITION::timestamp',
                'tables': tables
            }
        ]
    }
    
    output_path = os.path.join(output_directory, "source.yml")
    with open(output_path, 'w') as outfile:
        yaml.dump(dbt_source, outfile, default_flow_style=False, sort_keys=False)
    print(f"Generated {output_path}")

def write_dbt_model_refresh_external_tables(output_directory, tables,config):
    jv = config["jv"]
    project_path = config["project_path"]

    output = '{' + '%' + f" macro refresh_external_tables_model_{project_path.replace('-', '_')}() " + '%' + '}\n\n'
    output += '{' + '%' + ' set table_list = ' + str(tables) + ' %' + '}\n\n'
    output += "{" + "{ refresh_external_tables(table_list) }}\n\n"
    output += '{' + '%' + ' endmacro ' + '%' + '}'

    output_path = os.path.join(output_directory, f"refresh_external_tables_model_{project_path.replace('-', '_')}.sql")

    with open(output_path, "w") as file:
        file.write(output)
        
    print(f"Generated {output_path}")

def read_file(file_path):
    """Load file."""
    try:
        with open(file_path, "r") as file:
            return file.read()
    except FileNotFoundError:
        print(f"file {file_path} not found.")
        return None
    
def write_file(file_path,file_data):
    """Load file."""
    try:
        with open(file_path, "w") as file:
            file.write(file_data)
    except FileNotFoundError:
        print(f"file {file_path} not found.")
        return None

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
    # Directory containing all project folders
    root_folder = os.path.abspath('.')  # Adjust the path as neede
    config_directory = os.path.join(root_folder,'config')
    config_tech = load_configuration(os.path.join(config_directory, 'config_tech.yaml'))
    dbt_directory = os.path.join(root_folder, 'dbt')
    output_directory = os.path.join(os.path.join(root_folder, 'dbt'), 'models')
    macros_directory = os.path.join(os.path.join(root_folder, 'dbt'), 'macros')
    os.makedirs(output_directory, exist_ok=True)

    # dbt_project.yml
    dbt_project_file = read_file(os.path.join(dbt_directory, 'dbt_project.yml'))
    dbt_project_file = dbt_project_file.replace('VAR_DATE_BATCH_PARTITION','\'{{ var("date_batch_partition") }}\'')
    write_file(os.path.join(dbt_directory, 'dbt_project.yml'), dbt_project_file)


    if not config_tech:
        exit()
    for file_cfg in config_tech["yaml_cfg_files_path"]:
        config_file = os.path.join(config_directory, file_cfg)
        config = load_configuration(config_file)
        if not config:
            exit()

        jv = config["jv"]
        project_path = config["project_path"]
        json_path=config["json_path"]
        yaml_path=config["yaml_path"]
            
        # DBT Source parameters
        source_group = project_path
        database = f"DB_BNK_{jv.upper()}_" + '{' + '{' + "env_var('ENV_DBT')" + '}' + '}'
        schema = f"SCH_{source_group.split('-')[0].upper()}_SL"
        source_group = project_path.replace('-','_')


        # List to accumulate all table definitions
        all_tables = []
        tables_names = []           


        # Extract table information
        # Loop through all files in the output directory
        for filename in os.listdir(f"./{json_path}"):
            # Check if the file is a JSON file
            if filename.endswith("_OUT.json"):
                filename_yaml = filename.replace("_OUT.json",'')
                # Load the contents of the JSON file into a Python variable
                with open(f"./{json_path}/{filename}") as file:
                    table_meta = json.load(file)
                
                # Load the contents of the YAML file into a Python variable
                table_name = table_meta['Name']
                yaml_table = table_name.split(f"_{jv.upper()}")[0].upper() + f"_OUT.yaml"
                if table_name.endswith("_OUT"):
                    table_name = table_name[:-4]
                if table_name.endswith(f'_{jv.upper()}'):
                    table_name = table_name[:len(f'_{jv.upper()}')]
                if f'_{jv.upper()}_OUT' in table_name:
                    table_name = table_name.replace(f'_{jv.upper()}_OUT', '')
                table_name = table_name.replace('-', '_')
                tables_names.append(schema + '.' + table_name)

                #find yaml tests
                file_path = os.path.join(yaml_path, yaml_table)
                yaml_data = load_yaml(file_path)
                dbt_table = create_dbt_source_table(yaml_data, table_name,project_path)
                all_tables.append(dbt_table)

        write_dbt_model_refresh_external_tables(macros_directory, tables_names,config)

    # Write all tables to a single dbt source YAML file
    write_dbt_source_file(output_directory, source_group, database, schema, all_tables)

if __name__ == "__main__":
    main()
