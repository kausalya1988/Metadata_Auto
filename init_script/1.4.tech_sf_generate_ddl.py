import json
import yaml
import os
from pathlib import Path


def create_ddl(table_meta,  jv, project,source,filename_yaml, icr,config_data_format,table_meta_yaml):
    # Extract the relevant information for Snowflake
    db_name = f"DB_BNK_{jv.upper()}_" + '{' + '{' + "env_var('SHORT_ENV')" + '}' + '}'
    table_name = table_meta['Name']
    file_id = table_meta_yaml['file_id']
    if table_name.endswith("_OUT"):
        table_name = table_name[:-4]
    if table_name.endswith(f'_{jv.upper()}'):
        table_name = table_name[:len(f'_{jv.upper()}')]

    if file_id in config_data_format:
        date_format = config_data_format[file_id]['data_types']['date_format']
    else:
        date_format = config_data_format['data_types']['date_format']


    table_name = table_name.replace('_OUT','').replace(f'_{jv.upper()}','')
    table_name = table_name.replace('-', '_')
    period = table_name.lower().split("_")[-1]
    location_table_s3 = filename_yaml.replace('gdc_','').lower()
    location_table_s3 = location_table_s3.replace(f'_{period.lower()}','')
    tag = table_meta['Description']
    # Select the first element of the list to have the source name
    schema_name = f"SCH_{source.upper()}_SL"
    storage_integration = f"STI_S3_{jv.upper()}_" + '{' + '{' + "env_var('SHORT_ENV')" + '}' + '}'
    columns = table_meta['StorageDescriptor']['Columns']
    location = table_meta['StorageDescriptor']['Location']
    location = location.replace('-dev-', '-' + '{' + '{' + "env_var('ENVIRONMENT')" + '}' + '}' + '-')
    location = location.replace('-environment-', '-' + '{' + '{' + "env_var('ENVIRONMENT')" + '}' + '}' + '-')
    location = location.replace('-country-', f"-{jv.lower()}-")
    location_tmp = location.split('/')
    location_storage = '/'.join(location_tmp[:3]) + '/'
    partition_cols = ['DATE_BATCH_PARTITION']

    # Generate the column definitions
    column_defs = []
    for col in columns:
        col_name = col['Name']
        col_type = col['Type']
        if 'INTEGER' in col_type.upper() :
            col_type = 'INT'
        if 'DECIMAL' in col_type.upper() :
            col_type = 'NUMBER'
        elif 'VARCHAR' in col_type.upper() :
            col_type = 'VARCHAR'
        elif 'DATE' in col_type.upper() :
            col_type = 'DATE'
        elif 'TIMESTAMP' in col_type.upper() :
            col_type = 'TIMESTAMP_NTZ'

            
        if col_type == 'DATE' and 'date_batch' not in col_name.lower():
            column_defs.append(f"  {col_name.upper()} {col_type.upper()} as (to_date(NULLIF($1:{col_name.upper()}::TEXT,''),'{date_format}')), \n")
        if col_type == 'TIMESTAMP_NTZ':
            column_defs.append(f"  {col_name.upper()} {col_type.upper()} as (to_timestamp(NULLIF($1:{col_name.upper()}::TEXT,''),'{date_format} HH:MI:SS')), \n")
        if 'date_batch' not in col_name.lower() and 'filler_tech' not in col_name.lower() and col_type != 'DATE'  and col_type != 'TIMESTAMP_NTZ' :
            column_defs.append(f"  {col_name.upper()} {col_type.upper()} as ($1:{col_name.upper()}::{col_type.upper()}), \n")

    # Add the partitioning columns
    for col in partition_cols:
        column_defs.append(f"   {col} DATE as (TRY_CAST(split_part(split_part(metadata$filename, '/', {len(location.split('/')[:-1]) - 1}), '=', 2) AS DATE)),\n")

    # Add Technical columns
    column_defs.append(f"  ROW_NUMBER NUMBER as (metadata$file_row_number), \n")
    column_defs.append(f"  DATE_LAST_MODIFIED TIMESTAMP_NTZ as (metadata$file_last_modified)")
    # Make path if not exists
    if not os.path.exists(f"ddls-snowflake/{project.lower()}/tables"):
        os.makedirs(f"ddls-snowflake/{project.lower()}/tables")
    if not os.path.exists(f"ddls-snowflake/{project.lower()}/views"):
        os.makedirs(f"ddls-snowflake/{project.lower()}/views")
    if not os.path.exists(f"ddls-snowflake/{project.lower()}/stages"):
        os.makedirs(f"ddls-snowflake/{project.lower()}/stages")
        
    # Generate the CREATE EXTERNAL TABLE statement
    create_external_table_sql = f"""CREATE OR REPLACE EXTERNAL TABLE  {db_name}.{schema_name}.{table_name.upper()} (
    {' '.join(column_defs)}
    )
    PARTITION BY ({', '.join(f'{col}' for col in partition_cols)})
    LOCATION = @{db_name}.{schema_name}.STG_S3_{jv.upper()}_{source.upper()}/{project.lower()}/{period}/{location_table_s3.lower()}/
    FILE_FORMAT = (TYPE = 'PARQUET');
    COMMENT ON TABLE {db_name}.{schema_name}.{table_name.upper()} IS '{tag}';
    """

    # Write the SQL statement to a file
    print("create_external_table_sql")
    print(create_external_table_sql)
    with open(f"ddls-snowflake/{project.lower()}/tables/A__02_{schema_name}.{table_name.upper()}.sql", 'w') as f:
        f.write(create_external_table_sql)
        icr=icr+1
    
    # Generate the CREATE VIEW statement
    create_view_sql = f"""CREATE OR REPLACE VIEW  {db_name}.{schema_name}.VW_{table_name.upper()}	
    AS SELECT * EXCLUDE VALUE FROM {db_name}.{schema_name}.{table_name.upper()};
    """
    # Write the SQL statement to a file
    with open(f"ddls-snowflake/{project.lower()}/views/A__03_{schema_name}.VW_{table_name.upper()}.sql", 'w') as f:
        f.write(create_view_sql)
            
    # Generate the CREATE EXTERNAL STAGE statement
    create_external_stage_sql = f"""CREATE STAGE IF NOT EXISTS  {db_name}.{schema_name}.STG_S3_{jv.upper()}_{source.upper()} 	
    STORAGE_INTEGRATION = {storage_integration}
    URL = '{location_storage}' 
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = (TYPE = 'AWS_SSE_KMS' """ + 'KMS_KEY_ID = \'' +  '{' + "{env_var('KMS_KEY_ID')|lower}" + '}\'' + ");"
    
        
    # Write the SQL statement to a file
    with open(f"ddls-snowflake/{project.lower()}/stages/A__01_STG_S3_{jv.upper()}_{source.upper()}.sql", 'w') as f:
        f.write(create_external_stage_sql)

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
    config_tech = load_configuration(os.path.join(config_folder, 'config_tech.yaml'))
    if not config_tech:
        exit()
    for file_cfg in config_tech["yaml_cfg_files_path"]:
        config_file = os.path.join(config_folder, file_cfg)
        config = load_configuration(config_file)
        if not config:
            exit()
            
        json_path=config["json_path"]
        project = config["project"]
        source = config["source"]
        project_path = config["project_path"]

        #Catch format date/timestamp
        config_glue_folder = os.path.join(root_folder, 'glue/config')
        config_data_format = load_configuration(os.path.join(config_glue_folder, f'cfg_glue_{project_path}.yaml'))
        # Extract table information
        # Loop through all files in the output directory
        icr=2
        for filename in os.listdir(f"./{json_path}"):
            # Check if the file is a JSON file
            if filename.endswith("_OUT.json"):
                filename_yaml = filename.replace("_OUT.json",'')
                # Load the contents of the JSON file into a Python variable
                with open(f"./{json_path}/{filename}") as file:
                    table_meta = json.load(file)

                table_meta_yaml = load_configuration(os.path.join(f"{config_glue_folder}/{project}", f'{filename_yaml.replace("gdc_","").replace("_MONTHLY","").replace("_WEEKLY","").replace("_DAILY","")}_OUT.yaml')) 
      
                # Loop through all jvs
                list_jv =  config['jv']
                # Load the contents of the YAML file into a Python variable
                create_ddl(table_meta, list_jv,project,source,filename_yaml, icr,config_data_format,table_meta_yaml)
                icr=icr+2

if __name__ == "__main__":
    main()
