import yaml
import json
import boto3
import os
from botocore.exceptions import ClientError

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

def create_json_file(table_input, json_path, table_name):
    """Save the table_input in a JSON file for versioning."""
    json_file_path = os.path.join(json_path, f"gdc_{table_name}.json")
    txt_file_path = os.path.join(json_path, 'json_import_gdc.txt')
    try:
        with open(json_file_path, "w") as file:
            json.dump(table_input, file)
        print(f"Table configuration for {table_name} saved to {json_file_path}.")

        # Ajouter le nom du fichier JSON dans le fichier texte (fichier_liste)
        with open (txt_file_path,'a') as fichier_txt: 
        # 'a' pour append (ajouter à la fin)
                fichier_txt.write(f"gdc_{table_name}.json" +'\n')
    except Exception as e:
        print(f"Error saving JSON file for {table_name}: {e}")

def create_glue_table(table_name, columns, partition_keys, database_name, glue_client, file_format, location, separator, json_path,description):
    """Creates a Glue table and saves the table input configuration as JSON for versioning."""
    
    # Define the storage descriptor and serde info based on file format
    if file_format == "parquet":
        input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
        serialized_serde_info = {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"}
        }
    else:
        input_format = "org.apache.hadoop.mapred.TextInputFormat"
        output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        serialized_serde_info = {
            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "Parameters": {"field.delim": separator, "serialization.format": separator},
        }
    storage_descriptor = {
        "Columns": columns,
        "Location": location,
        "InputFormat": input_format,
        "OutputFormat": output_format,
        "SerdeInfo": serialized_serde_info,
    }

    parameters = {
        "EXTERNAL": "TRUE",
        "classification": file_format,
        "encoding": "UTF-8",
        "typeOfData": "file"
    }

    table_input = {
        "Name": table_name,
        "Description": description,
        "StorageDescriptor": storage_descriptor,
        "TableType": 'EXTERNAL_TABLE',
        "Parameters": parameters
    }

    # Save the table_input in a JSON file for versioning
    # create_json_file(table_input, json_path, table_name)
    
    table_exists = False
    # Try to get the table in Glue
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        table_exists = True
        print(f"Table {table_name} found in the database {database_name}.")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Table {table_name} does not exist in the database {database_name}.")
    except Exception as e:
        print(f"Error retrieving table {table_name}: {e}")
    
    # Try to update the table in Glue
    if table_exists:
        try:
            response = glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
            print(f"Table {table_name} updated successfully.")
        except Exception as e:
            print(f"Error updating table {table_name}: {e}")

    # Try to create the table in Glue
    else:
        try:
            response = glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")

def create_database_if_not_exists(glue_client, database_name, database_description):
    """Creates a Glue database if it does not exist."""
    try:
        # Check if the database already exists
        response = glue_client.get_database(Name=database_name)
        print(f"The database '{database_name}' already exists.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            # The database does not exist, so create it
            try:
                response = glue_client.create_database(
                    DatabaseInput={
                        'Name': database_name,
                        'Description': database_description
                    }
                )
                print(f"The database '{database_name}' has been created successfully.")
            except ClientError as create_error:
                print(f"Error occurred while creating the database: {create_error}")
        else:
            print(f"Error occurred while checking the database: {e}")

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
        
        project = config["project"]
        source = config["source"]
        jv = config["jv"]
        yaml_path = os.path.join(root_folder, config["yaml_path"]) 
        json_path = os.path.join(root_folder, config["json_path"]) 

        # Make path if not exists
        if not os.path.exists(json_path):
            os.makedirs(json_path)

        # Create an json_import_gdc.txt
        with open(os.path.join(json_path, 'json_import_gdc.txt'), 'w') as file:
            pass
            
        # Extract table information
        for filename in os.listdir(yaml_path):
            if filename.endswith(".yaml") and filename.startswith("STG"):
                # Load the contents of the YAML file into a Python variable
                with open(os.path.join(yaml_path, filename), "r") as file:
                    table_structure = yaml.safe_load(file)

                table_name_input = table_structure["table_name_output"]
                table_name_s3=table_structure["table_name"]
                periodicity=table_structure["periodicity"]
                file_code = table_structure["file_code"]
                columns = table_structure["columns"]
                data_type = table_structure["data_type"]
                separator = table_structure["separator"]
                header = table_structure["header"]
                footer = table_structure["footer"]
                quote = table_structure["quote"]
                input_location = "s3://s3b-dlz-environment" + f"-landing-{jv.lower()}-{source.lower()}/{project.lower()}/"
                parquet_location = "s3://s3b-dlz-environment" + f"-standard-{jv.lower()}-{source.lower()}/{project.lower()}/period/{table_name_s3.lower()}"

                # Prepare columns for Glue
                glue_columns = [{"Name": col["name"].lower(), "Type": col["type"], "Parameters": { "Protected": col["Protected"], "AnonymizationRule": col["AnonymizationRule"],"PrimaryKey": 'None' if col["PrimaryKey"] is None else col["PrimaryKey"],"Mandatory": 'None' if col["Mandatory"] is None else col["Mandatory"]}} for col in columns]
                
                # Prepare partition keys
                # glue_partition_keys = [{"Name": key["name"], "Type": key["type"]} for key in config["partition_keys"]]

                for period in periodicity:
                    # Create JSON configuration files before attempting Glue operations
                    if filename.lower().endswith("_out.yaml"):
                        # Create JSON configuration for partitioned table
                        create_json_file(
                            {
                                "Name": f'{table_name_s3}_{jv.upper()}_OUT_{period.upper()}',
                                "Description": f'{table_name_s3}_{jv.upper()}_OUT_{period.upper()}',
                                "StorageDescriptor": {
                                    "Columns": glue_columns,
                                    "Location": parquet_location,
                                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                                    "SerdeInfo": {
                                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                        "Parameters": {"serialization.format": "1"}
                                    }
                                },
                                "Parameters": {
                                    "classification": "parquet",
                                    "encoding": "UTF-8",
                                    "typeOfData": "file"
                                }
                            },
                            json_path,
                            f'{table_name_s3}_{period.upper()}_OUT'
                        )
                    else:
                        # Create JSON configuration for CSV table
                        create_json_file(
                            {
                                "Name": f'{table_name_s3}_{jv.upper()}_IN_{period.upper()}',
                                "Description": file_code,
                                "StorageDescriptor": {
                                    "Columns": glue_columns,
                                    "Location": input_location,
                                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                                    "SerdeInfo": {
                                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                                        "Parameters": {"field.delim": separator, "serialization.format": separator}
                                    }
                                },
                                "PartitionKeys": [],
                                "Parameters": {
                                    "classification": data_type,
                                    "encoding": "UTF-8",
                                    "typeOfData": "file",
                                    "header": header,
                                    "footer": footer,
                                    "quote": quote
                                }
                            },
                            json_path,
                            f'{table_name_s3}_{period.upper()}_IN'
                        )

if __name__ == "__main__":
    main()
