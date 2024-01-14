from typing import Any, Dict
import json
import pandas as pd
from io import StringIO
from typing import Optional
import logging

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.decorators import apply_defaults


from script.helper import (
    clean_text_column,
    clean_date,
    create_hashed_uuid_for_nan,
    create_drug_graph
)

def write_dict_to_bucket_json(
    s3_hook: S3Hook,
    key: str,
    bucket_name: str,
    data_dict: dict,
    overwrite: Optional[bool] = False
) -> None:
    """
    Write a dictionary to an S3 bucket in JSON format.

    Parameters:
    - s3_hook (S3Hook): An instance of the S3Hook.
    - key (str): The key (path) for the S3 object.
    - bucket_name (str): The name of the S3 bucket.
    - data_dict (dict): The dictionary to be written to S3.
    - overwrite (bool): If True, overwrite the file if it already exists. Default is False.
    """
    # Check if the file already exists
    file_exists = s3_hook.check_for_key(key=key, bucket_name=bucket_name)

    if file_exists and not overwrite:
        logging.warning(f"The file '{key}' already exists in the S3 bucket '{bucket_name}'. Set 'overwrite' to True to overwrite.")
        return

    # Convert the dictionary to a JSON string
    json_content = json.dumps(data_dict, indent=2, ensure_ascii=False)

    # Upload the JSON content to S3
    s3_hook.load_string(string_data=json_content, key=key, bucket_name=bucket_name)

    logging.info(f"Dictionary successfully written to S3 bucket '{bucket_name}' with key '{key}' in JSON format")

def write_dataframe_to_bucket(
    s3_hook: S3Hook,
    key: str,
    bucket_name: str,
    dataframe: pd.DataFrame,
    overwrite: Optional[bool] = False
) -> None:
    """
    Write a Pandas DataFrame to an S3 bucket.

    Parameters:
    - s3_hook (S3Hook): An instance of the S3Hook.
    - key (str): The key (path) for the S3 object.
    - bucket_name (str): The name of the S3 bucket.
    - dataframe (pd.DataFrame): The Pandas DataFrame to be written to S3.
    - overwrite (bool): If True, overwrite the file if it already exists. Default is False.
    """
    # Check if the file already exists
    file_exists = s3_hook.check_for_key(key=key, bucket_name=bucket_name)

    if file_exists and not overwrite:
        logging.warning(f"The file '{key}' already exists in the S3 bucket '{bucket_name}'. Set 'overwrite' to True to overwrite.")
        return

    # Convert the DataFrame to a CSV string
    csv_content = dataframe.to_csv(index=False)

    # Use StringIO to create a file-like object from the string
    csv_file = StringIO(csv_content)

    # Upload the CSV content to S3
    s3_hook.load_string(string_data=csv_file.getvalue(), key=key, bucket_name=bucket_name)

    logging.info(f"DataFrame successfully written to S3 bucket '{bucket_name}' with key '{key}'")


def read_from_bucket_and_load_to_dataframe(s3_hook: S3Hook, key: str, bucket_name: str, file_format: str = 'csv'):
    try:
        # Get the object from S3
        obj = s3_hook.get_key(key=key, bucket_name=bucket_name)

        # Read the content of the object and decode it
        content = obj.get()['Body'].read().decode('utf-8')

        # Use StringIO to create a file-like object from the string
        if file_format.lower() == 'json':
            # Load JSON data into a Pandas DataFrame
            data = json.loads(content)
            df = pd.json_normalize(data)  # Adjust this line based on your JSON structure
        elif file_format.lower() == 'csv':
            data_file = StringIO(content)
            # Use pd.read_csv to load the data into a Pandas DataFrame
            df = pd.read_csv(data_file)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        return df

    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON file '{key}': {str(e)} (Line {e.lineno}, Column {e.colno})")
        return pd.DataFrame()  # Return an empty DataFrame or handle the error as needed

    except Exception as e:
        logging.error(f"Error reading file '{key}': {str(e)}")
        return pd.DataFrame()  # Return an empty DataFrame or handle the error as needed
 

class CsvCleaningOperator(BaseOperator):
    def __init__(
        self,
        source_file: str,
        destination_file: str,
        bucket_name: str,
        aws_conn_id: str,
        column_groups: dict,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.source_file = source_file
        self.destination_file = destination_file
        self.bucket_name = bucket_name
        self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        self.column_groups = column_groups

    def execute(self, context: dict) -> Any:
        data = read_from_bucket_and_load_to_dataframe(
            self.s3_hook,
            self.source_file,
            self.bucket_name,
            "csv"
        )

        # Apply transformations to text columns
        for column in self.column_groups['text_columns']:
            data[column] = clean_text_column(data[column])

        # Apply transformations to date columns
        for column in self.column_groups['date_columns']:
            data[column] = clean_date(data[column])

        # Remove line with not usable information
        for column in self.column_groups['obligatory_columns']:
            data = data.drop(data[data[column] == ""].index)

        data = create_hashed_uuid_for_nan(data, "id")
        
        if self.column_groups['rename']:
            data.rename(columns=self.column_groups['rename'], inplace = True)

        write_dataframe_to_bucket(
            self.s3_hook,
            self.destination_file,
            self.bucket_name,
            data
            )
        
class FileMergeOperator(BaseOperator):
    """
    Operator for merging files with the same column names.
    """

    @apply_defaults
    def __init__(
        self,
        file_mapping: Dict[str, str],  # Dictionary mapping file names to file types
        destination_file: str,
        bucket_name: str,
        aws_conn_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.file_mapping = file_mapping
        self.destination_file = destination_file
        self.bucket_name = bucket_name
        self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    def execute(self, context: dict) -> Any:
        # Initialize an empty dataframe to store merged data
        merged_data = pd.DataFrame()

        # Read and merge data from each file based on its type
        for file_name, file_type in self.file_mapping.items():
            try:
                # Read data from the source file
                file_data = read_from_bucket_and_load_to_dataframe(
                    self.s3_hook,
                    file_name,
                    self.bucket_name,
                    file_type
                )
                # Merge dataframes without specifying columns
                merged_data = pd.concat([merged_data, file_data], axis=0, ignore_index=True)
            except Exception as e:
                self.log.error(f"Error reading file '{file_name}': {str(e)}")

        # Write the merged data to the destination file
        try:
            write_dataframe_to_bucket(
                self.s3_hook,
                self.destination_file,
                self.bucket_name,
                merged_data
            )
            self.log.info(f"Merged data written to '{self.destination_file}' in '{self.bucket_name}'")

        except Exception as e:
            self.log.error(f"Error writing merged data: {str(e)}")

class GenerateDrugGraphOperator(BaseOperator):
    """
    Operator for generating drug graph based on related data.
    """

    @apply_defaults
    def __init__(
        self,
        bucket_name: str,
        file_keys: Dict[str, str],
        destination_key: str,
        aws_conn_id: str,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.file_keys = file_keys
        self.destination_key = destination_key
        self.aws_conn_id = aws_conn_id
        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)



    def execute(self, context: Context) -> None:
        try:
            # Read data from S3
            drug_data = read_from_bucket_and_load_to_dataframe(
                self.s3_hook,
                self.file_keys['drug'],
                self.bucket_name,
                "csv"
            )
            logging.info(f"DataFrame drug_data successfully loaded")

            clinical_trials_data = read_from_bucket_and_load_to_dataframe(
                self.s3_hook,
                self.file_keys['clinical_trials'],
                self.bucket_name,
                "csv"
            )
            logging.info(f"DataFrame clinical_trials_data successfully loaded")
            pubmed_data = read_from_bucket_and_load_to_dataframe(
                self.s3_hook,
                self.file_keys['pubmed'],
                self.bucket_name,
                "csv"
            )
            logging.info(f"DataFrame pubmed_data successfully loaded")
            # Ensure date columns are treated as strings
            clinical_trials_data["date"] = clinical_trials_data["date"].astype("string")
            pubmed_data["date"] = pubmed_data["date"].astype("string")

            # Generate drug graph
            drug_graph = create_drug_graph(drug_data, pubmed_data, clinical_trials_data)
            
            logging.info(f"lgtm")
            # Write the result to S3
            write_dict_to_bucket_json(
                self.s3_hook, 
                self.destination_key, 
                self.bucket_name, 
                drug_graph, 
                overwrite=True
            )

        except Exception as e:
            error_message = f"Error executing GenerateDrugGraphOperator: {str(e)}"
            self.log.error(error_message)