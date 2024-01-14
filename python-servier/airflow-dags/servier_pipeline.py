from airflow import DAG
from datetime import datetime

from custom_operator import (
    CsvCleaningOperator,
    FileMergeOperator,
    GenerateDrugGraphOperator
)


with DAG(
    dag_id="servier_pipeline",
    start_date=datetime.utcnow(),
    schedule_interval=None
) as dag:

    clinical_trials_cleaning = CsvCleaningOperator(
        task_id="clinical_trials_cleaning",
        source_file="clinical_trials.csv",
        destination_file="DP_clinical_trials.csv",
        bucket_name="airflow",
        aws_conn_id="myminio",
        column_groups = {
            'text_columns': ['scientific_title', 'journal'],
            'date_columns': ['date'],
            'obligatory_columns': ['scientific_title', "journal"],
            'rename': {"scientific_title": "title"},
        }
    )

    pubmed_merging = FileMergeOperator(
        task_id="pubmed_merging",
        file_mapping={
            'pubmed.csv': "csv",
            'pubmed.json': "json"},
        destination_file="Merge_pubmed.csv",
        bucket_name="airflow",
        aws_conn_id="myminio",
    )

    pubmed_cleaning = CsvCleaningOperator(
        task_id="pubmed_cleaning",
        source_file="Merge_pubmed.csv",
        destination_file="DP_pubmed.csv",
        bucket_name="airflow",
        aws_conn_id="myminio",
        column_groups = {
            'text_columns': ['title', 'journal'],
            'date_columns': ['date'],
            'obligatory_columns': ['title', "journal"],
            'rename': {"": ""}
        }
    )

    generate_drug_graph = GenerateDrugGraphOperator(
        task_id='generate_drug_graph',
        bucket_name="airflow",
        file_keys={
            'drug': 'drugs.csv',
            'clinical_trials': 'DP_clinical_trials.csv',
            'pubmed': 'DP_pubmed.csv',
        },
        destination_key='drug_graph.json',
        aws_conn_id='myminio',
        dag=dag,
    )

    clinical_trials_cleaning >> pubmed_merging >> pubmed_cleaning  >> generate_drug_graph

