import re
from typing import List, Dict
from collections import defaultdict
import pandas as pd
from dateutil.parser import parse
import hashlib
import uuid

def clean_non_utf8_characters(string: str) -> str:
    """
    I found multiple non UTF8 characters.
    I tried to develop a function that detect multiencoding and decode all of them in utf8.
    The task was to hard so i stick with a deletion of this character.
    we can also keep them as so and decode same in a secon time.
    """
    cleaned_string = re.sub(r"(\\\\x|\\x)[0-9a-fA-F]{2} ?", "", string)
    return cleaned_string

def fuzzy_string_to_iso8601(string: str) -> str:
    return parse(string, fuzzy=True).date().isoformat()

def clean_text_column(s: pd.Series) -> pd.Series:
    """
    Clean a text column by stripping, removing non-UTF-8 characters, and converting to string.
    """
    return s.fillna(value="").str.strip().apply(clean_non_utf8_characters).astype("string")

def clean_date(s: pd.Series) -> pd.Series:
    return s.astype("string").apply(fuzzy_string_to_iso8601).astype("string")


def create_hashed_uuid_for_nan(df, id_column):
    """
    Create a hashed UUID for every NaN value in the specified column of a Pandas DataFrame
    based on the values in all other columns.

    Parameters:
    - df (pd.DataFrame): Input DataFrame.
    - id_column (str): Name of the column where hashed UUIDs will be created.

    Returns:
    - pd.DataFrame: DataFrame with hashed UUIDs for NaN values in the specified column.
    """
    # Copy the original DataFrame to avoid modifying the input DataFrame
    result_df = df.copy()

    # Create a mask for NaN values in the specified column
    nan_mask = result_df[id_column].isna()

    # Filter only NaN values that don't have an existing ID
    nan_without_existing_id = nan_mask & result_df[id_column].isna()

    # Generate hashed UUIDs for NaN values based on values in all other columns
    hash_values = result_df.loc[nan_without_existing_id, result_df.columns.difference([id_column])].apply(
        lambda row: hashlib.md5(','.join(map(str, row)).encode('utf-8')).hexdigest(), axis=1
    )

    # Assign hashed UUIDs to NaN values in the specified column
    result_df.loc[nan_without_existing_id, id_column] = hash_values.apply(lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, x)))

    return result_df

def create_drug_graph(
    drugs_df: pd.DataFrame, pubmed_df: pd.DataFrame, clinical_trials_df: pd.DataFrame
) -> Dict[str, Dict[str, List[Dict[str, str]]]]:
    drug_graph: Dict[str, Dict[str, List[Dict[str, str]]]] = defaultdict(
        lambda: {"pubmed": [], "clinical_trials": []}
    )

    for _, drug_row in drugs_df.iterrows():
        drug_atccode: str = drug_row["atccode"]
        drug_name: str = drug_row["drug"]

        def extract_related_data(related_df: pd.DataFrame, category: str) -> List[Dict[str, str]]:
            return [
                {
                    "id": str(row["id"]),
                    "title": row["title"],
                    "date": row["date"],
                    "journal": row["journal"],
                }
                for _, row in related_df.iterrows()
            ]

        # Filter NaN values before applying the mask
        related_pubmed: pd.DataFrame = pubmed_df[
            pubmed_df["title"].str.contains(drug_name, case=False) & ~pubmed_df["title"].isna()
        ]
        drug_graph[drug_atccode]["pubmed"] = extract_related_data(related_pubmed, "pubmed")

        related_trials: pd.DataFrame = clinical_trials_df[
            clinical_trials_df["title"].str.contains(drug_name, case=False) & ~clinical_trials_df["title"].isna()
        ]
        drug_graph[drug_atccode]["clinical_trials"] = extract_related_data(related_trials, "clinical_trials")

    return drug_graph

def extract_journal_with_most_drugs(drug_data:dict):
    """
    Extract the name of the journal that mentions the most different drugs.

    Parameters:
    - drug_data (dict): A nested dictionary structure containing drug data.

    Returns:
    - str: The name of the journal with the most mentions of different drugs.
    """
    max_drugs_count = 0
    most_mentioned_journal = None

    for drug, data in drug_data.items():
        pubmed_mentions = data.get("pubmed", [])
        clinical_trials_mentions = data.get("clinical_trials", [])
        all_mentions = pubmed_mentions + clinical_trials_mentions

        unique_drugs_count = len(set(mention["journal"] for mention in all_mentions))

        if unique_drugs_count > max_drugs_count:
            max_drugs_count = unique_drugs_count
            most_mentioned_journal = max(all_mentions, key=lambda x: all_mentions.count(x))["journal"]

    return most_mentioned_journal

def find_related_drugs(drug_data, target_drug):
    """
    For a given drug, find the set of drugs mentioned by the same journals referenced in PubMed but not in Clinical Trials.

    Parameters:
    - drug_data (dict): A nested dictionary structure containing drug data.
    - target_drug (str): The drug for which related drugs are to be found.

    Returns:
    - set: The set of drugs mentioned by the same journals in PubMed but not in Clinical Trials.
    """
    related_drugs = set()

    if target_drug in drug_data:
        pubmed_mentions = drug_data[target_drug].get("pubmed", [])

        # Extract PubMed journals for the target drug
        pubmed_journals = set(mention["journal"] for mention in pubmed_mentions)

        for drug, data in drug_data.items():
            if drug != target_drug:
                drug_pubmed_mentions = set(mention["journal"] for mention in data.get("pubmed", []))

                # Check if the PubMed journals of the target drug and the current drug intersect
                common_journals_with_target = drug_pubmed_mentions.intersection(pubmed_journals)

                if common_journals_with_target:
                    related_drugs.add(drug)

    return related_drugs