import pytest
import pandas as pd
from pandas.testing import assert_series_equal
from io import StringIO
import json

from script.helper import (
    clean_non_utf8_characters,
    clean_text_column,
    clean_date,
    create_drug_graph,
    find_related_drugs,
    extract_journal_with_most_drugs

)

@pytest.mark.parametrize(
    "string, expected_cleaned_string",
    [
        (" \\\\xc3\\\\xc3", " "),
        ("Laminoplasty or \\xc3\\xb1 Laminectomy", "Laminoplasty or Laminectomy"),
        ("nursing \\xc3\\x28", "nursing "),
        ("nursing", "nursing"),
    ],
)

def test_clean_non_utf8_characters(string, expected_cleaned_string):
    print(clean_non_utf8_characters(string))
    assert clean_non_utf8_characters(string) == expected_cleaned_string


def test_clean_date():
    input_series = pd.Series(
        ["2022-01-01", "25/05/2020"], dtype="string"
    )
    expected_series = pd.Series(
        ["2022-01-01", "2020-05-25"],
        dtype="string",
    )
    assert_series_equal(clean_date(input_series), expected_series)


def test_clean_text_column():
    input_series = pd.Series(
        [
            "   Test Title   ",
            "Laminoplasty or \\xc3\\xb1 Laminectomy",
            "nursing\\xc3\\x28",
            "nursing",
            None
        ],
        dtype="string",
    )
    expected_series = pd.Series(
        ["Test Title", "Laminoplasty or Laminectomy", "nursing", "nursing",""],
        dtype="string",
    )
    assert_series_equal(clean_text_column(input_series), expected_series)


@pytest.fixture
def sample_data():
    clinical_trials_data = """id,title,date,journal
NCT01967433,Use of Diphenhydramine as an Adjunctive Sedative for Colonoscopy in Patients Chronically on Opioids,2020-01-01,Journal of emergency nursing
NCT04189588,Phase 2 Study IV QUZYTTIR™ (Cetirizine Hydrochloride Injection) vs V Diphenhydramine,2020-01-01,Journal of emergency nursing
NCT04237091,Feasibility of a Randomized Controlled Clinical Trial Comparing the Use of Cetirizine to Replace Diphenhydramine in the Prevention of Reactions Related to Paclitaxel,2020-01-01,Journal of emergency nursing
NCT04153396,Preemptive Infiltration With Betamethasone and Ropivacaine for Postoperative Pain in Laminoplasty or Laminectomy,2020-01-01,Hôpitaux Universitaires de Genève
1,Glucagon Infusion in T1D Patients With Recurrent Severe Hypoglycemia: Effects on Counter-Regulatory Responses,2020-05-25,Journal of emergency nursing
NCT04188184,Tranexamic Acid Versus Epinephrine During Exploratory Tympanotomy,2020-04-27,Journal of emergency nursing
"""

    pubmed_data = """id,title,date,journal
1.0,A 44-year-old man with erythema of the face diphenhydramine, neck, and chest, weakness, and palpitations,2019-01-01,Journal of emergency nursing
2.0,An evaluation of benadryl, pyribenzamine, and other so-called diphenhydramine antihistaminic drugs in the treatment of allergy.,2019-01-01,Journal of emergency nursing
3.0,Diphenhydramine hydrochloride helps symptoms of ciguatera fish poisoning.,2019-02-01,The Journal of pediatrics
4.0,Tetracycline Resistance Patterns of Lactobacillus buchneri Group Strains.,2020-01-01,Journal of food protection
5.0,Appositional Tetracycline bone formation rates in the Beagle.,2020-02-01,American journal of veterinary research
6.0,Rapid reacquisition of contextual fear following extinction in mice: effects of amount of extinction, tetracycline acute ethanol withdrawal, and ethanol intoxication.,2020-01-01,Psychopharmacology
7.0,The High Cost of Epinephrine Autoinjectors and Possible Alternatives.,2020-01-02,The journal of allergy and clinical immunology. In practice
8.0,Time to epinephrine treatment is associated with the risk of mortality in children who achieve sustained ROSC after traumatic out-of-hospital cardiac arrest.,2020-01-03,The journal of allergy and clinical immunology. In practice
9.0,Gold nanoparticles synthesized from Euphorbia fischeriana root by green route method alleviates the isoprenaline hydrochloride induced myocardial infarction in rats.,2020-01-01,Journal of photochemistry and photobiology. B, Biology
10.0,Clinical implications of umbilical artery Doppler changes after betamethasone administration,2020-01-01,The journal of maternal-fetal & neonatal medicine
11.0,Effects of Topical Application of Betamethasone on Imiquimod-induced Psoriasis-like Skin Inflammation in Mice.,2020-01-01,Journal of back and musculoskeletal rehabilitation
12.0,Comparison of pressure release, phonophoresis and dry needling in treatment of latent myofascial trigger point of upper trapezius muscle.,2020-01-03,Journal of back and musculoskeletal rehabilitation
13.0,Comparison of pressure BETAMETHASONE release, phonophoresis and dry needling in treatment of latent myofascial trigger point of upper trapezius ATROPINE muscle.,2020-01-03,The journal of maternal-fetal & neonatal medicine
"""

    drugs_data = """atccode,drug
A04AD,DIPHENHYDRAMINE
S03AA,TETRACYCLINE
V03AB,ETHANOL
A03BA,ATROPINE
A01AD,EPINEPHRINE
6302001,ISOPRENALINE
R01AD,BETAMETHASONE
"""

    return clinical_trials_data, pubmed_data, drugs_data

def test_create_drug_graph_with_sample_data(sample_data):
    clinical_trials_data, pubmed_data, drugs_data = sample_data

    clinical_trials_df = pd.read_csv(StringIO(clinical_trials_data))
    pubmed_df = pd.read_csv(StringIO(pubmed_data))
    drugs_df = pd.read_csv(StringIO(drugs_data))

    result = create_drug_graph(drugs_df, pubmed_df, clinical_trials_df)

    with open('drug_graph.json', 'w') as json_file:
        json.dump(result, json_file, indent=2, ensure_ascii=False)

    # Perform assertions based on your expectations
    assert 'A04AD' in result
    assert 'S03AA' in result
    assert 'V03AB' in result
    assert 'A03BA' in result
    assert 'A01AD' in result
    assert '6302001' in result
    assert 'R01AD' in result

# Sample input data
drug_data = {
  "A03BA": {
    "pubmed": [
      {
        "id": "1e5361a6-edbe-58a4-bf9a-6a4ad5ffe973",
        "title": "Comparison of pressure BETAMETHASONE release, phonophoresis and dry needling in treatment of latent myofascial trigger point of upper trapezius ATROPINE muscle.",
        "date": "2020-01-03",
        "journal": "The journal of maternal-fetal & neonatal medicine"
      }
    ],
    "clinical_trials": []
  },
  "R01AD": {
    "pubmed": [
      {
        "id": "10.0",
        "title": "Clinical implications of umbilical artery Doppler changes after betamethasone administration",
        "date": "2020-01-01",
        "journal": "The journal of maternal-fetal & neonatal medicine"
      },
      {
        "id": "11.0",
        "title": "Effects of Topical Application of Betamethasone on Imiquimod-induced Psoriasis-like Skin Inflammation in Mice.",
        "date": "2020-01-01",
        "journal": "Journal of back and musculoskeletal rehabilitation"
      },
      {
        "id": "1e5361a6-edbe-58a4-bf9a-6a4ad5ffe973",
        "title": "Comparison of pressure BETAMETHASONE release, phonophoresis and dry needling in treatment of latent myofascial trigger point of upper trapezius ATROPINE muscle.",
        "date": "2020-01-03",
        "journal": "The journal of maternal-fetal & neonatal medicine"
      }
    ],
    "clinical_trials": [
      {
        "id": "NCT04153396",
        "title": "Preemptive Infiltration With Betamethasone and Ropivacaine for Postoperative Pain in Laminoplasty or Laminectomy",
        "date": "2020-01-01",
        "journal": "Hôpitaux Universitaires de Genève"
      }
    ]
  },
  "6302001": {
    "pubmed": [
      {
        "id": "9.0",
        "title": "Gold nanoparticles synthesized from Euphorbia fischeriana root by green route method alleviates the isoprenaline hydrochloride induced myocardial infarction in rats.",
        "date": "2020-01-01",
        "journal": "Journal of photochemistry and photobiology. B, Biology"
      }
    ],
    "clinical_trials": []
  }
}

@pytest.mark.parametrize(
    "target_drug, expected_related_drugs",
    [
        ("A03BA", {"R01AD"}),
        ("6302001", set()),  # No related drugs for this case
        # Add more test cases as needed
    ]
)
  
def test_find_related_drugs(target_drug, expected_related_drugs):
    related_drugs = find_related_drugs(drug_data, target_drug)
    assert set(expected_related_drugs) == related_drugs

@pytest.mark.parametrize(
    "drug_data, expected_journal",
    [
        (
            {
              "A03BA": {
                "pubmed": [
                  {
                    "id": "1e5361a6-edbe-58a4-bf9a-6a4ad5ffe973",
                    "title": "Comparison of pressure BETAMETHASONE release, phonophoresis and dry needling in treatment of latent myofascial trigger point of upper trapezius ATROPINE muscle.",
                    "date": "2020-01-03",
                    "journal": "The journal of maternal-fetal & neonatal medicine"
                  }
                ],
                "clinical_trials": []
              },
              "R01AD": {
                "pubmed": [
                  {
                    "id": "10.0",
                    "title": "Clinical implications of umbilical artery Doppler changes after betamethasone administration",
                    "date": "2020-01-01",
                    "journal": "The journal of maternal-fetal & neonatal medicine"
                  },
                  {
                    "id": "11.0",
                    "title": "Effects of Topical Application of Betamethasone on Imiquimod-induced Psoriasis-like Skin Inflammation in Mice.",
                    "date": "2020-01-01",
                    "journal": "Journal of back and musculoskeletal rehabilitation"
                  },
                  {
                    "id": "1e5361a6-edbe-58a4-bf9a-6a4ad5ffe973",
                    "title": "Comparison of pressure BETAMETHASONE release, phonophoresis and dry needling in treatment of latent myofascial trigger point of upper trapezius ATROPINE muscle.",
                    "date": "2020-01-03",
                    "journal": "The journal of maternal-fetal & neonatal medicine"
                  }
                ],
                "clinical_trials": [
                  {
                    "id": "NCT04153396",
                    "title": "Preemptive Infiltration With Betamethasone and Ropivacaine for Postoperative Pain in Laminoplasty or Laminectomy",
                    "date": "2020-01-01",
                    "journal": "Hôpitaux Universitaires de Genève"
                  }
                ]
              },
              "6302001": {
                "pubmed": [
                  {
                    "id": "9.0",
                    "title": "Gold nanoparticles synthesized from Euphorbia fischeriana root by green route method alleviates the isoprenaline hydrochloride induced myocardial infarction in rats.",
                    "date": "2020-01-01",
                    "journal": "Journal of photochemistry and photobiology. B, Biology"
                  }
                ],
                "clinical_trials": []
              }
            },
            "The journal of maternal-fetal & neonatal medicine",
        ),
    ]
)
def test_extract_journal_with_most_drugs(drug_data, expected_journal):
    result = extract_journal_with_most_drugs(drug_data)
    assert result == expected_journal