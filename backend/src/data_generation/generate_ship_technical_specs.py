import json
import re
import pandas as pd

from src.utils.data import fetch_ship_ids
from dotenv import load_dotenv
from dataclasses import dataclass

from sdv.metadata import Metadata
from sdv.single_table import GaussianCopulaSynthesizer

load_dotenv()

@dataclass
class ShipTechnicalSpecs:
    IMO_number: str
    built_year: str
    length: str
    beam: str
    gross_tonnage: str
    dwt: str
    
    
def extract_field(value, field_name):
    pattern = {
        "IMO": r"IMO number:\s*(\d+)",
        "Call Sign": r"Call\s?sign[:\s]*([A-Za-z0-9]+)",
        "MMSI": r"MMSI number[:\s]*([A-Za-z0-9]+)",
        "GT": r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\u00A0]*(?:GT|GRT)",
        "NT": r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\u00A0]*NT",
        "DWT": r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\u00A0]*(?:t[\s\u00A0]*)?DWT"
    }
    match = re.search(pattern.get(field_name, ""), value, re.IGNORECASE)
    if match:
        return match.group(1).replace(",", "")
    return "N/A"


def get_year(ship_details, name1:str, name2:str):
    year_of_completion = ship_details.get(name1, "")
    if year_of_completion:
        return year_of_completion
    
    return ship_details.get(name2, "Missing")


def get_similar_values(ship_details, name1:str, name2:str):
    beam = ship_details.get(name1, "")
    if beam:
        match = re.search(r"\d+\.?\d*\s*m", beam, re.IGNORECASE)
        if match:
            return match.group(0)
    
    return ship_details.get(name2, "Missing")


def get_tonnage_value(ship_details, primary_field, backup_field, regex_pattern):
    direct_value = ship_details.get(backup_field)
    if direct_value and direct_value != "Missing":
        return str(direct_value)
    
    tonnage = ship_details.get("Tonnage", "")
    if tonnage:
        extracted = extract_field(tonnage, regex_pattern)
        if extracted != "N/A":
            return extracted
            
    return "Missing"


def process_ship_data(ship_data):
    processed_data = []

    for imo_number, ship_details in ship_data.items():
        imo_number = imo_number
        built_year = get_year(ship_details, "Completed", "Built year")
        length = get_similar_values(ship_details, "Length", "Length (m)")
        beam = get_similar_values(ship_details, "Beam", "Breadth (m)")
        gross_tonnage = get_tonnage_value(ship_details, "Tonnage", "grossTonnage", "GT")
        deadweight = get_tonnage_value(ship_details, "DWT", "deadweight", "DWT")
        
        ship = ShipTechnicalSpecs(
            IMO_number=imo_number,
            built_year=built_year,
            length=length,
            beam=beam,
            gross_tonnage=gross_tonnage,
            dwt=deadweight,
        )
        processed_data.append(ship)
    return processed_data


def fix_column_values_and_types(df):
    regex_pattern = r'(\d+(\.\d+)?)\s*m'
    df['length (m)'] = df['length'].str.extract(regex_pattern)[0]
    df['beam (m)'] = df['beam'].str.extract(regex_pattern)[0]

    df['built_year'] = pd.to_datetime(df['built_year'])
    df['year'] = df['built_year'].dt.year
    
    df.drop(['built_year', 'length', 'beam'], axis=1, inplace=True)
    df.rename(columns={'year': 'built_year', 'dwt': 'dwt (tonnes)'}, inplace=True)

    df['length (m)'] = df['length (m)'].astype(float)
    df['beam (m)'] = df['beam (m)'].astype(float)
    
    return df

def generate_synthetic_data(df):
    # create the metadata first
    metadata = Metadata.detect_from_dataframe(
        data=df,
        table_name='ship_specs'
    )
    
    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(df)

    synthetic_data = synthesizer.sample(num_rows=20795)
    
    return synthetic_data

def main():
    file_names = ["wikipedia_ship_data_v2", "pleiades_fleet_v2"]
    dataframes = []

    for name in file_names:
        with open(f'../data/raw/ship_particulars/{name}.json', 'r') as f:
            data = json.load(f)
            processed_data = process_ship_data(data)
            output_data = [ship.__dict__ for ship in processed_data]
            dataframes.append(pd.DataFrame(output_data))

    df = pd.concat(dataframes, ignore_index=True)
    df.to_csv('../../data/processed/ship_specs_sample.csv', index=False)
    
    # after some manual processing
    full_data = pd.read_csv('ship_specs_full.csv')
    full_data = fix_column_values_and_types(full_data)
    
    unique_ship_ids = fetch_ship_ids()
    imo_numbers_for_synthetic_data = unique_ship_ids[~unique_ship_ids['imo_number'].isin(full_data['IMO_number'].to_list())].reset_index(drop=True)
    imo_numbers_for_synthetic_data.shape
    
    synthetic_data = generate_synthetic_data(full_data)
    synthetic_data = imo_numbers_for_synthetic_data.join(synthetic_data)
    synthetic_data['synthetic'] = True
    synthetic_data.drop(['IMO_number'], axis=1, inplace=True)
    
    # prepare the ground truth dataset to combine it
    full_data = full_data.rename(columns={'IMO_number': 'imo_number'})
    full_data['synthetic'] = False
    
    # combine synthetic data and ground truth data
    ship_specs_final = pd.concat([synthetic_data, full_data], ignore_index=True).reset_index(drop=True)
    ship_specs_final.to_csv('../data/processed/ship_technical_specs.csv', index=False)
    

if __name__ == "__main__":
    main()