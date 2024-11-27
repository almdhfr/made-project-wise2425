import pandas as pd
import requests
import os
from sqlalchemy import create_engine


def download_data(url, save_path):
    """
    Downloads a CSV file from the given URL and saves it to the specified path.
    """
    print(f"Downloading data from {url}...")
    response = requests.get(url)
    response.raise_for_status()
    
    with open(save_path, "wb") as file:
        file.write(response.content)
    print(f"Saved raw data to {save_path}")
    
    return pd.read_csv(save_path)


def save_to_sqlite(dataframe, database_path, table_name):
    """
    Saves a DataFrame to an SQLite database.
    """
    print(f"Saving data to SQLite database: {database_path} (table: {table_name})...")
    engine = create_engine(f"sqlite:///{database_path}")
    dataframe.to_sql(table_name, engine, if_exists="replace", index=False)
    print("Data saved successfully.")


def clean_collisions_data(data):
    """
    Cleans the Motor Vehicle Collisions dataset.
    """
    print("Cleaning collisions data...")
    
    # Drop duplicate rows
    data = data.drop_duplicates()
    
    # Handle missing borough data
    data['borough'] = data['borough'].fillna("Unknown")
    
    # Convert date columns to datetime
    data['crash_date'] = pd.to_datetime(data['crash_date'], errors='coerce')
    data['crash_time'] = pd.to_datetime(data['crash_time'], format='%H:%M', errors='coerce').dt.time
    
    # Filter out rows with invalid dates
    data = data.dropna(subset=['crash_date'])
    
    # Replace missing values in fatality-related columns with 0
    fatality_columns = [
        'number_of_persons_killed',
        'number_of_pedestrians_killed',
        'number_of_cyclist_killed',
        'number_of_motorist_killed'
    ]
    for col in fatality_columns:
        if col in data.columns:
            data[col] = data[col].fillna(0).astype(int)
        else:
            print(f"Warning: Column {col} is missing in the dataset.")

    # Create a new column for total fatalities
    data['total_fatalities'] = data[fatality_columns].sum(axis=1)
    
    print("Collisions data cleaned.")
    return data


def clean_population_data(data):
    """
    Cleans the Population by Community District dataset, focusing on the 2010 population column.
    """
    print("Cleaning population data...")
    
    # Normalize column names
    data.columns = data.columns.str.lower().str.strip()
    
    # Check if '_2010_population' column exists
    if '_2010_population' not in data.columns:
        raise KeyError("'_2010_population' column is missing in the dataset. Check the dataset format.")
    
    # Rename `_2010_population` to `population` for easier handling
    data.rename(columns={'_2010_population': 'population'}, inplace=True)
    
    # Ensure the population column is numeric
    data['population'] = pd.to_numeric(data['population'], errors='coerce')
    data = data.dropna(subset=['population'])  # Drop rows with invalid population values
    
    # Drop unnecessary columns (keep only relevant ones, like borough/community name)
    relevant_columns = ['borough', 'population'] if 'borough' in data.columns else ['community_district', 'population']
    data = data[relevant_columns]
    
    print("Population data cleaned.")
    return data

def load_street_to_borough_mapping():
    """
    Downloads and processes the street-to-borough dataset to create a mapping dictionary.
    """
    print("Loading street-to-borough mapping...")
    url = "https://data.cityofnewyork.us/api/views/8rma-cm9c/rows.csv?accessType=DOWNLOAD"
    save_path = "./data/raw_street_to_borough.csv"
    
    if not os.path.exists(save_path):
        street_data = download_data(url, save_path)
    else:
        street_data = pd.read_csv(save_path)
    
    # Normalize column names
    street_data.columns = street_data.columns.str.lower().str.strip()
    
    # Rename `full_stree` to `street_name` for consistency
    if 'full_stree' in street_data.columns:
        street_data.rename(columns={'full_stree': 'street_name'}, inplace=True)
    else:
        raise KeyError("Column 'full_stree' not found in the dataset.")
    
    # Translate `borocode` to borough names
    borocode_to_borough = {
        1: "Manhattan",
        2: "Bronx",
        3: "Brooklyn",
        4: "Queens",
        5: "Staten Island"
    }
    if 'borocode' in street_data.columns:
        street_data['borough'] = street_data['borocode'].map(borocode_to_borough)
    else:
        raise KeyError("Column 'borocode' not found in the dataset.")
    
    # Ensure relevant columns exist
    if 'street_name' not in street_data.columns or 'borough' not in street_data.columns:
        raise KeyError("Expected columns 'street_name' and 'borough' not found in the dataset.")
    
    # Create a mapping dictionary: {street_name: borough}
    mapping = street_data.set_index('street_name')['borough'].to_dict()
    print("Street-to-borough mapping loaded.")
    return mapping


def detect_borough(data, street_to_borough_mapping):
    """
    Detects the borough for each row based on on_street_name and off_street_name using a provided mapping.
    """
    print("Detecting boroughs based on street names...")
    
    def match_borough(row):
        # Check on_street_name
        if row['on_street_name'] in street_to_borough_mapping:
            return street_to_borough_mapping[row['on_street_name']]
        # Check off_street_name
        if row['off_street_name'] in street_to_borough_mapping:
            return street_to_borough_mapping[row['off_street_name']]
        # Fallback to existing borough or mark as Unknown
        return row['borough'] if row['borough'] != "Unknown" else "Unknown"
    
    # Apply the matching function row-wise
    data['borough'] = data.apply(match_borough, axis=1)
    print("Borough detection completed.")
    return data


def main():
    # Load collision data
    collisions_data = download_data(
        "https://data.cityofnewyork.us/resource/h9gi-nx95.csv", "./data/raw_collisions.csv"
    )
    
    # Load population data
    population_data = download_data(
        "https://data.cityofnewyork.us/resource/xi7c-iiu2.csv", "./data/raw_population.csv"
    )
    
    # Load street-to-borough mapping
    street_to_borough_mapping = load_street_to_borough_mapping()
    
    # Clean collision data and detect boroughs
    cleaned_collisions = clean_collisions_data(collisions_data)
    cleaned_collisions = detect_borough(cleaned_collisions, street_to_borough_mapping)
    
    # Clean population data
    cleaned_population = clean_population_data(population_data)
    
    # Save cleaned data
    save_to_sqlite(cleaned_collisions, "./data/collisions.db", "collisions")
    save_to_sqlite(cleaned_population, "./data/population.db", "population")

if __name__ == "__main__":
    main()
