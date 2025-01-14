import pandas as pd
import requests
import os
from sqlalchemy import create_engine
import logging
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configuration
DATA_DIR = os.getenv("DATA_DIR", "./data")
COLLISION_URL = os.getenv("COLLISION_URL", "https://data.cityofnewyork.us/resource/h9gi-nx95.csv")
POPULATION_URL = os.getenv("POPULATION_URL", "https://data.cityofnewyork.us/resource/xi7c-iiu2.csv")
STREET_MAPPING_URL = os.getenv(
    "STREET_MAPPING_URL",
    "https://data.cityofnewyork.us/api/views/8rma-cm9c/rows.csv?accessType=DOWNLOAD"
)

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

def download_data(url, save_path, dtype=None):
    """
    Downloads a CSV file from the given URL and saves it to the specified path.
    """
    try:
        logging.info(f"Downloading data from {url}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(save_path, "wb") as file:
            file.write(response.content)
        logging.info(f"Saved raw data to {save_path}")
        return pd.read_csv(save_path, dtype=dtype, low_memory=False)
    except Exception as e:
        logging.error(f"Failed to download data from {url}: {e}")
        raise

def save_to_sqlite(dataframe, database_path, table_name):
    """
    Saves a DataFrame to an SQLite database.
    """
    try:
        logging.info(f"Saving data to SQLite database: {database_path} (table: {table_name})...")
        engine = create_engine(f"sqlite:///{database_path}")
        dataframe.to_sql(table_name, engine, if_exists="replace", index=False)
        logging.info("Data saved successfully.")
    except Exception as e:
        logging.error(f"Error saving data to SQLite: {e}")
        raise


def clean_collisions_data(data):
    """
    Cleans the Motor Vehicle Collisions dataset.
    """
    logging.info("Cleaning collisions data...")

    # Drop duplicate rows
    data = data.drop_duplicates()

    # Handle missing borough data
    data['borough'] = data['borough'].fillna("Unknown")

    # Convert date columns to datetime
    data['crash_date'] = pd.to_datetime(data['crash_date'], errors='coerce')
    data['crash_time'] = pd.to_datetime(data['crash_time'], format='%H:%M', errors='coerce').dt.time

    # Filter out rows with invalid dates
    data = data.dropna(subset=['crash_date'])

    # Drop unwanted columns
    columns_to_drop = [
                          'crash_date', 'crash_time', 'latitude', 'longitude', 'location', 'collision_id'
                      ] + [col for col in data.columns if "vehicle" in col and col != "vehicle_type_code1"]
    data = data.drop(columns=columns_to_drop, errors='ignore')

    # Rename 'vehicle_type_code1' to 'vehicle_type'
    if 'vehicle_type_code1' in data.columns:
        data.rename(columns={'vehicle_type_code1': 'vehicle_type'}, inplace=True)

    # Identify fatality-related columns dynamically
    fatality_columns = [col for col in data.columns if 'killed' in col.lower()]
    if not fatality_columns:
        raise KeyError("No columns with 'killed' found in the dataset.")

    # Convert fatality columns to numeric and handle missing values
    for col in fatality_columns:
        data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0).astype(int)

    # Create a new column for total injuries
    data['total_fatalities'] = data[fatality_columns].sum(axis=1)

    # Identify injury-related columns dynamically
    injury_columns = [col for col in data.columns if 'injured' in col.lower()]
    if not injury_columns:
        raise KeyError("No columns with 'injured' found in the dataset.")

    # Convert injury columns to numeric and handle missing values
    for col in injury_columns:
        data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0).astype(int)

    # Create a new column for total injuries
    data['total_injuries'] = data[injury_columns].sum(axis=1)

    logging.info("Collisions data cleaned.")
    return data

def clean_population_data(data):
    """
    Cleans the Population by Community District dataset, focusing on the 2010 population column.
    """
    logging.info("Cleaning population data...")
    data.columns = data.columns.str.lower().str.strip()
    if '_2010_population' not in data.columns:
        raise KeyError("'_2010_population' column is missing in the dataset.")
    data.rename(columns={'_2010_population': 'population'}, inplace=True)
    data['population'] = pd.to_numeric(data['population'], errors='coerce')
    data = data.dropna(subset=['population'])
    relevant_columns = ['borough', 'population'] if 'borough' in data.columns else ['community_district', 'population']
    data = data[relevant_columns]
    logging.info("Population data cleaned.")
    return data

def detect_borough(data, street_to_borough_mapping):
    """
    Detects the borough for rows with 'Unknown' boroughs using street names.
    Priority order: on_street_name > off_street_name > cross_street_name.
    """
    logging.info("Detecting boroughs based on street names...")

    def match_borough(row):
        # Check on_street_name
        if row['on_street_name'] in street_to_borough_mapping:
            return street_to_borough_mapping[row['on_street_name']]
        # Check off_street_name
        if row['off_street_name'] in street_to_borough_mapping:
            return street_to_borough_mapping[row['off_street_name']]
        # Check cross_street_name
        if row['cross_street_name'] in street_to_borough_mapping:
            return street_to_borough_mapping[row['cross_street_name']]
        # Fallback to existing borough or mark as Unknown
        return row['borough'] if row['borough'] != "Unknown" else "Unknown"

    # Apply matching logic only to rows with 'Unknown' boroughs
    mask = data['borough'] == "Unknown"
    data.loc[mask, 'borough'] = data[mask].apply(match_borough, axis=1)

    logging.info("Borough detection completed.")
    return data

def load_street_to_borough_mapping():
    """
    Downloads and processes the street-to-borough dataset to create a mapping dictionary.
    """
    logging.info("Loading street-to-borough mapping...")
    save_path = os.path.join(DATA_DIR, "raw_street_to_borough.csv")
    street_data = download_data(STREET_MAPPING_URL, save_path)

    # Normalize column names
    street_data.columns = street_data.columns.str.lower().str.strip()

    # Rename relevant columns for clarity
    if 'full_stree' in street_data.columns:
        street_data.rename(columns={'full_stree': 'street_name'}, inplace=True)
    else:
        raise KeyError("Column 'full_stree' not found in the dataset.")

    # Translate borocode to borough names
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
    logging.info("Street-to-borough mapping loaded successfully.")
    return mapping

def parallel_download():
    """
    Downloads all datasets in parallel.
    """
    urls = [COLLISION_URL, POPULATION_URL, STREET_MAPPING_URL]
    save_paths = [
        os.path.join(DATA_DIR, "raw_collisions.csv"),
        os.path.join(DATA_DIR, "raw_population.csv"),
        os.path.join(DATA_DIR, "raw_street_to_borough.csv")
    ]
    with ThreadPoolExecutor() as executor:
        executor.map(lambda args: download_data(*args), zip(urls, save_paths))

def main():
    parallel_download()
    collisions_data = pd.read_csv(os.path.join(DATA_DIR, "raw_collisions.csv"))
    population_data = pd.read_csv(os.path.join(DATA_DIR, "raw_population.csv"))
    street_to_borough_mapping = load_street_to_borough_mapping()

    cleaned_collisions = clean_collisions_data(collisions_data)
    cleaned_collisions = detect_borough(cleaned_collisions, street_to_borough_mapping)

    cleaned_population = clean_population_data(population_data)

    save_to_sqlite(cleaned_collisions, os.path.join(DATA_DIR, "collisions.db"), "collisions")
    save_to_sqlite(cleaned_population, os.path.join(DATA_DIR, "population.db"), "population")

if __name__ == "__main__":
    main()
