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
    data = data.drop_duplicates()
    data['borough'] = data['borough'].fillna("Unknown")
    data['crash_date'] = pd.to_datetime(data['crash_date'], errors='coerce')
    data['crash_time'] = pd.to_datetime(data['crash_time'], format='%H:%M', errors='coerce').dt.time
    data = data.dropna(subset=['crash_date'])

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
            logging.warning(f"Column {col} is missing in the dataset.")
    data['total_fatalities'] = data[fatality_columns].sum(axis=1)
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

def load_street_to_borough_mapping():
    """
    Downloads and processes the street-to-borough dataset to create a mapping dictionary.
    """
    logging.info("Loading street-to-borough mapping...")
    save_path = os.path.join(DATA_DIR, "raw_street_to_borough.csv")
    street_data = download_data(STREET_MAPPING_URL, save_path)
    street_data.columns = street_data.columns.str.lower().str.strip()
    street_data.rename(columns={'full_stree': 'street_name'}, inplace=True)
    borocode_to_borough = {
        1: "Manhattan", 2: "Bronx", 3: "Brooklyn", 4: "Queens", 5: "Staten Island"
    }
    street_data['borough'] = street_data['borocode'].map(borocode_to_borough)
    mapping = street_data.set_index('street_name')['borough'].to_dict()
    logging.info("Street-to-borough mapping loaded.")
    return mapping

def detect_borough(data, street_to_borough_mapping):
    """
    Detects the borough for each row based on street names.
    """
    logging.info("Detecting boroughs based on street names...")
    on_street_boroughs = data['on_street_name'].map(street_to_borough_mapping)
    off_street_boroughs = data['off_street_name'].map(street_to_borough_mapping)
    data['borough'] = on_street_boroughs.fillna(off_street_boroughs).fillna(data['borough'])
    logging.info("Borough detection completed.")
    return data

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
