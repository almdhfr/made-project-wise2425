import pandas as pd
import requests
import os
import zipfile
import io
from sqlalchemy import create_engine
import logging
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configuration
DATA_DIR = os.getenv("DATA_DIR", "./data")
COLLISION_URL = os.getenv("COLLISION_URL", "https://data.cityofnewyork.us/resource/h9gi-nx95.csv")
POPULATION_URL = os.getenv("POPULATION_URL", "https://data.cityofnewyork.us/resource/xi7c-iiu2.csv")
STREET_ZIP_URL = "https://data.cityofnewyork.us/download/w4v2-rv6b/application%2Fzip"
STREET_FILENAME = "bobaadr.txt"

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

def download_data(url, save_path):
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
        return pd.read_csv(save_path)
    except Exception as e:
        logging.error(f"Failed to download data from {url}: {e}")
        raise


def download_and_extract_zip(zip_url, extract_filename, save_path):
    """
    Downloads a ZIP file, extracts a specific file, and saves it to the specified path.
    Handles nested directory structures within the ZIP file.
    """
    try:
        logging.info(f"Downloading ZIP file from {zip_url}...")
        response = requests.get(zip_url, timeout=10)
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Look for the target file in the ZIP archive
            target_file = None
            for file_name in zf.namelist():
                if file_name.endswith(extract_filename):
                    target_file = file_name
                    break

            if not target_file:
                raise FileNotFoundError(f"{extract_filename} not found in the ZIP archive.")

            # Extract and save the file
            with zf.open(target_file) as file:
                with open(save_path, "wb") as output_file:
                    output_file.write(file.read())
        logging.info(f"Extracted {target_file} to {save_path}")
    except Exception as e:
        logging.error(f"Failed to download or extract {extract_filename}: {e}")
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
    # Ensure the population column is numeric
    data['population'] = pd.to_numeric(data['population'], errors='coerce').fillna(0).astype(int)
    # Group by borough and aggregate population
    if 'borough' in data.columns:
        data = data.groupby('borough', as_index=False)['population'].sum()
    else:
        raise KeyError("'borough' column is missing in the dataset. Cannot aggregate by borough.")
    # Rename the aggregated column to 'total_population' for clarity
    data.rename(columns={'population': 'total_population'}, inplace=True)
    logging.info("Population data aggregated by borough.")
    return data

def preprocess_street_mapping(txt_path):
    """
    Preprocesses bobaadr.txt into a dictionary for fast lookups.
    """
    logging.info("Preprocessing street mapping from bobaadr.txt...")
    data = pd.read_csv(txt_path, delimiter=',', dtype=str)

    # Normalize column names
    data.columns = data.columns.str.lower().str.strip()

    # Map borough codes to borough names
    borocode_to_borough = {
        "1": "Manhattan",
        "2": "Bronx",
        "3": "Brooklyn",
        "4": "Queens",
        "5": "Staten Island"
    }
    data['borough'] = data['boro'].map(borocode_to_borough)

    # Create a dictionary {stname: borough}
    mapping = data.set_index('stname')['borough'].to_dict()
    logging.info("Street mapping preprocessed successfully.")
    return mapping


def integrate_street_names(data, street_mapping):
    """
    Optimized integration of street names with the bobaadr.txt mapping.
    Uses hash-based lookups for faster performance and updates borough names.
    """
    logging.info("Integrating street names with the bobaadr.txt mapping...")

    # Check for matches in on_street_name, off_street_name, and cross_street_name
    data['on_borough'] = data['on_street_name'].map(street_mapping)
    data['off_borough'] = data['off_street_name'].map(street_mapping)
    data['cross_borough'] = data['cross_street_name'].map(street_mapping)

    # Debugging: Check intermediate results for null boroughs
    logging.debug(f"on_borough matches:\n{data[['on_street_name', 'on_borough']].head()}")
    logging.debug(f"off_borough matches:\n{data[['off_street_name', 'off_borough']].head()}")
    logging.debug(f"cross_borough matches:\n{data[['cross_street_name', 'cross_borough']].head()}")

    # Prioritize on_street_name, then off_street_name, then cross_street_name
    data['borough'] = data['borough'].where(data['borough'].notna(), data['on_borough'])
    data['borough'] = data['borough'].where(data['borough'].notna(), data['off_borough'])
    data['borough'] = data['borough'].where(data['borough'].notna(), data['cross_borough'])

    # Drop intermediate columns
    data.drop(columns=['on_borough', 'off_borough', 'cross_borough'], inplace=True)

    logging.info("Street name integration completed.")
    return data

def parallel_download():
    """
    Downloads all datasets in parallel.
    """
    urls = [COLLISION_URL, POPULATION_URL, STREET_ZIP_URL]
    save_paths = [
        os.path.join(DATA_DIR, "raw_collisions.csv"),
        os.path.join(DATA_DIR, "raw_population.csv"),
        os.path.join(DATA_DIR, "raw_street_to_borough.csv")
    ]
    with ThreadPoolExecutor() as executor:
        executor.map(lambda args: download_data(*args), zip(urls, save_paths))


def main():
    # Download collisions data
    collisions_data = download_data(COLLISION_URL, os.path.join(DATA_DIR, "raw_collisions.csv"))

    # Download and extract bobaadr.txt
    street_txt_path = os.path.join(DATA_DIR, STREET_FILENAME)
    download_and_extract_zip(STREET_ZIP_URL, STREET_FILENAME, street_txt_path)

    # Load street-to-borough mapping
    street_mapping = preprocess_street_mapping(street_txt_path)

    # Clean and process collisions data
    cleaned_collisions = clean_collisions_data(collisions_data)
    cleaned_collisions = integrate_street_names(cleaned_collisions, street_mapping)

    # Download and process population data
    population_data = download_data(POPULATION_URL, os.path.join(DATA_DIR, "raw_population.csv"))
    cleaned_population = clean_population_data(population_data)

    # Save cleaned data
    save_to_sqlite(cleaned_collisions, os.path.join(DATA_DIR, "collisions.db"), "collisions")
    save_to_sqlite(cleaned_population, os.path.join(DATA_DIR, "population.db"), "population")


if __name__ == "__main__":
    main()
