import os
import pandas as pd
import sqlite3

# Create a directory to store the cleaned data
DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)

# URLs for the datasets
COLLISIONS_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.csv"
POPULATION_URL = "https://data.cityofnewyork.us/resource/xi7c-iiu2.csv"

def download_data(url, filename):
    """
    Downloads a CSV file from a URL and saves it locally.
    """
    try:
        print(f"Downloading data from {url}...")
        data = pd.read_csv(url)
        filepath = os.path.join(DATA_DIR, filename)
        data.to_csv(filepath, index=False)
        print(f"Saved raw data to {filepath}")
        return data
    except Exception as e:
        print(f"Error downloading data: {e}")
        return None

def clean_collisions_data(data):
    """
    Cleans the Motor Vehicle Collisions dataset and adds analysis for fatalities per borough.
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
    
    print("Collisions data cleaned and fatality information added.")
    return data

def clean_population_data(data):
    
    data.rename(columns={'_2010_population': 'population'}, inplace=True)
    """
    Cleans the Population by Community District dataset.
    """
    print("Cleaning population data...")
    
    # Debugging: Print column names to ensure correctness
    print("Population data columns:", data.columns)
    
    # Normalize column names (lowercase and stripped of leading/trailing spaces)
    data.columns = data.columns.str.lower().str.strip()
    
    # Check if 'population' column exists
    if 'population' not in data.columns:
        raise KeyError("'population' column is missing in the dataset. Check the dataset format.")
    
    # Drop duplicate rows
    data = data.drop_duplicates()
    
    # Ensure population is numeric
    data['population'] = pd.to_numeric(data['population'], errors='coerce')
    data = data.dropna(subset=['population'])  # Drop rows with invalid population values
    
    print("Population data cleaned.")
    return data

def summarize_fatalities_by_borough(data):
    """
    Summarizes total and average fatalities per borough.
    """
    print("Summarizing fatalities by borough...")
    
    # Group by borough and calculate total and average fatalities
    summary = (
        data.groupby('borough')
        .agg(total_fatalities=('total_fatalities', 'sum'),
             average_fatalities=('total_fatalities', 'mean'),
             incidents=('borough', 'size'))
        .reset_index()
    )
    
    # Sort by total fatalities in descending order
    summary = summary.sort_values(by='total_fatalities', ascending=False)
    
    print("Fatality summary by borough:\n", summary)
    return summary

def save_to_sqlite(data, db_name, table_name):
    """
    Saves the dataframe to an SQLite database.
    """
    db_path = os.path.join(DATA_DIR, db_name)
    conn = sqlite3.connect(db_path)
    try:
        data.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Saved data to SQLite database: {db_path} (table: {table_name})")
    finally:
        conn.close()

def main():
    # Download datasets
    collisions_data = download_data(COLLISIONS_URL, "raw_collisions.csv")
    population_data = download_data(POPULATION_URL, "raw_population.csv")

    # Process collisions data
    if collisions_data is not None:
        cleaned_collisions = clean_collisions_data(collisions_data)
        save_to_sqlite(cleaned_collisions, "collisions.db", "collisions")
        
        # Summarize fatalities by borough
        fatality_summary = summarize_fatalities_by_borough(cleaned_collisions)
        summary_path = os.path.join(DATA_DIR, "fatality_summary.csv")
        fatality_summary.to_csv(summary_path, index=False)
        print(f"Fatality summary saved to {summary_path}")

    # Process population data
    if population_data is not None:
        cleaned_population = clean_population_data(population_data)
        save_to_sqlite(cleaned_population, "population.db", "population")
        
        # Join population data with fatality summary
        print("Merging population data with fatality summary...")
        cleaned_population.rename(columns={'borough': 'borough_name'}, inplace=True)
        merged_data = pd.merge(
            fatality_summary,
            cleaned_population,
            left_on='borough',
            right_on='borough_name',
            how='inner'
        )
        merged_data['fatalities_per_100k'] = (merged_data['total_fatalities'] / merged_data['population']) * 100000
        
        # Save merged data
        merged_path = os.path.join(DATA_DIR, "merged_fatalities_population.csv")
        merged_data.to_csv(merged_path, index=False)
        print(f"Merged data saved to {merged_path}")

if __name__ == "__main__":
    main()
