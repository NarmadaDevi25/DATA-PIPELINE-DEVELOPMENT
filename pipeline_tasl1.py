import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import schedule
import time
import requests
import io # Used to read string as a file

# --- 1. Extraction ---

def extract_data_from_url(url):
    """
    Extracts data from a given URL (assuming it's a CSV) into a Pandas DataFrame.

    Args:
        url (str): The URL of the CSV file.

    Returns:
        pd.DataFrame: The extracted data.
    """
    print(f"Attempting to download data from: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
        df = pd.read_csv(io.StringIO(response.text))
        print(f"Successfully extracted data from URL: {url}")
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data from URL: {e}")
        return None
    except pd.errors.EmptyDataError:
        print(f"Error: Empty data received from URL: {url}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during extraction: {e}")
        return None

# --- 2. Transformation ---

def transform_data(df):
    """
    Performs basic data cleaning and transformation steps.

    Args:
        df (pd.DataFrame): The DataFrame to transform.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    if df is None:
        return None

    print("Starting data transformation...")

    # Example: Handle missing values in numerical columns with mean
    for col in df.select_dtypes(include=np.number).columns:
        if df[col].isnull().any():
            df[col].fillna(df[col].mean(), inplace=True)
            print(f"  Filled missing numerical values in '{col}' with mean.")

    # Example: Handle missing values in categorical columns with mode
    for col in df.select_dtypes(include='object').columns:
        if df[col].isnull().any():
            mode_val = df[col].mode()[0]  # Get the first mode if multiple exist
            df[col].fillna(mode_val, inplace=True)
            print(f"  Filled missing categorical values in '{col}' with mode.")
    
    # Remove duplicate rows
    initial_rows = len(df)
    df.drop_duplicates(inplace=True)
    if len(df) < initial_rows:
        print(f"  Removed {initial_rows - len(df)} duplicate rows.")

    # Example: Convert a column to datetime if it's a date string
    # Assuming 'OrderDate' or similar might exist, adapt to your dataset
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        if df['Date'].isnull().any():
            print("  Warning: Some 'Date' values could not be converted to datetime.")
        else:
            print("  Converted 'Date' to datetime format.")
    
    # Example: Create a new feature (adjust based on your dataset's columns)
    # If your dataset has columns like 'Sales' and 'Profit', you could do:
    # if 'Sales' in df.columns and 'Profit' in df.columns:
    #     df['ProfitMargin'] = (df['Profit'] / df['Sales']) * 100
    #     print("  Created 'ProfitMargin' column.")

    print("Data transformation complete.")
    return df

# --- 3. Loading ---

def load_data(df, db_url, table_name):
    """
    Loads the transformed data into a specified database table.

    Args:
        df (pd.DataFrame): The DataFrame to load.
        db_url (str): The SQLAlchemy database connection string.
        table_name (str): The name of the table to load data into.
    """
    if df is None:
        print("No data to load.")
        return

    try:
        engine = create_engine(db_url)
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"Successfully loaded data into table: {table_name} in {db_url}")
    except Exception as e:
        print(f"An error occurred during loading data to database: {e}")

# --- ETL Pipeline Execution ---

def run_etl_pipeline(data_source_url, db_url, table_name):
    """
    Runs the complete ETL pipeline using a URL as the data source.

    Args:
        data_source_url (str): URL to the source data file (e.g., CSV).
        db_url (str): The SQLAlchemy database connection string.
        table_name (str): The name of the target database table.
    """
    print(f"\nETL pipeline started at {time.ctime()}...")
    
    # Extract
    raw_data = extract_data_from_url(data_source_url)
    if raw_data is None:
        print("ETL pipeline aborted due to extraction errors.")
        return

    # Transform
    transformed_data = transform_data(raw_data.copy())
    if transformed_data is None:
        print("ETL pipeline aborted due to transformation errors.")
        return

    # Load
    load_data(transformed_data, db_url, table_name)
    
    print(f"ETL pipeline finished at {time.ctime()}.")

# --- Automation with schedule library ---

def schedule_etl_pipeline(data_source_url, db_url, table_name, schedule_time):
    """
    Schedules the ETL pipeline to run daily at a specified time.

    Args:
        data_source_url (str): URL to the source data file (e.g., CSV).
        db_url (str): The SQLAlchemy database connection string.
        table_name (str): The name of the target database table.
        schedule_time (str): The time of day to run the pipeline (e.g., "10:30").
    """
    # Schedule the ETL pipeline to run daily at the specified time
    schedule.every().day.at(schedule_time).do(run_etl_pipeline, data_source_url, db_url, table_name)
    print(f"ETL pipeline scheduled to run daily at {schedule_time}.")

    while True:
        schedule.run_pending()
        time.sleep(1) # Check for pending jobs every second
    try:
        while True:
            schedule.run_pending()
            time.sleep(1) # Check for pending jobs every second
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt detected. Shutting down scheduler gracefully...")
        # You can add any cleanup code here that needs to run before the script exits
        # For example, closing connections, saving state, etc.
        sys.exit(0) # Exit cleanly

# --- Example Usage (using a real dataset) ---
if __name__ == "__main__":
    # Define parameters for the ETL pipeline
    # Example using a dataset from data.gov (Adjust URL based on the dataset you choose)
    # Search Result mentions using a World Bank dataset from Kaggle.
    # Searching for \"world bank dataset kaggle\" led to this.
    # Dataset: World Happiness Report (2015-2019) from Kaggle
    # You'll need to find a direct link to the CSV file or download it and store it locally
    # For a public dataset, we'll try to find a direct CSV link.
    # Let's use a simpler, directly accessible CSV example:
    # Example: World Health Organization (WHO) mortality dataset
    # (Note: This URL might change or become unavailable. Always verify the source.)
    # The below CSV includes columns like 'Year', 'Country', 'Sex', 'AgeGroup', 'Deaths'
    public_data_url = "https://raw.githubusercontent.com/datasets/who-mortality-database/main/data/mortality_data.csv"
    destination_db_url = "sqlite:///health_data.db"  # Using SQLite for simplicity
    target_table = "processed_health_stats"
    daily_run_time = "06:40"  # Schedule to run daily at 6:40 AM GMT+5:30

    # Run the ETL pipeline
    schedule_etl_pipeline(public_data_url, destination_db_url, target_table, daily_run_time)

    # To verify the loaded data (optional, for debugging/testing purposes)
    # This part won't execute if the script runs indefinitely for scheduling,
    # unless you stop it and then run this section separately, or run it once.
    try:
        engine = create_engine(destination_db_url)
        loaded_df = pd.read_sql_table(target_table, con=engine)
        print("\nData loaded into the database:")
        print(loaded_df.head())
        print(loaded_df.info())
    except Exception as e:
        print(f"Error verifying data in database: {e}")
