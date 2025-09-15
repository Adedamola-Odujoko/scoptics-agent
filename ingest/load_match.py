import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pydantic import ValidationError

# We need to add the src directory to the Python path to allow imports from our package.
# This is a common pattern when running scripts from a project's sub-directory.
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from scoptics_agent.schemas import TrackingRecord

def ingest_tracking_data(file_path: str):
    """
    Reads tracking data from a CSV, validates it, and loads it into the database.
    """
    print(f"Starting ingestion for file: {file_path}")

# 1. Load Database Configuration from .env file
    # --------------------------------------------------------------------------
    # Find the project's root directory (which is one level up from the 'ingest' directory)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dotenv_path = os.path.join(project_root, '.env')
    
    # Explicitly load the .env file from the project root
    load_dotenv(dotenv_path=dotenv_path)
    
    DATABASE_URL = os.getenv("DATABASE_URL")

    # 2. Read and Prepare Data with Pandas
    # --------------------------------------------------------------------------
    try:
        # We replace empty strings with None, which Pydantic and the DB handle correctly.
        df = pd.read_csv(file_path).replace({float('nan'): None})
        # Convert the DataFrame to a list of dictionaries, which is easy to work with.
        raw_records = df.to_dict(orient='records')
        print(f"Successfully read {len(raw_records)} records from CSV.")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return

    # 3. Validate Every Record with Pydantic
    # --------------------------------------------------------------------------
    validated_records = []
    for i, raw_record in enumerate(raw_records):
        try:
            # Here is where our Pydantic schema does its magic.
            # It will raise a ValidationError if any data is malformed.
            validated_record = TrackingRecord(**raw_record)
            # Pydantic models have a `.model_dump()` method to convert them back to dicts.
            validated_records.append(validated_record.model_dump())
        except ValidationError as e:
            print(f"Validation Error in row {i+2}: {e}")
            # In a real system, you might log these errors to a file instead of stopping.
            return
    print(f"Successfully validated {len(validated_records)} records.")

    # 4. Connect to the Database and Insert Data
    # --------------------------------------------------------------------------
    if not validated_records:
        print("No valid records to insert. Exiting.")
        return

    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            # For safety, let's clear any old data for this match before inserting.
            match_id = validated_records[0]['match_id']
            print(f"Deleting existing data for match_id: {match_id}...")
            connection.execute(text(f"DELETE FROM tracking WHERE match_id = :match_id"), {'match_id': match_id})
            
            print(f"Inserting {len(validated_records)} new records...")
            # SQLAlchemy's Core `insert` is very efficient for bulk operations.
            connection.execute(text("""
                INSERT INTO tracking (match_id, frame, timestamp_iso, team_id, player_id, x, y, z, speed, orientation)
                VALUES (:match_id, :frame, :timestamp_iso, :team_id, :player_id, :x, :y, :z, :speed, :orientation)
            """), validated_records)
            
            connection.commit() # Finalize the transaction
            print("Insertion complete. Data committed to the database.")

    except Exception as e:
        print(f"Database Error: {e}")

if __name__ == "__main__":
    # This allows us to run the script from the command line with a file path argument.
    if len(sys.argv) > 1:
        file_to_ingest = sys.argv[1]
        ingest_tracking_data(file_to_ingest)
    else:
        print("Please provide the path to the CSV file to ingest.")
        print("Usage: python ingest/load_match.py <path_to_csv>")