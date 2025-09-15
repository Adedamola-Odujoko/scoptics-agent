import os
import sys
import pandas as pd
import weaviate
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

# NEW: Import the specific configuration classes we need for the schema
from weaviate.classes.config import Property, DataType

# Add the 'src' directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

def setup_weaviate_schema(client: weaviate.WeaviateClient):
    """
    Ensures the 'Event' collection schema exists in Weaviate.
    """
    collection_name = "Event"
    if client.collections.exists(collection_name):
        print(f"Schema for collection '{collection_name}' already exists. Skipping creation.")
        return

    print(f"Creating schema for collection '{collection_name}'...")
    client.collections.create(
        name=collection_name,
        description="A tactical football event from a match.",
        vectorizer_config=None,
        properties=[
            Property(name="eventId", data_type=DataType.UUID),
            Property(name="matchId", data_type=DataType.TEXT),
            Property(name="eventType", data_type=DataType.TEXT),
            Property(name="teamId", data_type=DataType.TEXT),
            Property(name="startTime", data_type=DataType.DATE),
        ]
    )
    print("Schema created successfully.")

def process_embeddings():
    """
    Loads events from Postgres, generates embeddings, and saves them to Weaviate.
    """
    print("="*50)
    print("Starting Embedding Processor")
    print("="*50)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dotenv_path = os.path.join(project_root, '.env')
    load_dotenv(dotenv_path=dotenv_path)
    DATABASE_URL = os.getenv("DATABASE_URL")

    weaviate_client = weaviate.connect_to_local()
    
    try:
        setup_weaviate_schema(weaviate_client)

        print("Loading sentence-transformer model (all-MiniLM-L6-v2)...")
        model = SentenceTransformer('all-MiniLM-L6-v2')
        print("Model loaded.")

        print("Loading events from PostgreSQL database...")
        engine = create_engine(DATABASE_URL)
        try:
            with engine.connect() as connection:
                events_df = pd.read_sql("SELECT * FROM events", connection)
            if events_df.empty:
                print("No events found in the database. Exiting.")
                return
            print(f"Found {len(events_df)} events to process.")
        except Exception as e:
            print(f"Database Error: Could not load events. {e}")
            return

        print("Generating embeddings and preparing for batch import...")
        events_collection = weaviate_client.collections.get("Event")

        with events_collection.batch.dynamic() as batch:
            for index, event in events_df.iterrows():
                text_summary = (
                    f"A {event['event_type']} event "
                    f"by team {event['team_id']} "
                    f"in match {event['match_id']}."
                )
                
                embedding = model.encode(text_summary)
                
                properties = {
                    "eventId": str(event['event_id']),
                    "matchId": event['match_id'],
                    "eventType": event['event_type'],
                    "teamId": event['team_id'],
                    "startTime": event['start_time'],
                }
                
                batch.add_object(
                    properties=properties,
                    vector=embedding
                )
        print("Batch import complete. Embeddings are now stored in Weaviate.")

    finally:
        weaviate_client.close()


if __name__ == "__main__":
    process_embeddings()