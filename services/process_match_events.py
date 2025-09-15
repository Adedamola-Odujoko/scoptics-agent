import os
import sys
import pandas as pd
import uuid
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pydantic import ValidationError

# Add the 'src' directory to the Python path to allow our package imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from scoptics_agent.schemas import Event
from scoptics_agent.events.detectors import detect_2v1_in_final_third
from scoptics_agent.events.clustering import cluster_frames_into_events

def process_match(match_id: str, attacking_team_id: str):
    """
    Loads a match's tracking data, detects and clusters events,
    and saves them to the database.
    """
    print("="*50)
    print(f"Starting event processing for Match ID: {match_id}")
    print(f"Analyzing attacks for Team ID: {attacking_team_id}")
    print("="*50)

    # 1. Load Configuration
    # --------------------------------------------------------------------------
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dotenv_path = os.path.join(project_root, '.env')
    load_dotenv(dotenv_path=dotenv_path)
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable not set.")

    engine = create_engine(DATABASE_URL)

    try:
        # 2. Load Tracking Data from Database
        # --------------------------------------------------------------------------
        print(f"Loading tracking data for match '{match_id}' from database...")
        query = text("SELECT * FROM tracking WHERE match_id = :match_id")
        with engine.connect() as connection:
            tracking_df = pd.read_sql(query, connection, params={'match_id': match_id})
        
        if tracking_df.empty:
            print(f"Error: No tracking data found for match_id '{match_id}'. Aborting.")
            return
        print(f"Successfully loaded {len(tracking_df)} tracking records.")

        # 3. Run Event Detector
        # --------------------------------------------------------------------------
        print("Running '2v1 in Final Third' detector...")
        detector_params = {"pitch_length": 105, "local_radius": 10}
        frame_detections = detect_2v1_in_final_third(
            tracking_df, 
            attacking_team_id=attacking_team_id, 
            params=detector_params
        )
        print(f"Found {len(frame_detections)} raw frame-level detections.")

        # 4. Cluster Detections into Events
        # --------------------------------------------------------------------------
        if not frame_detections:
            print("No frame detections to cluster. Process finished.")
            return

        print("Clustering frame detections into continuous events...")
        # A 10-frame gap is about 0.4 seconds at 25fps - a reasonable threshold.
        clustered_events = cluster_frames_into_events(frame_detections, max_frame_gap=10)
        print(f"Successfully clustered into {len(clustered_events)} distinct events.")

        # 5. Prepare and Validate Events for Database Insertion
        # --------------------------------------------------------------------------
        events_to_insert = []
        event_type = "2v1_final_third"
        for event_data in clustered_events:
            try:
                # Enrich the clustered data with more details for the DB
                full_event_data = {
                    "event_id": uuid.uuid4(),
                    "match_id": event_data['match_id'],
                    "event_type": event_type,
                    "start_time": event_data['start_time'],
                    "end_time": event_data['end_time'],
                    "start_frame": event_data['start_frame'],
                    "end_frame": event_data['end_frame'],
                    "team_id": attacking_team_id,
                    # NOTE: A more advanced detector would identify the specific players.
                    # For now, we'll use a placeholder.
                    "players_involved": [],
                    "metadata_json": {
                        "detection_source": "rule_based_v1",
                        "frame_count": event_data['frame_count']
                    }
                }
                # Validate the final object against our Pydantic schema
                event_model = Event(**full_event_data)
                events_to_insert.append(event_model.model_dump())
            except ValidationError as e:
                print(f"Data validation error for an event: {e}")
                continue # Skip this event and continue
        
        # 6. Save Events to Database
        # --------------------------------------------------------------------------
        if not events_to_insert:
            print("No valid events to insert after validation. Exiting.")
            return

        print(f"Preparing to insert {len(events_to_insert)} events into the database...")
        with engine.connect() as connection:
            # First, delete any old events of this type for this match to avoid duplicates
            del_query = text("""
                DELETE FROM events WHERE match_id = :match_id AND event_type = :event_type
            """)
            connection.execute(del_query, {'match_id': match_id, 'event_type': event_type})

            # Now, insert the new events
            ins_query = text("""
                INSERT INTO events (event_id, match_id, event_type, start_time, end_time, 
                                    start_frame, end_frame, team_id, players_involved, metadata_json)
                VALUES (:event_id, :match_id, :event_type, :start_time, :end_time, 
                        :start_frame, :end_frame, :team_id, :players_involved, :metadata_json)
            """)
            connection.execute(ins_query, events_to_insert)
            connection.commit()
        
        print("Successfully saved events to the database!")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    # This allows us to run the script from the command line, e.g.:
    # python services/process_match_events.py match_001 team_A
    if len(sys.argv) != 3:
        print("Usage: python services/process_match_events.py <match_id> <attacking_team_id>")
    else:
        match_id_arg = sys.argv[1]
        team_id_arg = sys.argv[2]
        process_match(match_id=match_id_arg, attacking_team_id=team_id_arg)