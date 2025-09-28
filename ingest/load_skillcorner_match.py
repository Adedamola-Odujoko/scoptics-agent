import os
import sys
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pydantic import ValidationError

# Ensure the script can find the source directory for schemas
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from scoptics_agent.schemas import TrackingFrame, MatchMetadata, TrackedObject

def parse_time_string(time_str: str) -> timedelta:
    """
    Robustly parses a time string that could be in H:M:S, M:S, or S format.
    """
    if not time_str:
        return timedelta(0)
    try:
        parts = list(map(float, time_str.split(':')))
        if len(parts) == 3:
            return timedelta(hours=parts[0], minutes=parts[1], seconds=parts[2])
        elif len(parts) == 2:
            return timedelta(minutes=parts[0], seconds=parts[1])
        elif len(parts) == 1:
            return timedelta(seconds=parts[0])
        else:
            return timedelta(0)
    except (ValueError, IndexError):
        # Return zero if format is unexpected or invalid
        return timedelta(0)

def load_skillcorner_match(skillcorner_repo_path: str, match_id: int):
    """
    Loads all data for a given match from the SkillCorner open data repository
    into the database, including match metadata, player roster, and tracking data.
    """
    print("="*60)
    print(f"Starting SkillCorner ingestion for Match ID: {match_id}")
    print("="*60)

    # --- 1. Database Connection Setup ---
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dotenv_path = os.path.join(project_root, '.env')
    load_dotenv(dotenv_path=dotenv_path)
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable not set.")
    engine = create_engine(DATABASE_URL)

    # --- 2. File Path and Existence Check ---
    base_data_path = os.path.join(skillcorner_repo_path, 'data')
    meta_file = os.path.join(base_data_path, 'matches', str(match_id), 'match_data.json')
    data_file = os.path.join(base_data_path, 'matches', str(match_id), 'structured_data.json')
    
    if not (os.path.exists(meta_file) and os.path.exists(data_file)):
        print(f"Error: Could not find data or metadata file for match {match_id}. Aborting.")
        return

    with open(meta_file, 'r') as f: meta_data = json.load(f)
    with open(data_file, 'r') as f: tracking_frames_raw = json.load(f)

    with engine.connect() as connection:
        # --- 3. Clean Existing Data (Idempotency) ---
        print("Clearing any existing data for this match...")
        connection.execute(text("DELETE FROM tracking_data WHERE match_id = :match_id"), {'match_id': str(match_id)})
        connection.execute(text("DELETE FROM players WHERE match_id = :match_id"), {'match_id': str(match_id)})
        connection.execute(text("DELETE FROM match_metadata WHERE match_id = :match_id"), {'match_id': str(match_id)})
        
        # --- 4. Ingest Match Metadata ---
        print("Processing and inserting match metadata...")
        pitch_size = meta_data.get('pitch_size', [105, 68])
        metadata_to_insert = MatchMetadata(
            match_id=str(match_id),
            competition_name=meta_data.get('competition'),
            home_team_name=meta_data.get('home_team', {}).get('name'),
            away_team_name=meta_data.get('away_team', {}).get('name'),
            pitch_length_m=pitch_size[0],
            pitch_width_m=pitch_size[1]
        ).model_dump()

        connection.execute(text("""
            INSERT INTO match_metadata (match_id, competition_name, home_team_name, away_team_name, pitch_length_m, pitch_width_m)
            VALUES (:match_id, :competition_name, :home_team_name, :away_team_name, :pitch_length_m, :pitch_width_m)
        """), [metadata_to_insert])

        # --- 5. Ingest Player Roster (THE CRITICAL NEW STEP) ---
        print("Processing and inserting player metadata...")
        players_to_insert = []
        for player_data in meta_data.get('players', []):
            try:
                # Ensure the core identifier is present
                if 'trackable_object' in player_data:
                    players_to_insert.append({
                        "match_id": str(match_id),
                        "trackable_object": str(player_data['trackable_object']),
                        "first_name": player_data.get('first_name'),
                        "last_name": player_data.get('last_name'),
                        "number": player_data.get('number'),
                        "player_role": player_data.get('player_role', {}).get('name')
                    })
            except KeyError as e:
                print(f"Skipping a player record due to missing key: {e}")
                continue
        
        if players_to_insert:
            ins_player_query = text("""
                INSERT INTO players (match_id, trackable_object, first_name, last_name, "number", player_role)
                VALUES (:match_id, :trackable_object, :first_name, :last_name, :number, :player_role)
            """)
            connection.execute(ins_player_query, players_to_insert)
            print(f"Successfully inserted {len(players_to_insert)} player records.")

        connection.commit()

    # --- 6. Transform and Ingest Tracking Data ---
    print(f"Found {len(tracking_frames_raw)} raw frames. Transforming and validating...")
    records_to_insert = []
    base_date = datetime(1970, 1, 1)

    for frame_data in tracking_frames_raw:
        try:
            # Basic validation for required fields
            if frame_data.get('time') is None or frame_data.get('period') is None:
                continue

            time_delta = parse_time_string(frame_data['time'])
            frame_datetime = base_date + time_delta
            
            # Use Pydantic models for validation and structure
            frame_model = TrackingFrame(
                match_id=str(match_id),
                period=frame_data['period'],
                frame=frame_data['frame'],
                timestamp_iso=frame_datetime,
                tracked_objects=[TrackedObject(**obj) for obj in frame_data.get('data', [])],
                frame_metadata={"possession": frame_data.get("possession")}
            )
            records_to_insert.append(frame_model.model_dump())
        except (ValidationError, KeyError, AttributeError, ValueError) as e:
            print(f"Skipping frame {frame_data.get('frame')} due to data error: {e}")
            continue

    if not records_to_insert:
        print("No valid frame records to insert after cleaning. Exiting.")
        return

    # --- 7. Efficient Bulk Insert in Chunks ---
    print(f"Preparing to insert {len(records_to_insert)} valid frame records into 'tracking_data'...")
    with engine.connect() as connection:
        for record in records_to_insert:
            # Serialize JSON columns to strings for the database driver
            record['tracked_objects'] = json.dumps(record['tracked_objects'])
            record['frame_metadata'] = json.dumps(record['frame_metadata'])
        
        ins_query = text("""
            INSERT INTO tracking_data (match_id, period, frame, timestamp_iso, tracked_objects, frame_metadata)
            VALUES (:match_id, :period, :frame, :timestamp_iso, :tracked_objects, :frame_metadata)
        """)
        
        chunk_size = 2000
        for i in range(0, len(records_to_insert), chunk_size):
            chunk = records_to_insert[i:i + chunk_size]
            connection.execute(ins_query, chunk)
        
        connection.commit()
    
    print(f"Successfully saved tracking data for match {match_id}.")
    print("\n" + "="*60); print("SkillCorner ingestion complete!"); print("="*60)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python ingest/load_skillcorner_match.py <path_to_skillcorner_repo> <match_id>")
        print("Example: python ingest/load_skillcorner_match.py ../opendata 4039")
    else:
        repo_path = sys.argv[1]
        match_id_arg = int(sys.argv[2])
        load_skillcorner_match(repo_path, match_id_arg)