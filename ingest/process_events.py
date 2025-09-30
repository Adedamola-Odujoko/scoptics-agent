import os
import sys
import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from scipy.signal import find_peaks

# --- Helper Functions (largely unchanged) ---

def get_ball_trackable_id(match_id: str, engine) -> str:
    # ... (no changes needed in this function)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    meta_file = os.path.join(project_root, '..', 'opendata', 'data', 'matches', match_id, 'match_data.json')
    if not os.path.exists(meta_file): raise FileNotFoundError(f"Could not find match_data.json for match {match_id}")
    with open(meta_file, 'r') as f: meta_data = json.load(f)
    ball_id = meta_data.get('ball', {}).get('trackable_object')
    if not ball_id: raise ValueError(f"Ball trackable_object ID not found in metadata for match {match_id}")
    return str(ball_id)


def get_player_team_mapping(match_id: str) -> dict:
    # ... (no changes needed in this function)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    meta_file = os.path.join(project_root, '..', 'opendata', 'data', 'matches', match_id, 'match_data.json')
    if not os.path.exists(meta_file): raise FileNotFoundError(f"Could not find match_data.json for match {match_id}")
    with open(meta_file, 'r') as f: meta_data = json.load(f)
    home_team_id, home_team_name = meta_data['home_team']['id'], meta_data['home_team']['name']
    away_team_id, away_team_name = meta_data['away_team']['id'], meta_data['away_team']['name']
    team_id_to_name_map = {home_team_id: home_team_name, away_team_id: away_team_name}
    players = meta_data.get('players')
    if not players: raise ValueError("Could not find 'players' in match_data.json")
    mapping = {str(p['trackable_object']): team_id_to_name_map.get(p['team_id']) for p in players if 'trackable_object' in p and 'team_id' in p}
    return mapping


def get_location_score(x, y, pitch_length, pitch_width):
    # ... (no changes needed in this function)
    abs_x, abs_y = abs(x), abs(y)
    goal_line_x = pitch_length / 2
    if (goal_line_x - abs_x) < 16.5 and abs_y < 20.16: return 1.0
    if (goal_line_x - abs_x) < 20.0 and abs_y < 10.0: return 0.8
    if (goal_line_x - abs_x) < 30.0 and abs_y > 20.16: return 0.3
    if abs_x > 0: return max(0, 0.5 * (1 - ((goal_line_x - abs_x) / (pitch_length / 2))))
    return 0.0


def is_point_in_triangle(p, a, b, c):
    # ... (no changes needed in this function)
    v0, v1, v2 = c - a, b - a, p - a
    dot00, dot01, dot02 = np.dot(v0, v0), np.dot(v0, v1), np.dot(v0, v2)
    dot11, dot12 = np.dot(v1, v1), np.dot(v1, v2)
    inv_denom = 1 / (dot00 * dot11 - dot01 * dot01) if (dot00 * dot11 - dot01 * dot01) != 0 else 0
    if inv_denom == 0: return False
    u = (dot11 * dot02 - dot01 * dot12) * inv_denom
    v = (dot00 * dot12 - dot01 * dot02) * inv_denom
    return (u >= 0) and (v >= 0) and (u + v < 1)


def get_player_positions_at_frame(df_players_tracking, frame, team_name, pitch_length):
    # ... (no changes needed in this function)
    players_at_frame = df_players_tracking[df_players_tracking['frame'] == frame].copy()
    opponents = players_at_frame[players_at_frame['team_name'] != team_name]
    if opponents.empty: return [], None
    goal_x = pitch_length / 2 if opponents['x'].mean() > 0 else -pitch_length / 2
    opponents['dist_to_goal'] = np.sqrt((opponents['x'] - goal_x)**2 + opponents['y']**2)
    goalkeeper = opponents.loc[opponents['dist_to_goal'].idxmin()]
    defenders = [pos for _, pos in opponents.iterrows() if pos.name != goalkeeper.name]
    return defenders, goalkeeper


def calculate_advanced_xg(shot_x, shot_y, defender_positions, goalkeeper_position, pitch_length):
    # ... (no changes needed in this function)
    GOAL_WIDTH = 7.32
    goal_x_pos = pitch_length / 2 if shot_x > 0 else -pitch_length / 2
    shot_loc = np.array([shot_x, shot_y])
    lp, rp = np.array([goal_x_pos, GOAL_WIDTH / 2]), np.array([goal_x_pos, -GOAL_WIDTH / 2])
    v_l, v_r = lp - shot_loc, rp - shot_loc
    if np.linalg.norm(v_l) == 0 or np.linalg.norm(v_r) == 0: return 0.01
    angle = np.arccos(np.dot(v_l, v_r) / (np.linalg.norm(v_l) * np.linalg.norm(v_r)))
    dist = np.linalg.norm(shot_loc - np.array([goal_x_pos, 0]))
    pressure = sum(1 for p in defender_positions if is_point_in_triangle(np.array([p['x'], p['y']]), shot_loc, lp, rp))
    gk_factor = 0
    if goalkeeper_position is not None and not goalkeeper_position.empty:
        shot_line = np.array([goal_x_pos, 0]) - shot_loc
        if np.linalg.norm(shot_line) == 0: return 0.01
        shot_line_unit = shot_line / np.linalg.norm(shot_line)
        gk_vec = np.array([goalkeeper_position['x'], goalkeeper_position['y']]) - shot_loc
        proj = np.dot(gk_vec, shot_line_unit)
        dist_from_line = np.linalg.norm(gk_vec - proj * shot_line_unit)
        if dist_from_line < 1.5: gk_factor = -0.5
    z = -1.5 + (1.2 * angle) + (-0.08 * dist) + (-0.3 * pressure) + gk_factor
    return round(1 / (1 + np.exp(-z)), 3)


def is_big_chance(xg_value):
    # ... (no changes needed in this function)
    return xg_value > 0.35


# --- Main Event Processing Functions ---

def setup_database(engine):
    """
    MODIFIED: Drops and creates all event tables with new TIMESTAMPTZ columns.
    """
    print("Setting up database tables for all events...")
    with engine.connect() as connection:
        connection.execute(text("DROP TABLE IF EXISTS possession_spells CASCADE;"))
        connection.execute(text("DROP TABLE IF EXISTS passes CASCADE;"))
        connection.execute(text("DROP TABLE IF EXISTS shots CASCADE;"))
        connection.execute(text("DROP TABLE IF EXISTS big_chances CASCADE;"))

        # MODIFIED: Added start_timestamp and end_timestamp
        connection.execute(text("""
            CREATE TABLE possession_spells (
                match_id TEXT,
                spell_id TEXT PRIMARY KEY,
                period INT,
                start_frame INT,
                end_frame INT,
                start_timestamp TIMESTAMPTZ,
                end_timestamp TIMESTAMPTZ,
                duration_seconds FLOAT,
                possessing_team TEXT,
                possessing_player_id TEXT
            );
        """))
        # MODIFIED: Added timestamp
        connection.execute(text("""
            CREATE TABLE passes (
                pass_id TEXT PRIMARY KEY,
                match_id TEXT,
                period INT,
                start_frame INT,
                end_frame INT,
                start_timestamp TIMESTAMPTZ,
                end_timestamp TIMESTAMPTZ,
                passer_id TEXT,
                receiver_id TEXT,
                passer_team TEXT,
                start_x FLOAT,
                start_y FLOAT,
                end_x FLOAT,
                end_y FLOAT,
                distance_m FLOAT,
                outcome TEXT
            );
        """))
        # MODIFIED: Added timestamp
        connection.execute(text("""
            CREATE TABLE shots (
                shot_id TEXT PRIMARY KEY,
                match_id TEXT,
                period INT,
                frame INT,
                "timestamp" TIMESTAMPTZ,
                game_minute INT,
                game_second INT,
                shooter_id TEXT,
                shooter_team TEXT,
                start_x FLOAT,
                start_y FLOAT,
                outcome TEXT,
                xg FLOAT
            );
        """))
        # MODIFIED: Added timestamp
        connection.execute(text("""
            CREATE TABLE big_chances (
                big_chance_id TEXT PRIMARY KEY,
                match_id TEXT,
                period INT,
                frame INT,
                "timestamp" TIMESTAMPTZ,
                player_id TEXT,
                team_name TEXT,
                x FLOAT,
                y FLOAT,
                num_defenders_between INT
            );
        """))
        connection.commit()
    print("Database setup complete.")

def calculate_possession_spells(df_tracking, ball_id, engine):
    """
    MODIFIED: Now saves the start and end timestamps of each spell to the database.
    """
    print("Processing possession spells...")
    df_ball = df_tracking[df_tracking['trackable_object'] == ball_id][['frame', 'x', 'y']].rename(columns={'x': 'ball_x', 'y': 'ball_y'})
    df_players = df_tracking[df_tracking['team_name'].notna()].copy()
    df_merged = pd.merge(df_players, df_ball, on='frame', how='inner')
    df_merged['distance_to_ball'] = np.sqrt((df_merged['x'] - df_merged['ball_x'])**2 + (df_merged['y'] - df_merged['ball_y'])**2)
    
    idx = df_merged.groupby('frame')['distance_to_ball'].idxmin()
    df_closest = df_merged.loc[idx].copy()
    df_possession = df_closest[df_closest['distance_to_ball'] < 2.5].sort_values('frame')
    df_possession['is_new_spell'] = (df_possession['trackable_object'] != df_possession['trackable_object'].shift(1))
    df_possession['spell_group_id'] = df_possession['is_new_spell'].cumsum()
    
    df_spells = df_possession.groupby('spell_group_id').agg(
        match_id=('match_id', 'first'),
        period=('period', 'first'),
        start_frame=('frame', 'min'),
        end_frame=('frame', 'max'),
        start_timestamp=('timestamp', 'min'), # MODIFIED: Renamed from start_time
        end_timestamp=('timestamp', 'max'),   # MODIFIED: Renamed from end_time
        possessing_team=('team_name', 'first'),
        possessing_player_id=('trackable_object', 'first')
    ).reset_index(drop=True)

    if df_spells.empty:
        print("  -> No possession spells found.")
        return pd.DataFrame()

    df_spells['duration_seconds'] = (df_spells['end_timestamp'] - df_spells['start_timestamp']).dt.total_seconds()
    df_final_spells = df_spells[df_spells['duration_seconds'] > 0.05].copy()

    if not df_final_spells.empty:
        df_final_spells['spell_id'] = df_final_spells.apply(lambda row: f"{row['match_id']}-{row['period']}-{row['start_frame']}", axis=1)
        
        # MODIFIED: Added timestamp columns to the list for insertion
        columns_to_insert = [
            'match_id', 'spell_id', 'period', 'start_frame', 'end_frame',
            'start_timestamp', 'end_timestamp', 'duration_seconds',
            'possessing_team', 'possessing_player_id'
        ]
        df_final_spells[columns_to_insert].to_sql('possession_spells', engine, if_exists='append', index=False)
        print(f"  -> Saved {len(df_final_spells)} possession spells.")

    return df_final_spells

def identify_passes(df_spells, df_tracking, ball_id, engine):
    """
    MODIFIED: This function is completely reworked to store the full context of a pass.
    """
    print("Identifying passes...")
    if df_spells.empty or len(df_spells) < 2:
        print("  -> Not enough possession spells to analyze, skipping pass identification.")
        return

    # Create pairs of consecutive spells to represent potential passes
    df_spells_shifted = df_spells.shift(-1)
    df_pass_events = df_spells.join(df_spells_shifted, rsuffix='_next')
    
    # Filter for valid pass events: same team, different player, short time gap
    df_pass_events = df_pass_events[df_pass_events['possessing_team'] == df_pass_events['possessing_team_next']]
    df_pass_events = df_pass_events[df_pass_events['possessing_player_id'] != df_pass_events['possessing_player_id_next']]
    time_diff = (df_pass_events['start_timestamp_next'] - df_pass_events['end_timestamp']).dt.total_seconds()
    df_pass_events = df_pass_events[(time_diff > 0.05) & (time_diff < 3.0)]

    if df_pass_events.empty:
        print("  -> No valid pass events found.")
        return

    # Get ball positions for all frames to join against
    df_ball_pos = df_tracking[df_tracking['trackable_object'] == ball_id][['frame', 'x', 'y']].set_index('frame')

    # Join to get the ball's start position (at the end of the passer's spell)
    df_passes = df_pass_events.join(df_ball_pos, on='end_frame')
    df_passes.rename(columns={'x': 'start_x', 'y': 'start_y'}, inplace=True)

    # Join to get the ball's end position (at the start of the receiver's spell)
    df_passes = df_passes.join(df_ball_pos, on='start_frame_next', rsuffix='_end')
    df_passes.rename(columns={'x': 'end_x', 'y': 'end_y'}, inplace=True)
    
    df_passes.dropna(subset=['start_x', 'start_y', 'end_x', 'end_y'], inplace=True)
    
    # NEW: Calculate distance based on the new start and end coordinates
    df_passes['distance_m'] = np.sqrt(
        (df_passes['end_x'] - df_passes['start_x'])**2 +
        (df_passes['end_y'] - df_passes['start_y'])**2
    )

    df_passes['pass_id'] = df_passes.apply(lambda row: f"pass-{row['match_id']}-{row['end_frame']}", axis=1)

    # NEW: Assemble the final DataFrame with the new, enriched schema
    df_to_insert = pd.DataFrame({
        'pass_id': df_passes['pass_id'],
        'match_id': df_passes['match_id'],
        'period': df_passes['period'],
        'start_frame': df_passes['end_frame'], # Pass starts at the end of the first spell
        'end_frame': df_passes['start_frame_next'], # Pass ends at the start of the next spell
        'start_timestamp': df_passes['end_timestamp'],
        'end_timestamp': df_passes['start_timestamp_next'],
        'passer_id': df_passes['possessing_player_id'],
        'receiver_id': df_passes['possessing_player_id_next'],
        'passer_team': df_passes['possessing_team'],
        'start_x': df_passes['start_x'],
        'start_y': df_passes['start_y'],
        'end_x': df_passes['end_x'],
        'end_y': df_passes['end_y'],
        'distance_m': df_passes['distance_m'],
        'outcome': 'Completed' # Current heuristic only finds completed passes
    })

    if not df_to_insert.empty:
        df_to_insert.to_sql('passes', engine, if_exists='append', index=False)
        print(f"  -> Saved {len(df_to_insert)} enriched passes.")
    else:
        print("  -> No passes found after enrichment.")

def identify_big_chances(df_spells, df_players, home_team_name, pitch_length, engine):
    """
    MODIFIED: Now includes the exact timestamp of when the big chance occurred.
    """
    print("Identifying big chances from possession spells...")
    if df_spells.empty:
        print("  -> No spells to analyze for big chances."); return
    
    big_chances = []
    for _, spell in df_spells.iterrows():
        end_frame_player = df_players[(df_players['frame'] == spell['end_frame']) & (df_players['trackable_object'] == spell['possessing_player_id'])]
        if end_frame_player.empty: continue
        
        player_pos = end_frame_player.iloc[0]
        is_home_team = player_pos['team_name'] == home_team_name
        attacking_goal_x = pitch_length / 2 if (player_pos['period'] == 1 and is_home_team) or (player_pos['period'] == 2 and not is_home_team) else -pitch_length / 2
        
        if (attacking_goal_x > 0 and player_pos['x'] < 0) or (attacking_goal_x < 0 and player_pos['x'] > 0): continue
        if abs(pitch_length / 2 - abs(player_pos['x'])) > 20 or abs(player_pos['y']) > 15: continue
        
        defenders_at_frame = df_players[(df_players['frame'] == spell['end_frame']) & (df_players['team_name'] != player_pos['team_name'])]
        lp, rp = np.array([attacking_goal_x, 7.32 / 2]), np.array([attacking_goal_x, -7.32 / 2])
        player_loc = np.array([player_pos['x'], player_pos['y']])
        defenders_between = sum(1 for _, d in defenders_at_frame.iterrows() if is_point_in_triangle(np.array([d['x'], d['y']]), player_loc, lp, rp))
        
        if defenders_between <= 2:
            # MODIFIED: Added timestamp to the record
            big_chances.append({
                'big_chance_id': f"bc-{spell['match_id']}-{spell['end_frame']}",
                'match_id': spell['match_id'],
                'period': spell['period'],
                'frame': spell['end_frame'],
                'timestamp': spell['end_timestamp'], # NEW: The chance occurs at the end of the spell
                'player_id': spell['possessing_player_id'],
                'team_name': spell['possessing_team'],
                'x': player_pos['x'],
                'y': player_pos['y'],
                'num_defenders_between': defenders_between
            })

    if big_chances:
        df_big_chances = pd.DataFrame(big_chances).drop_duplicates(subset=['frame', 'player_id'])
        df_big_chances.to_sql('big_chances', engine, if_exists='append', index=False)
        print(f"  -> Identified and saved {len(df_big_chances)} big chances.")
    else:
        print("  -> No big chances identified.")


def identify_shots_and_calculate_xg(df_tracking, ball_id, home_team_name, pitch_length, match_id_str, engine):
    """
    MODIFIED: Now includes the exact timestamp of when the shot was taken.
    """
    print("Processing shots and calculating xG...")
    # ... (initial part of the function is unchanged until the final loop)
    PITCH_WIDTH = 68.0
    PEAK_DISTANCE_FRAMES, VEL_PEAK_HEIGHT, VEL_PEAK_PROMINENCE, ACCEL_PEAK_HEIGHT, ACCEL_PEAK_PROMINENCE = 8, 4, 2, 10, 5
    MIN_THREAT_SCORE = 30.0
    DEVIATION_CHECK_FRAMES, DEVIATION_THRESHOLD_METERS_SQ, BLOCK_PROXIMITY_METERS_SQ, GOAL_AREA_WIDTH = 8, 1.0**2, 2.5**2, 20.0
    
    df_ball = df_tracking[df_tracking['trackable_object'] == ball_id].sort_values('frame').copy()
    if df_ball.empty: print("  -> No ball tracking data found for shots."); return
    df_ball.set_index('frame', inplace=True, drop=False)
    df_ball['vel_x'] = df_ball['x'].diff() / df_ball['time_delta']
    df_ball['vel_y'] = df_ball['y'].diff() / df_ball['time_delta']
    df_ball['speed_mps'] = np.sqrt(df_ball['vel_x']**2 + df_ball['vel_y']**2)
    smoothing_window = 3
    df_ball['speed_smooth'] = df_ball['speed_mps'].rolling(window=smoothing_window, center=True).mean().fillna(0)
    df_ball['accel_smooth'] = abs((df_ball['speed_smooth'].diff() / df_ball['time_delta']).rolling(window=smoothing_window, center=True).mean().fillna(0))
    df_ball.dropna(subset=['speed_mps', 'vel_x', 'vel_y', 'accel_smooth'], inplace=True)
    
    vel_peaks, _ = find_peaks(df_ball['speed_smooth'], height=VEL_PEAK_HEIGHT, prominence=VEL_PEAK_PROMINENCE, distance=PEAK_DISTANCE_FRAMES)
    accel_peaks, _ = find_peaks(df_ball['accel_smooth'], height=ACCEL_PEAK_HEIGHT, prominence=ACCEL_PEAK_PROMINENCE, distance=PEAK_DISTANCE_FRAMES)
    candidate_frames = np.union1d(df_ball.iloc[vel_peaks].frame.values, df_ball.iloc[accel_peaks].frame.values)
    print(f"  -> Stage 1 (Peak Detection): Found {len(candidate_frames)} potential kick events.")
    
    df_players = df_tracking[df_tracking['trackable_object'] != ball_id].copy()
    all_candidates = []
    for frame in candidate_frames:
        event_data = df_ball.loc[frame]
        if pd.isna(event_data['x']) or pd.isna(event_data['y']): continue
        players_at_frame = df_players[df_players['frame'] == frame].dropna(subset=['x', 'y'])
        if players_at_frame.empty: continue
        min_dist_sq, shooter_row = float('inf'), None
        ball_x, ball_y = event_data['x'], event_data['y']
        for _, player in players_at_frame.iterrows():
            dist_sq = (player['x'] - ball_x)**2 + (player['y'] - ball_y)**2
            if dist_sq < min_dist_sq: min_dist_sq, shooter_row = dist_sq, player
        if shooter_row is None or min_dist_sq > 4.0: continue
        is_shooter_home_team = (shooter_row['team_name'] == home_team_name)
        target_goal_x = pitch_length / 2 if (event_data['period'] == 1 and is_shooter_home_team) or (event_data['period'] == 2 and not is_shooter_home_team) else -pitch_length / 2
        if (target_goal_x > 0 and shooter_row['x'] < 0) or (target_goal_x < 0 and shooter_row['x'] > 0): continue
        power_score = (event_data['speed_mps'] * 5) + (event_data['accel_smooth'] * 1)
        location_score = get_location_score(shooter_row['x'], shooter_row['y'], pitch_length, PITCH_WIDTH)
        threat_score = power_score * location_score
        all_candidates.append({'frame': frame, 'period': int(event_data['period']), 'shooter_team': shooter_row['team_name'], 'player_x': shooter_row['x'], 'player_y': shooter_row['y'], 'shooter_id': shooter_row['trackable_object'], 'vel_x': event_data['vel_x'], 'vel_y': event_data['vel_y'], 'attacking_goal_x': target_goal_x, 'threat_score': threat_score})
    
    if not all_candidates:
        print("  -> No valid candidates found after peak detection."); return
    df_all_candidates = pd.DataFrame(all_candidates)
    df_shot_attempts = df_all_candidates[df_all_candidates['threat_score'] >= MIN_THREAT_SCORE].copy()
    print(f"  -> Stage 2 (Threat Score > {MIN_THREAT_SCORE}): {len(df_shot_attempts)} shot attempts identified.")
    
    final_shots_data = []
    for i, attempt in df_shot_attempts.iterrows():
        outcome = None
        start_frame = int(attempt['frame']) + 1
        end_frame = start_frame + DEVIATION_CHECK_FRAMES
        trajectory_data = df_ball.loc[start_frame:end_frame]
        if len(trajectory_data) > 2:
            initial_pos = trajectory_data.iloc[0][['x', 'y']].values
            initial_vel = attempt[['vel_x', 'vel_y']].values
            time_deltas = (pd.to_datetime(trajectory_data['timestamp_iso']) - pd.to_datetime(trajectory_data.iloc[0]['timestamp_iso'])).dt.total_seconds().values
            predicted_path = initial_pos + np.outer(time_deltas, initial_vel)
            actual_path = trajectory_data[['x', 'y']].values
            deviations_sq = np.sum((actual_path - predicted_path)**2, axis=1)
            if np.max(deviations_sq) > DEVIATION_THRESHOLD_METERS_SQ:
                max_deviation_frame = trajectory_data.index[np.argmax(deviations_sq)]
                defenders_at_block = df_players[(df_players['frame'] == max_deviation_frame) & (df_players['team_name'] != attempt['shooter_team'])].dropna(subset=['x', 'y'])
                if not defenders_at_block.empty:
                    ball_pos_at_block = df_ball.loc[max_deviation_frame][['x', 'y']].values
                    min_dist_sq = np.min(np.sum((defenders_at_block[['x', 'y']].values - ball_pos_at_block)**2, axis=1))
                    if min_dist_sq < BLOCK_PROXIMITY_METERS_SQ: outcome = "Blocked Shot"
        if outcome is None:
            ball_pos = np.array([attempt['player_x'], attempt['player_y']])
            shot_velocity_vector = np.array([attempt['vel_x'], attempt['vel_y']])
            target_x = attempt['attacking_goal_x']
            cone_top_post, cone_bottom_post = np.array([target_x, GOAL_AREA_WIDTH / 2]), np.array([target_x, -GOAL_AREA_WIDTH / 2])
            vec_to_top, vec_to_bottom = cone_top_post - ball_pos, cone_bottom_post - ball_pos
            try:
                angle_of_cone = np.arccos(np.dot(vec_to_top, vec_to_bottom) / (np.linalg.norm(vec_to_top) * np.linalg.norm(vec_to_bottom)))
                angle_to_top = np.arccos(np.dot(shot_velocity_vector, vec_to_top) / (np.linalg.norm(shot_velocity_vector) * np.linalg.norm(vec_to_top)))
                angle_to_bottom = np.arccos(np.dot(shot_velocity_vector, vec_to_bottom) / (np.linalg.norm(shot_velocity_vector) * np.linalg.norm(vec_to_bottom)))
                if np.isclose(angle_of_cone, angle_to_top + angle_to_bottom): outcome = "Shot"
            except (ValueError, ZeroDivisionError): pass
        if outcome is not None:
            attempt_dict = attempt.to_dict()
            attempt_dict['outcome'] = outcome
            final_shots_data.append(attempt_dict)
    
    if not final_shots_data:
        print("  -> No final shots or blocks identified."); return
    df_final_shots = pd.DataFrame(final_shots_data)
    blocked_shot_count = (df_final_shots['outcome'] == 'Blocked Shot').sum()
    print(f"  -> Stage 3 (Classification): Identified {len(df_final_shots)} total shots ({blocked_shot_count} blocked).")

    shots_for_db = []
    p1_start_time = df_tracking[df_tracking['period'] == 1]['timestamp'].min()
    p1_duration_seconds = 45 * 60.0
    p2_start_time = df_tracking[df_tracking['period'] == 2]['timestamp'].min() if 2 in df_tracking.period.unique() else None

    for i, shot in df_final_shots.iterrows():
        frame, period = int(shot['frame']), int(shot['period'])
        defenders, goalkeeper = get_player_positions_at_frame(df_players, frame, shot['shooter_team'], pitch_length)
        xg = calculate_advanced_xg(shot['player_x'], shot['player_y'], defenders, goalkeeper, pitch_length)
        
        kick_timestamp = pd.to_datetime(df_ball.loc[frame]['timestamp_iso'])
        
        if period == 1: elapsed_seconds = (kick_timestamp - p1_start_time).total_seconds()
        elif p2_start_time is not None: elapsed_seconds = p1_duration_seconds + (kick_timestamp - p2_start_time).total_seconds()
        else: elapsed_seconds = 0
        
        game_minute, game_second = int(elapsed_seconds // 60), int(elapsed_seconds % 60)
        if period == 1 and game_minute >= 45: game_minute = 45
        shot_id = f"shot-{match_id_str}-{frame}"

        # MODIFIED: Added timestamp to the record
        shots_for_db.append({
            'shot_id': shot_id,
            'match_id': match_id_str,
            'period': period,
            'frame': frame,
            'timestamp': kick_timestamp, # NEW: The exact timestamp of the shot
            'game_minute': game_minute,
            'game_second': game_second,
            'shooter_id': shot['shooter_id'],
            'shooter_team': shot['shooter_team'],
            'start_x': shot['player_x'],
            'start_y': shot['player_y'],
            'outcome': shot['outcome'],
            'xg': xg
        })
    if shots_for_db:
        pd.DataFrame(shots_for_db).to_sql('shots', engine, if_exists='append', index=False)
        print(f"  -> Saved {len(shots_for_db)} shots with xG to the database.")

# --- Main Orchestrator (Modified to add 'timestamp' column during data load) ---

def process_all_events(match_id_str: str, engine):
    """Orchestrates the entire event detection and processing pipeline for a given match."""
    print(f"\n--- Starting Full Event Processing for Match {match_id_str} ---")
    setup_database(engine)
    try:
        ball_id_val = get_ball_trackable_id(match_id_str, engine)
        player_team_map = get_player_team_mapping(match_id_str)
        with open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'opendata', 'data', 'matches', match_id_str, 'match_data.json'), 'r') as f:
            meta_data = json.load(f)
            home_team_name_val, away_team_name_val, pitch_length_val = meta_data['home_team']['name'], meta_data['away_team']['name'], meta_data.get('pitch_length', 105.0)
        print(f"  -> Match Config: Home='{home_team_name_val}', Away='{away_team_name_val}'.")
    except (FileNotFoundError, ValueError) as e:
        print(f"  -> CRITICAL ERROR: Could not load metadata. Aborting. Error: {e}"); return

    print("  -> Loading all tracking data into memory...")
    sql = text(f"SELECT t.match_id, t.frame, t.period, t.timestamp_iso, (p.obj ->> 'trackable_object') as trackable_object, (p.obj ->> 'x')::FLOAT as x, (p.obj ->> 'y')::FLOAT as y FROM tracking_data t, LATERAL jsonb_array_elements(t.tracked_objects) p(obj) WHERE t.match_id = '{match_id_str}' AND (p.obj ->> 'trackable_object') IS NOT NULL")
    df_tracking = pd.read_sql(sql, engine)
    
    # MODIFIED: Ensure the timestamp column is in datetime format for all subsequent functions
    df_tracking['timestamp'] = pd.to_datetime(df_tracking['timestamp_iso'])
    df_tracking['team_name'] = df_tracking['trackable_object'].map(player_team_map)
    df_tracking.sort_values('frame', inplace=True)
    df_tracking['time_delta'] = df_tracking.groupby('trackable_object')['timestamp'].diff().dt.total_seconds()
    print(f"  -> Data loaded and enriched. Total frames: {len(df_tracking)}")
    
    df_players = df_tracking[df_tracking['trackable_object'] != ball_id_val].copy()
    
    # Pass the enriched DataFrame to all functions
    df_spells = calculate_possession_spells(df_tracking, ball_id_val, engine)
    identify_passes(df_spells, df_tracking, ball_id_val, engine)
    identify_big_chances(df_spells, df_players, home_team_name_val, pitch_length_val, engine)
    identify_shots_and_calculate_xg(df_tracking, ball_id_val, home_team_name_val, pitch_length_val, match_id_str, engine)
    
    print(f"\n--- Full Event Processing Complete for Match {match_id_str} ---")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python ingest/process_events.py <match_id>")
        sys.exit(1)
    
    match_id_arg = sys.argv[1]
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dotenv_path = os.path.join(project_root, '.env')
    
    if not os.path.exists(dotenv_path):
        raise FileNotFoundError(f"Could not find .env file at {dotenv_path}")
    
    load_dotenv(dotenv_path=dotenv_path)
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable not set.")
        
    db_engine = create_engine(DATABASE_URL)
    process_all_events(match_id_arg, db_engine)