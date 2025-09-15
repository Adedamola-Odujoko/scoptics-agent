import pandas as pd
import numpy as np

def detect_2v1_in_final_third(tracking_df: pd.DataFrame, attacking_team_id: str, params: dict):
    """
    Detects frame-level instances of a 2v1 situation in the final third.

    Args:
        tracking_df: DataFrame containing the tracking data for a match.
        attacking_team_id: The ID of the team we are analyzing for the attack.
        params: A dictionary containing detector parameters like 'pitch_length' and 'local_radius'.

    Returns:
        A list of dictionaries, where each dictionary represents a frame
        that meets the 2v1 criteria.
    """
    # Extract parameters for easier access
    pitch_length = params.get("pitch_length", 105)
    local_radius = params.get("local_radius", 10)
    
    # Calculate the start of the final third based on pitch length
    final_third_x = pitch_length / 3

    detections = []

    # Grouping by frame is an efficient way to process the data frame-by-frame
    for frame, frame_df in tracking_df.groupby('frame'):
        
        # --- Condition 1: Find the ball's position ---
        ball_position = frame_df[frame_df['player_id'] == 'ball'][['x', 'y']].values
        if ball_position.size == 0:
            continue # Skip frames where there is no ball data
        ball_pos = ball_position[0]

        # --- Condition 2: Check if the ball is in the final third ---
        # Note: We assume the attack is towards the positive x direction.
        # A more robust system would handle attacking direction.
        if ball_pos[0] < final_third_x:
            continue

        # --- Condition 3: Identify attackers and defenders ---
        players_df = frame_df[frame_df['player_id'] != 'ball']
        attackers_df = players_df[players_df['team_id'] == attacking_team_id]
        defenders_df = players_df[players_df['team_id'] != attacking_team_id]

        if attackers_df.empty or defenders_df.empty:
            continue

        # --- Condition 4: Calculate distances and count players near the ball ---
        # We use NumPy for fast, vectorized distance calculations.
        attacker_positions = attackers_df[['x', 'y']].values
        defender_positions = defenders_df[['x', 'y']].values

        # Calculate the distance from each player to the ball
        attacker_distances = np.linalg.norm(attacker_positions - ball_pos, axis=1)
        defender_distances = np.linalg.norm(defender_positions - ball_pos, axis=1)
        
        # Count how many are within the specified local radius
        num_attackers_near = np.sum(attacker_distances <= local_radius)
        num_defenders_near = np.sum(defender_distances <= local_radius)

        # --- Final Check: Is it a 2v1 situation? ---
        if num_attackers_near >= 2 and num_defenders_near == 1:
            # If all conditions are met, we have a detection!
            detections.append({
                'match_id': frame_df['match_id'].iloc[0],
                'frame': frame,
                'timestamp_iso': frame_df['timestamp_iso'].iloc[0],
                'details': f"{num_attackers_near} attackers vs {num_defenders_near} defenders"
            })

    return detections