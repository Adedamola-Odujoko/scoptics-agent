import pandas as pd
import pytest

# This is the TDD magic: we import a function that does NOT exist yet.
# Our goal in the next task will be to create this function and make this test pass.
from scoptics_agent.events.detectors import detect_2v1_in_final_third
from scoptics_agent.events.clustering import cluster_frames_into_events

def test_detect_2v1_finds_clear_situation():
    """
    Tests that the detector can identify a clear, multi-frame 2v1 situation
    in the final third and ignore frames where the condition is not met.
    """
    # 1. ARRANGE: Create a synthetic dataset
    # --------------------------------------------------------------------------
    # This data represents 5 frames of a match.
    # The pitch length is assumed to be 105m, so the final third starts at x > 69.3.
    # We will define a 2v1 situation in frames 2, 3, and 4.
    test_data = [
        # Frame 1: Ball is in the middle third - SHOULD BE IGNORED
        {'match_id': 'test_01', 'frame': 1, 'timestamp_iso': '2025-01-01T12:00:00.0Z', 'player_id': 'ball', 'team_id': None, 'x': 50.0, 'y': 0.0, 'z': 0.25},

        # --- Start of 2v1 Event ---
        # Frame 2: Ball is in final third. 2 attackers vs 1 defender are near.
        {'match_id': 'test_01', 'frame': 2, 'timestamp_iso': '2025-01-01T12:00:00.1Z', 'player_id': 'ball', 'team_id': None, 'x': 80.0, 'y': 0.0, 'z': 0.25},
        {'match_id': 'test_01', 'frame': 2, 'timestamp_iso': '2025-01-01T12:00:00.1Z', 'player_id': 'att_1', 'team_id': 'team_A', 'x': 81.0, 'y': 1.0, 'z': 0.0}, # Near
        {'match_id': 'test_01', 'frame': 2, 'timestamp_iso': '2025-01-01T12:00:00.1Z', 'player_id': 'att_2', 'team_id': 'team_A', 'x': 79.0, 'y': -2.0, 'z': 0.0}, # Near
        {'match_id': 'test_01', 'frame': 2, 'timestamp_iso': '2025-01-01T12:00:00.1Z', 'player_id': 'def_1', 'team_id': 'team_B', 'x': 80.5, 'y': -1.0, 'z': 0.0}, # Near
        {'match_id': 'test_01', 'frame': 2, 'timestamp_iso': '2025-01-01T12:00:00.1Z', 'player_id': 'def_2', 'team_id': 'team_B', 'x': 60.0, 'y': 10.0, 'z': 0.0}, # Far away

        # Frame 3: The 2v1 situation continues.
        {'match_id': 'test_01', 'frame': 3, 'timestamp_iso': '2025-01-01T12:00:00.2Z', 'player_id': 'ball', 'team_id': None, 'x': 82.0, 'y': 0.5, 'z': 0.25},
        {'match_id': 'test_01', 'frame': 3, 'timestamp_iso': '2025-01-01T12:00:00.2Z', 'player_id': 'att_1', 'team_id': 'team_A', 'x': 83.0, 'y': 1.5, 'z': 0.0},
        {'match_id': 'test_01', 'frame': 3, 'timestamp_iso': '2025-01-01T12:00:00.2Z', 'player_id': 'att_2', 'team_id': 'team_A', 'x': 81.0, 'y': -1.5, 'z': 0.0},
        {'match_id': 'test_01', 'frame': 3, 'timestamp_iso': '2025-01-01T12:00:00.2Z', 'player_id': 'def_1', 'team_id': 'team_B', 'x': 82.5, 'y': -0.5, 'z': 0.0},
        {'match_id': 'test_01', 'frame': 3, 'timestamp_iso': '2025-01-01T12:00:00.2Z', 'player_id': 'def_2', 'team_id': 'team_B', 'x': 61.0, 'y': 10.5, 'z': 0.0},

        # Frame 4: The 2v1 situation continues.
        {'match_id': 'test_01', 'frame': 4, 'timestamp_iso': '2025-01-01T12:00:00.3Z', 'player_id': 'ball', 'team_id': None, 'x': 84.0, 'y': 1.0, 'z': 0.25},
        {'match_id': 'test_01', 'frame': 4, 'timestamp_iso': '2025-01-01T12:00:00.3Z', 'player_id': 'att_1', 'team_id': 'team_A', 'x': 85.0, 'y': 2.0, 'z': 0.0},
        {'match_id': 'test_01', 'frame': 4, 'timestamp_iso': '2025-01-01T12:00:00.3Z', 'player_id': 'att_2', 'team_id': 'team_A', 'x': 83.0, 'y': -1.0, 'z': 0.0},
        {'match_id': 'test_01', 'frame': 4, 'timestamp_iso': '2025-01-01T12:00:00.3Z', 'player_id': 'def_1', 'team_id': 'team_B', 'x': 84.5, 'y': 0.0, 'z': 0.0},
        {'match_id': 'test_01', 'frame': 4, 'timestamp_iso': '2025-01-01T12:00:00.3Z', 'player_id': 'def_2', 'team_id': 'team_B', 'x': 62.0, 'y': 11.0, 'z': 0.0},
        # --- End of 2v1 Event ---

        # Frame 5: A second defender comes near - situation is now 2v2 - SHOULD BE IGNORED
        {'match_id': 'test_01', 'frame': 5, 'timestamp_iso': '2025-01-01T12:00:00.4Z', 'player_id': 'ball', 'team_id': None, 'x': 86.0, 'y': 1.5, 'z': 0.25},
        {'match_id': 'test_01', 'frame': 5, 'timestamp_iso': '2025-01-01T12:00:00.4Z', 'player_id': 'att_1', 'team_id': 'team_A', 'x': 87.0, 'y': 2.5, 'z': 0.0},
        {'match_id': 'test_01', 'frame': 5, 'timestamp_iso': '2025-01-01T12:00:00.4Z', 'player_id': 'att_2', 'team_id': 'team_A', 'x': 85.0, 'y': -0.5, 'z': 0.0},
        {'match_id': 'test_01', 'frame': 5, 'timestamp_iso': '2025-01-01T12:00:00.4Z', 'player_id': 'def_1', 'team_id': 'team_B', 'x': 86.5, 'y': 0.5, 'z': 0.0},
        {'match_id': 'test_01', 'frame': 5, 'timestamp_iso': '2025-01-01T12:00:00.4Z', 'player_id': 'def_2', 'team_id': 'team_B', 'x': 85.5, 'y': 1.0, 'z': 0.0}, # Now near
    ]
    tracking_df = pd.DataFrame(test_data)

    # 2. ACT: Run the (not-yet-written) detector on the data
    # --------------------------------------------------------------------------
    # We'll pass some parameters for our detector logic.
    params = {
        "pitch_length": 105,
        "local_radius": 10  # 10 meter radius to check for players
    }
    # The detector is expected to return a list of dictionaries, one for each
    # frame where the 2v1 condition is met.
    detections = detect_2v1_in_final_third(tracking_df, attacking_team_id="team_A", params=params)

    # 3. ASSERT: Check that the output is exactly what we expect
    # --------------------------------------------------------------------------
    # We expect to find exactly 3 frames that match our criteria.
    assert len(detections) == 3

    # We can also check the specific frames that were detected.
    detected_frames = [d['frame'] for d in detections]
    assert detected_frames == [2, 3, 4]

def test_cluster_frames_into_events():
    """
    Tests that a list of frame-level detections are correctly grouped into
    events based on a maximum frame gap.
    """
    # 1. ARRANGE: Create a synthetic list of frame detections.
    # --------------------------------------------------------------------------
    # This list represents three distinct events:
    # - Event 1: A continuous event from frame 10 to 12.
    # - Event 2: A continuous event from frame 50 to 51 (after a large gap).
    # - Event 3: A single-frame event at frame 100.
    frame_detections = [
        {'frame': 10, 'timestamp_iso': '2025-01-01T12:00:10.0Z', 'match_id': 'test_01'},
        {'frame': 11, 'timestamp_iso': '2025-01-01T12:00:11.0Z', 'match_id': 'test_01'},
        {'frame': 12, 'timestamp_iso': '2025-01-01T12:00:12.0Z', 'match_id': 'test_01'},
        # Large gap here, should start a new event.
        {'frame': 50, 'timestamp_iso': '2025-01-01T12:00:50.0Z', 'match_id': 'test_01'},
        {'frame': 51, 'timestamp_iso': '2025-01-01T12:00:51.0Z', 'match_id': 'test_01'},
        # Another large gap.
        {'frame': 100, 'timestamp_iso': '2025-01-01T12:01:40.0Z', 'match_id': 'test_01'},
    ]

    # 2. ACT: Run the (not-yet-written) clustering function
    # --------------------------------------------------------------------------
    # We'll set the max gap to 5 frames. Any gap larger than this will split the event.
    clustered_events = cluster_frames_into_events(
        frame_detections, 
        max_frame_gap=5
    )

    # 3. ASSERT: Check that the output is exactly what we expect
    # --------------------------------------------------------------------------
    # We expect the function to find 3 distinct events.
    assert len(clustered_events) == 3

    # Now, let's check the details of each event to be sure.
    event1, event2, event3 = clustered_events

    assert event1['start_frame'] == 10
    assert event1['end_frame'] == 12
    assert event1['start_time'] == '2025-01-01T12:00:10.0Z'
    assert event1['end_time'] == '2025-01-01T12:00:12.0Z'

    assert event2['start_frame'] == 50
    assert event2['end_frame'] == 51
    assert event2['start_time'] == '2025-01-01T12:00:50.0Z'
    assert event2['end_time'] == '2025-01-01T12:00:51.0Z'

    assert event3['start_frame'] == 100
    assert event3['end_frame'] == 100
    assert event3['start_time'] == '2025-01-01T12:01:40.0Z'
    assert event3['end_time'] == '2025-01-01T12:01:40.0Z'