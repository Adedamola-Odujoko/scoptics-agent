from typing import List, Dict

def cluster_frames_into_events(frame_detections: List[Dict], max_frame_gap: int) -> List[Dict]:
    """
    Groups a list of frame-level detections into continuous events.

    Args:
        frame_detections: A list of dictionaries, where each represents a detected frame.
                          Must contain at least 'frame' and 'timestamp_iso'.
        max_frame_gap: The maximum number of frames allowed between two detections
                       for them to be considered part of the same event.

    Returns:
        A list of dictionaries, where each represents a clustered event with
        start/end frames and timestamps.
    """
    # Edge case: If there are no detections, return an empty list.
    if not frame_detections:
        return []

    # CRITICAL: Sort the detections by frame number to process them in order.
    sorted_detections = sorted(frame_detections, key=lambda d: d['frame'])

    clustered_events = []
    # Start the first event with the first detection.
    current_event = {
        'match_id': sorted_detections[0]['match_id'],
        'start_frame': sorted_detections[0]['frame'],
        'end_frame': sorted_detections[0]['frame'],
        'start_time': sorted_detections[0]['timestamp_iso'],
        'end_time': sorted_detections[0]['timestamp_iso'],
        'frame_count': 1
    }

    # Iterate through the rest of the detections.
    for i in range(1, len(sorted_detections)):
        current_detection = sorted_detections[i]
        
        # Calculate the gap between the last frame of the current event and this new frame.
        frame_gap = current_detection['frame'] - current_event['end_frame']

        if frame_gap <= max_frame_gap:
            # This detection is part of the current event. Extend the event.
            current_event['end_frame'] = current_detection['frame']
            current_event['end_time'] = current_detection['timestamp_iso']
            current_event['frame_count'] += 1
        else:
            # The gap is too large. The previous event is finished.
            # 1. Save the completed event.
            clustered_events.append(current_event)
            # 2. Start a new event with the current detection.
            current_event = {
                'match_id': current_detection['match_id'],
                'start_frame': current_detection['frame'],
                'end_frame': current_detection['frame'],
                'start_time': current_detection['timestamp_iso'],
                'end_time': current_detection['timestamp_iso'],
                'frame_count': 1
            }

    # After the loop finishes, the very last event is still in `current_event`.
    # We need to add it to our list.
    clustered_events.append(current_event)

    return clustered_events