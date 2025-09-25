from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import UUID
from typing import Union # Make sure to add Union to this import line at the top of the file
from pydantic import field_validator # Make sure to add field_validator to this import

from pydantic import BaseModel, Field

# =============================================================================
# PYDANTIC DATA CONTRACTS (V2 - SKILLCORNER ALIGNED)
# =============================================================================

# --- Sub-models for nested objects ---

class TrackedObject(BaseModel):
    """
    Represents a single object (player, ball, ref) within a frame.
    This version is more robust to variations in the source data types.
    """
    track_id: int
    # Allow the source data to be a string, an integer, or None.
    trackable_object: Optional[Union[str, int]] = None
    group_name: Optional[str] = None
    x: float
    y: float
    z: Optional[float] = None

    # This is a Pydantic validator. It runs after the initial data is read
    # and ensures the final `trackable_object` is always a string.
    @field_validator('trackable_object', mode='before')
    @classmethod
    def convert_trackable_object_to_str(cls, v):
        if v is not None:
            return str(v)
        return v

class Possession(BaseModel):
    """Represents the possession data for a frame."""
    trackable_object: str
    group: str

# --- Main Table Models ---

class TrackingFrame(BaseModel):
    """
    Represents a single row in our new `tracking_data` table.
    """
    match_id: str
    period: int
    frame: int
    timestamp_iso: datetime
    tracked_objects: List[TrackedObject]
    frame_metadata: Optional[Dict[str, Any]] = None

class MatchMetadata(BaseModel):
    """
    Represents a single row in our `match_metadata` table.
    """
    match_id: str
    competition_name: Optional[str] = None
    home_team_name: Optional[str] = None
    away_team_name: Optional[str] = None
    pitch_length_m: Optional[float] = None
    pitch_width_m: Optional[float] = None
    additional_info: Optional[Dict[str, Any]] = None