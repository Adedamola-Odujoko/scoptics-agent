# Import necessary components from Python's standard libraries and Pydantic.
from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import UUID  # A universally unique identifier, great for primary keys.

from pydantic import BaseModel, Field

# =============================================================================
# PYDANTIC DATA CONTRACTS
# =============================================================================
# These classes define the expected structure and data types for objects in our
# application. Pydantic uses these to validate data and provide helpful
# editor support (like autocompletion).
# =============================================================================


class TrackingRecord(BaseModel):
    """
    Represents a single row of data from the raw `tracking` table.
    This model validates the data upon creation.
    """
    match_id: str
    frame: int
    timestamp_iso: datetime  # Pydantic will automatically parse ISO strings into datetime objects.
    
    # `Optional` means this field can be `None`. This is useful for the ball,
    # which has no team_id or player_id. `None` is the default value.
    team_id: Optional[str] = None
    player_id: Optional[str] = None
    
    x: float
    y: float
    z: float
    speed: Optional[float] = None
    orientation: Optional[float] = None


class Event(BaseModel):
    """
    Represents a single row from the pre-computed `events` table.
    """
    # `Field(default_factory=uuid.uuid4)` is a common pattern to generate a new
    # unique ID for every new event we create.
    event_id: UUID
    match_id: str
    event_type: str
    
    start_time: datetime
    end_time: datetime
    start_frame: int
    end_frame: int
    
    team_id: Optional[str] = None
    
    # For JSONB fields, we can use standard Python types like List or Dict.
    # `List[str]` means we expect a list of strings, e.g., ["player1", "player7"].
    players_involved: List[str]
    
    # `Dict[str, Any]` is a flexible way to handle a JSON object where we might
    # not know all the keys in advance. It's a dictionary with string keys and
    # any type of value.
    metadata_json: Dict[str, Any]