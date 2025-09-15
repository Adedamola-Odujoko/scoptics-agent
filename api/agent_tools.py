from pydantic import BaseModel, Field
from typing import Dict, Any

# Import the actual database query functions we built in Phase 3
from .retrieval import query_events_by_filters, query_events_by_semantic
from .retrieval import execute_dynamic_sql_query

# =============================================================================
# TOOL DEFINITIONS FOR THE LLM AGENT
# =============================================================================
# Each tool consists of:
# 1. A Pydantic model for structured, validated arguments.
# 2. A Python function that takes the Pydantic model and executes the tool's logic.
# 3. A detailed docstring explaining to the LLM what the tool does.
# =============================================================================


# --- Tool 1: Structured Filter-Based Search ---

class StructuredFilterArgs(BaseModel):
    """Arguments for the structured event search tool."""
    filters: Dict[str, Any] = Field(..., description="A dictionary of filters to apply, where keys are column names and values are the values to match. Example: {'team_id': 'team_A', 'event_type': 'pass'}")

def find_events_with_filters(args: StructuredFilterArgs):
    """
    Use this tool to find events that match a set of exact, specific criteria.
    This is the best tool for queries like "show me all passes by team A" or
    "find all shots in match_001". It is not suitable for vague or semantic
    searches like "find exciting moments".
    """
    print(f"TOOL EXECUTED: find_events_with_filters with filters: {args.filters}")
    return query_events_by_filters(args.filters)


# --- Tool 2: Semantic Similarity Search ---

class SemanticSearchArgs(BaseModel):
    """Arguments for the semantic event search tool."""
    query_text: str = Field(..., description="The natural language text to search for. Example: 'a fast counter attack down the right wing'")
    top_k: int = Field(5, description="The maximum number of similar events to return.")

def find_similar_events(args: SemanticSearchArgs):
    """
    Use this tool to find events based on their tactical meaning or similarity
    to a descriptive text. This is the best tool for vague or conceptual
    queries like "show me moments of high pressure" or "find dangerous attacks".
    It is not suitable for finding events based on exact criteria like a specific
    player_id or match_id.
    """
    print(f"TOOL EXECUTED: find_similar_events with query: '{args.query_text}'")
    return query_events_by_semantic(args.query_text, args.top_k)

class DynamicSqlArgs(BaseModel):
    """Arguments for the dynamic SQL query execution tool."""
    sql_query: str = Field(..., description="A single, valid, read-only PostgreSQL SELECT query to be executed against the raw 'tracking' data table.")

def run_dynamic_query(args: DynamicSqlArgs):
    """
    Use this SUPER-TOOL ONLY when no other tool can answer the user's question.
    This is for advanced, dynamic analysis. Formulate a single, read-only
    PostgreSQL SELECT query to find the answer. The query will be run against the
    'tracking' table which contains all raw frame-by-frame data. Do NOT use this tool
    for simple requests that can be handled by other tools.

    The 'tracking' table schema is:
    - match_id (TEXT)
    - frame (INTEGER)
    - timestamp_iso (TIMESTAMPTZ)
    - team_id (TEXT)
    - player_id (TEXT)
    - x (DOUBLE PRECISION)
    - y (DOUBLE PRECISION)
    - z (DOUBLE PRECISION)
    - speed (DOUBLE PRECISION)
    - orientation (DOUBLE PRECISION)
    
    IMPORTANT: For any query that describes a situation over time (like "defenders
    are in a flat line" or "ball is in the box"), you MUST select the `frame`,
    `timestamp_iso`, and `match_id` columns, as they are needed for clustering.
    For simple aggregations like "fastest moment", you can select fewer columns.
    """
    print(f"TOOL EXECUTED: run_dynamic_query with SQL: '{args.sql_query}'")
    return execute_dynamic_sql_query(args.sql_query)