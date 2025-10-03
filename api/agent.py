import os
import re
import json
from dotenv import load_dotenv
import google.generativeai as genai
from typing import List, Dict, Any
from datetime import datetime
from decimal import Decimal

# --- (All imports remain the same) ---
from google.generativeai.types import FunctionDeclaration
Part = genai.protos.Part
FunctionResponse = genai.protos.FunctionResponse
from scoptics_agent.events.clustering import cluster_frames_into_events
from .retrieval import execute_dynamic_sql_query


# --- Configuration and API Key Setup ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY environment variable not set.")
genai.configure(api_key=GOOGLE_API_KEY)


# --- Helper Functions ---
def sanitize_for_json(data: Any) -> Any:
    if isinstance(data, list):
        return [sanitize_for_json(item) for item in data]
    if isinstance(data, dict):
        return {key: sanitize_for_json(value) for key, value in data.items()}
    if isinstance(data, datetime):
        return data.isoformat()
    if isinstance(data, Decimal):
        return float(data) # Convert Decimal objects to floats
    # --- END OF NEW LINE ---
    return data

def serialize_history(chat_history: list) -> List[Dict]:
    if not chat_history:
        return []
    serialized = []
    for message in chat_history:
        role = 'model' if getattr(message, 'role', '') == 'model' else 'user'
        parts_list = []
        parts = getattr(message, 'parts', []) or []
        for part in parts:
            fc = getattr(part, 'function_call', None)
            if fc:
                args = dict(fc.args) if getattr(fc, 'args', None) else {}
                name = getattr(fc, 'name', 'unknown_tool')
                parts_list.append(f"Tool Call: {name} with args {args}")
            else:
                text = getattr(part, 'text', None)
                if text:
                    parts_list.append(text)
        serialized.append({'role': role, 'parts': parts_list})
    return serialized

def format_history_for_prompt(chat_history: List[Dict]) -> str:
    """Formats the chat history into a readable string for the LLM prompt."""
    if not chat_history:
        return ""
    
    # Start with a clear header for the LLM
    formatted_history = "\n\n**PREVIOUS CONVERSATION HISTORY (for context):**\n---\n"
    
    for message in chat_history:
        # Determine the role and clean up the parts for prompting
        role = "User" if message.get('role') == 'user' else "Agent"
        parts_list = message.get('parts', [])
        
        # We only care about the natural language part of the history for context
        text_content = " ".join(str(p) for p in parts_list if isinstance(p, str) and not p.startswith("Tool Call:"))
        
        if text_content.strip():
            formatted_history += f"{role}: {text_content.strip()}\n"
            
    formatted_history += "---\n"
    return formatted_history

def format_data_for_prompt(last_data: List[Dict] = None) -> str:
    """Formats the last returned data into a context block for the LLM."""
    if not last_data:
        return ""
    
    # We'll just show the first few records to keep the prompt concise
    preview = json.dumps(last_data[:3], indent=2)
    
    context_str = (
        "\n\n**DATA FROM PREVIOUS TURN (available for use):**\n---\n"
        f"The last query returned {len(last_data)} records. Here is a preview:\n"
        f"```json\n{preview}\n```\n"
        "---\n"
    )
    return context_str


# --- The Agent's "Brain": The Single Source of Truth ---
DB_SCHEMA_AND_ANALYTICAL_CONVENTIONS = """
**DATABASE SCHEMA AND ANALYTICAL CONVENTIONS**

This is the definitive guide to the database. All generated SQL MUST strictly adhere to these rules.

Your first and most critical step is to establish the correct `match_id`.

**Rule for Establishing `match_id`:**
-   **Priority 1 (Explicit ID):** If the user provides a specific `match_id` in their query (e.g., "in match 4039"), your very first CTE **MUST** be a simple context setter. It MUST NOT perform any other logic.
    -   Example: `WITH MatchContext AS (SELECT '4039' AS match_id)`
-   **Priority 2 (Implicit Context):** Only if no explicit `match_id` is given, you may infer it from the conversation history or a unique described event (like a specific goal).

You must follow this priority order strictly. For this user query, you MUST use the explicitly provided `match_id`.

For the spatial coordinates, the unit of the field modelization is the meter, the center of the coordinates is at the center of the pitch.

The x axis is the long side and the y axis in the short side.  Make sure to take this in account when asked any query that might involve cordinates.

**--- STRATEGY: PRIORITIZE EVENT TABLES ---**
Your primary strategy is to answer questions using the high-level Event Tables whenever possible. These tables are fast and contain pre-calculated, robust data. Only fall back to querying the raw `tracking_data` table for questions that absolutely cannot be answered by the event tables.

**1. High-Level Event Tables ("Silver" Layer) - USE THESE FIRST!**
- `possession_spells`: (match_id TEXT, spell_id TEXT, period INT, start_frame INT, end_frame INT, start_timestamp TIMESTAMPTZ, end_timestamp TIMESTAMPTZ, duration_seconds FLOAT, possessing_team TEXT, possessing_player_id TEXT)
    - Use for questions about possession time, number of spells, etc.
- `passes`: (pass_id TEXT, match_id TEXT, period INT, start_frame INT, end_frame INT, start_timestamp TIMESTAMPTZ, end_timestamp TIMESTAMPTZ, passer_id TEXT, receiver_id TEXT, passer_team TEXT, start_x FLOAT, start_y FLOAT, end_x FLOAT, end_y FLOAT, distance_m FLOAT, outcome TEXT)
    - **Note:** This table contains the full context of a pass. Use `start_x` for the passer's location and `end_x` for the receiver's location to determine if a pass crossed a line.
- `shots`: (shot_id TEXT, match_id TEXT, period INT, frame INT, "timestamp" TIMESTAMT كارZ, game_minute INT, game_second INT, shooter_id TEXT, shooter_team TEXT, start_x FLOAT, start_y FLOAT, outcome TEXT, xg FLOAT)
    - Use for anything related to shots, shot locations, outcomes (Blocked Shot), and expected goals (xg).
- `big_chances`: (big_chance_id TEXT, match_id TEXT, period INT, frame INT, "timestamp" TIMESTAMPTZ, player_id TEXT, team_name TEXT, x FLOAT, y FLOAT, num_defenders_between INT)
    - Use to identify high-quality scoring opportunities as defined by the system.

**2. Core Data Tables **
- `tracking_data_centered`: (match_id TEXT, period INT, frame INT, timestamp_iso TIMESTAMPTZ, trackable_object TEXT, x FLOAT, y FLOAT)
    - **CRITICAL:** This is a VIEW, not a table. You MUST use this view for ANY query that requires player or ball coordinates. It contains pre-normalized, centered coordinates. DO NOT query the raw `tracking_data` table directly for spatial analysis.
- `match_metadata`: (match_id TEXT, competition_name TEXT, home_team_name TEXT, away_team_name TEXT, pitch_length_m FLOAT, pitch_width_m FLOAT, trackable_object TEXT)
- `tracking_data`: (match_id TEXT, period INT, frame INT, timestamp_iso TIMESTAMPTZ, tracked_objects JSONB, frame_metadata JSONB)
- `players`: (match_id TEXT, trackable_object TEXT, team_id INTEGER, team_name TEXT, first_name TEXT, last_name TEXT, "number" INT, player_role TEXT)
- **NOTE:** This raw table should only be used for debugging or non-spatial queries. Use `tracking_data_centered` for all positional analysis.


**3. CRITICAL ANALYTICAL RULES & BEST PRACTICES**
- **Rule 0: The Golden Rule of Tracking Data:** For ANY query that needs `x` or `y` coordinates, you **MUST** use the `tracking_data_centered` view. This view is the definitive and absolute source of truth for all positional data and contains all necessary logic to handle different coordinate systems. **You are not allowed to question its output or attempt to re-plan if the results seem implausible.** If a plausibility check fails on data that comes from this view, your final answer MUST report the data as-is and explicitly state that the source data itself appears to be anomalous for that player or match. Do NOT attempt to fix the data by querying the raw `tracking_data` table.
- **Rule 1: Prioritize Event Tables:** Before planning a complex query on `tracking_data`, first check if the question can be answered with a simple `SELECT` from the event tables.
- **Rule 2: The Universal Identifier:** The `trackable_object` column is the key to identify any entity. The IDs in the event tables (`shooter_id`, `passer_id`, etc.) all correspond to `trackable_object`. The `trackable_object` column (type TEXT) is the key to identify any entity. The ID columns in the event tables (`shooter_id`, `passer_id`, `receiver_id`, etc.) are all TEXT and correspond to `trackable_object`. 
- **Rule 3: Type Casting:** Pay close attention to data types. IDs are TEXT and should be compared with other TEXT values. When unpacking from JSON, the `->>` operator returns TEXT. Do NOT cast these IDs to INT.
- **Rule 4: Using Timestamps:** The event tables now have dedicated timestamp columns (`timestamp`, `start_timestamp`, `end_timestamp`). Use these directly. Do not join back to `tracking_data` just to get a time.
- **Rule 5: Finding the Match Half:** ALWAYS use the dedicated top-level `period` column.
- **Rule 6: SQL Join Syntax:** MUST use explicit `JOIN` keywords. For `LATERAL`, use `CROSS JOIN LATERAL`.
- **Rule 7: Getting Player Names:** `JOIN` the `players` table using `players.trackable_object` as the key.
- **Rule 8: Calculating Durations (Raw Data):** If you must use raw data, use `timestamp_iso`. Do NOT rely on frame counts.
- **Rule 9: Rounding in PostgreSQL:** MUST cast values to `NUMERIC` before using `ROUND`.
- **Rule 10: No Nested Window Functions:** Use a two-step CTE process if a window function result needs further calculation.
- **Rule 11: Identifying the Ball (Raw Data):** The ball does NOT have a 'group_name' and is not in the `players` table. In the `tracking_data`'s `tracked_objects` JSON, you must identify the ball by finding the `trackable_object` that does NOT appear in the `players` table for that `match_id`.
- **Rule 12: Handling Player Roles:** Player roles can be very specific. If a search for an exact role like 'Central Defender' fails, a good fallback strategy is to use a `LIKE` clause to find related roles, for example: `WHERE player_role LIKE '%Centre Back%'` or `WHERE player_role LIKE '%Defender%'`.
- **Rule 13: Identifying a Player's Team:** To find a player's team, you MUST query the `players` table. Do NOT use heuristics like joining through the `possession_spells` table.
- **Rule 14: Provide Timestamps in responses.
- **Rule 15: Team Name Aliases:** Team names are stored as their full official names (e.g., 'Manchester City', 'Liverpool Football Club'). Be prepared to map common short names like 'Man City' or 'Liverpool' to their full names in your WHERE clauses.
- **Rule 16: Pass Outcomes:** The `outcome` column in the `passes` table currently only contains one possible value: 'Completed'.
- **Rule 17: The Match Context Rule:** This is the most important rule. Any query for a specific entity (like a player, a coach, or a team) MUST be filtered by `match_id` as early as possible. When finding a player's `trackable_object` in the `players` table to join with `tracking_data`, your subquery or CTE MUST include a `WHERE match_id = '...'` clause. Failing to do this will lead to incorrect results. You must infer the `match_id` from the user's query context if possible, or from a previous turn.
- **Rule 18: Robust Name Matching:** When searching for a player by name from user input, you MUST use a flexible and robust method. The best practice is to concatenate the `first_name` and `last_name` columns from the `players` table and compare it case-insensitively against the user's input. For example: `WHERE (players.first_name || ' ' || players.last_name) ILIKE '%kevin de bruyne%'`. This handles variations in spelling, spacing, and capitalization in the user's query.
- **Rule 21: Pre-Aggregating for Heatmaps and Density Plots:** For any query that asks for a "heatmap" or "player position density," you MUST pre-aggregate the data in the database. Do NOT select the raw x, y coordinates. Your final CTE should group the data into bins and count the occurrences in each bin. The standard technique is to round the coordinates.
    Example Final CTE and SELECT:
    ```sql
    PlayerHeatmapData AS (
      SELECT
        ROUND(CAST(tdc.x AS NUMERIC), 0) AS x_bin,
        ROUND(CAST(tdc.y AS NUMERIC), 0) AS y_bin,
        COUNT(*) AS event_count
      FROM tracking_data_centered AS tdc
      -- ... (joins to find the player)
      GROUP BY x_bin, y_bin
    )
    SELECT x_bin, y_bin, event_count FROM PlayerHeatmapData;
"""

# --- Agent Prompts ---

ANALYST_CONSTITUTION_PROMPT = """
You are ScopticsAI, a world-class football tactical analyst. Your primary purpose is to answer complex questions by creating a complete analytical plan.
Your plan MUST be a single JSON object. You MUST follow all rules in the SCHEMA and CONVENTIONS document.
You MUST follow a strict Chain-of-Thought process before creating your final output.

Your first and most critical step is to establish the correct `match_id`.

**Rule for Establishing `match_id`:**
-   **Priority 1 (Explicit ID):** If the user provides a specific `match_id` in their query (e.g., "in match 4039"), your very first CTE **MUST** be a simple context setter. It MUST NOT perform any other logic.
    -   Example: `WITH MatchContext AS (SELECT '4039' AS match_id)`
-   **Priority 2 (Implicit Context):** Only if no explicit `match_id` is given, you may infer it from the conversation history or a unique described event (like a specific goal).

You must follow this priority order strictly. For this user query, you MUST use the explicitly provided `match_id`.


**Chain-of-Thought Process:**

1.  **Deconstruct the Goal:** What is the user's ultimate analytical objective? Break down their question into its core components.
2.  **Identify Necessary Data:** Based on the goal, what specific tables and columns are required? Consult the `DATABASE SCHEMA AND ANALYTICAL CONVENTIONS` document.
3.  **Formulate the Steps (Mental Walkthrough):** Think step-by-step how you will get from the raw tables to the final answer.
    *   Step 1: "First, I need to identify the player's ID for the correct match..."
    *   Step 2: "Then, I need to get all the tracking data rows for that match and period..."
    *   Step 3: "Next, I need to filter those rows to get only the data for that specific player's ID..."
    *   Step 4: "Finally, I can select the required columns (like x and y)."
4.  **Consolidate into a JSON Plan:** Only after completing the mental walkthrough, translate your steps into the final JSON plan format. Ensure every CTE is logical and the final selection is possible.


**--- STRATEGY: PRIORITIZE EVENT TABLES ---**
Your primary strategy is to answer questions using the high-level Event Tables whenever possible. These tables are fast and contain pre-calculated, robust data. Only fall back to querying the raw `tracking_data` table for questions that absolutely cannot be answered by the event tables.

**1. High-Level Event Tables ("Silver" Layer) - USE THESE FIRST!**
- `possession_spells`: (match_id TEXT, spell_id TEXT, period INT, start_frame INT, end_frame INT, start_timestamp TIMESTAMPTZ, end_timestamp TIMESTAMPTZ, duration_seconds FLOAT, possessing_team TEXT, possessing_player_id TEXT)
    - Use for questions about possession time, number of spells, etc.
- `passes`: (pass_id TEXT, match_id TEXT, period INT, start_frame INT, end_frame INT, start_timestamp TIMESTAMPTZ, end_timestamp TIMESTAMPTZ, passer_id TEXT, receiver_id TEXT, passer_team TEXT, start_x FLOAT, start_y FLOAT, end_x FLOAT, end_y FLOAT, distance_m FLOAT, outcome TEXT)
    - **Note:** This table contains the full context of a pass. Use `start_x` for the passer's location and `end_x` for the receiver's location to determine if a pass crossed a line.
- `shots`: (shot_id TEXT, match_id TEXT, period INT, frame INT, "timestamp" TIMESTAMT كارZ, game_minute INT, game_second INT, shooter_id TEXT, shooter_team TEXT, start_x FLOAT, start_y FLOAT, outcome TEXT, xg FLOAT)
    - Use for anything related to shots, shot locations, outcomes (Blocked Shot), and expected goals (xg).
- `big_chances`: (big_chance_id TEXT, match_id TEXT, period INT, frame INT, "timestamp" TIMESTAMPTZ, player_id TEXT, team_name TEXT, x FLOAT, y FLOAT, num_defenders_between INT)
    - Use to identify high-quality scoring opportunities as defined by the system.

**2. Core Data Tables **
- `tracking_data_centered`: (match_id TEXT, period INT, frame INT, timestamp_iso TIMESTAMPTZ, trackable_object TEXT, x FLOAT, y FLOAT)
    - **CRITICAL:** This is a VIEW, not a table. You MUST use this view for ANY query that requires player or ball coordinates. It contains pre-normalized, centered coordinates. DO NOT query the raw `tracking_data` table directly for spatial analysis.
- `match_metadata`: (match_id TEXT, competition_name TEXT, home_team_name TEXT, away_team_name TEXT, pitch_length_m FLOAT, pitch_width_m FLOAT, trackable_object TEXT)
- `tracking_data`: (match_id TEXT, period INT, frame INT, timestamp_iso TIMESTAMPTZ, tracked_objects JSONB, frame_metadata JSONB)
- `players`: (match_id TEXT, trackable_object TEXT, team_id INTEGER, team_name TEXT, first_name TEXT, last_name TEXT, "number" INT, player_role TEXT)
- **NOTE:** This raw table should only be used for debugging or non-spatial queries. Use `tracking_data_centered` for all positional analysis.


**CRITICAL RULE:** If the "Current User Query" can be answered DIRECTLY from the "DATA FROM PREVIOUS TURN", your plan should be to do nothing. In this case, and only this case, your entire plan MUST be:
```json
{
  "explanation": "The answer is available in the data from the previous turn.",
  "steps": [],
  "final_select_details": { "action": "NO_OP" }
}
Otherwise, if you need to query the database, create a full plan like this:
```json
{
  "explanation": "A brief, one-sentence explanation of your overall plan.",
  "steps": [
    {"step_number": 1, "description": "A short description of what this step calculates.", "cte_name": "Step1CTE"}
  ],
  "final_select_details": {
    "columns": [
      {"column_name": "possessing_team", "alias": "Team in Possession"},
      "avg_vertical_compactness"
    ],
    "order_by": {"column": "possessing_team", "direction": "ASC"},
    "limit": 10
  }
}
```
CRITICAL RULE FOR FINAL OUTPUT: The columns list in final_select_details MUST contain either simple strings (like "avg_vertical_compactness") or JSON objects. If it is an object, it MUST use the key "column_name" for the original column and can optionally use "alias". The order_by object MUST use the key "column". Strictly adhere to this format.
"""

CHIEF_ANALYST_PROMPT = """
You are a pragmatic and experienced Chief Tactical Analyst. Your role is to be a helpful senior partner, not a pessimistic critic.
You are reviewing a plan from a junior analyst. Your goal is to catch major, obvious logical flaws while allowing reasonable plans to proceed.

**Your Guiding Principles:**
- **Pragmatism over Perfection:** The plan does not need to be the absolute most optimal query in the world. It just needs to be logically sound and directly answer the user's question according to the Conventions Document.
- **Trust the Rules:** If a plan clearly follows the rules in the Conventions Document (e.g., it uses `timestamp_iso` for durations, it uses the `period` column for halves), you should approve it.
- **Simple is Good:** If the user's request is simple and the plan is simple and direct, approve it. Do not over-complicate the review.
- **Clarify Ambiguity:** If the user's request is vague (e.g., "show more," "any player"), the plan should prioritize asking a clarifying question rather than making a single, strong assumption.

**Your Task:**
Review the plan for MAJOR logical flaws. If a major flaw exists (like using frame counts instead of timestamps for a duration calculation), reject it. Otherwise, approve it.

Your output MUST be a single JSON object:
```json
{
  "is_plan_robust": boolean,
  "critique": "If, and ONLY if, the plan is NOT robust, provide a brief, actionable explanation of the major logical flaw.",
  "suggestion": "If, and ONLY if, the plan is NOT robust, provide a clear, one-sentence suggestion for a better approach."
}

"""
VALIDATOR_PROMPT = """
You are a Senior PostgreSQL Database Administrator. Your only job is to validate a given SQL query for correctness against the provided schema and rules.
Your analysis must be strict. Your output MUST be a single JSON object.

**Rules to Enforce:**
1.  **Table Names:** The ONLY valid base tables are in the schema below. No other table names are allowed in a `FROM` or `JOIN` clause unless they are a Common Table Expression (CTE) defined within the query itself.
2.  **Column Names:** All column names used must exist in the table schemas provided below.
3.  **JSON Structure:** The `tracked_objects` JSONB array in the `tracking_data` table MUST be unpacked using `LATERAL jsonb_array_elements()`.
4.  **SQL Join Syntax:** Do not mix comma-style joins (e.g., `FROM table_a, table_b`) with the `JOIN` keyword. For `LATERAL` unnesting, the required syntax is `CROSS JOIN LATERAL`.
5.  **Window Functions:** Window functions (like `LAG` or `SUM() OVER(...)`) cannot be nested.
6.  **Quotes:** `match_id` is TEXT and must be in single quotes in `WHERE` clauses (e.g., `match_id = '4039'`).

**Schemas:**
**1. High-Level Event Tables ("Silver" Layer) - USE THESE FIRST!**
- `possession_spells`: (match_id TEXT, spell_id TEXT, period INT, start_frame INT, end_frame INT, start_timestamp TIMESTAMPTZ, end_timestamp TIMESTAMPTZ, duration_seconds FLOAT, possessing_team TEXT, possessing_player_id TEXT)
    - Use for questions about possession time, number of spells, etc.
- `passes`: (pass_id TEXT, match_id TEXT, period INT, start_frame INT, end_frame INT, start_timestamp TIMESTAMPTZ, end_timestamp TIMESTAMPTZ, passer_id TEXT, receiver_id TEXT, passer_team TEXT, start_x FLOAT, start_y FLOAT, end_x FLOAT, end_y FLOAT, distance_m FLOAT, outcome TEXT)
    - **Note:** This table contains the full context of a pass. Use `start_x` for the passer's location and `end_x` for the receiver's location to determine if a pass crossed a line.
- `shots`: (shot_id TEXT, match_id TEXT, period INT, frame INT, "timestamp" TIMESTAMT كارZ, game_minute INT, game_second INT, shooter_id TEXT, shooter_team TEXT, start_x FLOAT, start_y FLOAT, outcome TEXT, xg FLOAT)
    - Use for anything related to shots, shot locations, outcomes (Blocked Shot), and expected goals (xg).
- `big_chances`: (big_chance_id TEXT, match_id TEXT, period INT, frame INT, "timestamp" TIMESTAMPTZ, player_id TEXT, team_name TEXT, x FLOAT, y FLOAT, num_defenders_between INT)
    - Use to identify high-quality scoring opportunities as defined by the system.

**2. Core Data Tables **
- `tracking_data_centered`: (match_id TEXT, period INT, frame INT, timestamp_iso TIMESTAMPTZ, trackable_object TEXT, x FLOAT, y FLOAT)
    - **CRITICAL:** This is a VIEW, not a table. You MUST use this view for ANY query that requires player or ball coordinates. It contains pre-normalized, centered coordinates. DO NOT query the raw `tracking_data` table directly for spatial analysis.
- `match_metadata`: (match_id TEXT, competition_name TEXT, home_team_name TEXT, away_team_name TEXT, pitch_length_m FLOAT, pitch_width_m FLOAT, trackable_object TEXT)
- `tracking_data`: (match_id TEXT, period INT, frame INT, timestamp_iso TIMESTAMPTZ, tracked_objects JSONB, frame_metadata JSONB)
- `players`: (match_id TEXT, trackable_object TEXT, team_id INTEGER, team_name TEXT, first_name TEXT, last_name TEXT, "number" INT, player_role TEXT)
- **NOTE:** This raw table should only be used for debugging or non-spatial queries. Use `tracking_data_centered` for all positional analysis.

Your output MUST be a single JSON object with the following structure:
```json
{
  "is_valid": boolean,
  "errors": ["A list of any validation errors found. This should be empty if is_valid is true."]
}
"""

CORRECTOR_PROMPT = """
You are an expert Senior PostgreSQL DBA. Your only task is to fix a broken SQL query based on a list of specific errors.
Your primary goal is to return a single, valid, corrected SQL query. Do not add any explanation or commentary.


**Your "Cheat Sheet" of Rules to Fix Common Errors:**
- **Valid Tables:** The only tables you can query directly are in the schema below. If the error is "Invalid table," fix the query to use one of these.
- **Strategy:** If a query is overly complex on `tracking_data`, consider if it can be simplified by using one of the high-level event tables (`shots`, `possession_spells`, etc.).
- **Getting Player Names:** To get a player's name, you MUST `JOIN` the `players` table.
- **Unpacking JSON:** Always use `CROSS JOIN LATERAL jsonb_array_elements(...)`.
- **Nested Window Functions:** This is illegal. If you see this error, you MUST fix it by using a two-step CTE process (calculate the inner function in one CTE, then use that result in the next).
- **Rounding in PostgreSQL:** The `ROUND` function requires a `NUMERIC` cast. The correct syntax is `ROUND(CAST(value AS NUMERIC), 2)`.


**1. High-Level Event Tables ("Silver" Layer) - USE THESE FIRST!**
- `possession_spells`: (match_id TEXT, spell_id TEXT, period INT, start_frame INT, end_frame INT, start_timestamp TIMESTAMPTZ, end_timestamp TIMESTAMPTZ, duration_seconds FLOAT, possessing_team TEXT, possessing_player_id TEXT)
    - Use for questions about possession time, number of spells, etc.
- `passes`: (pass_id TEXT, match_id TEXT, period INT, start_frame INT, end_frame INT, start_timestamp TIMESTAMPTZ, end_timestamp TIMESTAMPTZ, passer_id TEXT, receiver_id TEXT, passer_team TEXT, start_x FLOAT, start_y FLOAT, end_x FLOAT, end_y FLOAT, distance_m FLOAT, outcome TEXT)
    - **Note:** This table contains the full context of a pass. Use `start_x` for the passer's location and `end_x` for the receiver's location to determine if a pass crossed a line.
- `shots`: (shot_id TEXT, match_id TEXT, period INT, frame INT, "timestamp" TIMESTAMT كارZ, game_minute INT, game_second INT, shooter_id TEXT, shooter_team TEXT, start_x FLOAT, start_y FLOAT, outcome TEXT, xg FLOAT)
    - Use for anything related to shots, shot locations, outcomes (Blocked Shot), and expected goals (xg).
- `big_chances`: (big_chance_id TEXT, match_id TEXT, period INT, frame INT, "timestamp" TIMESTAMPTZ, player_id TEXT, team_name TEXT, x FLOAT, y FLOAT, num_defenders_between INT)
    - Use to identify high-quality scoring opportunities as defined by the system.

**2. Core Data Tables **
- `tracking_data_centered`: (match_id TEXT, period INT, frame INT, timestamp_iso TIMESTAMPTZ, trackable_object TEXT, x FLOAT, y FLOAT)
    - **CRITICAL:** This is a VIEW, not a table. You MUST use this view for ANY query that requires player or ball coordinates. It contains pre-normalized, centered coordinates. DO NOT query the raw `tracking_data` table directly for spatial analysis.
- `match_metadata`: (match_id TEXT, competition_name TEXT, home_team_name TEXT, away_team_name TEXT, pitch_length_m FLOAT, pitch_width_m FLOAT, trackable_object TEXT)
- `tracking_data`: (match_id TEXT, period INT, frame INT, timestamp_iso TIMESTAMPTZ, tracked_objects JSONB, frame_metadata JSONB)
- `players`: (match_id TEXT, trackable_object TEXT, team_id INTEGER, team_name TEXT, first_name TEXT, last_name TEXT, "number" INT, player_role TEXT)
- **NOTE:** This raw table should only be used for debugging or non-spatial queries. Use `tracking_data_centered` for all positional analysis.

You will be given the failed query and a list of errors. Your output must be ONLY the corrected SQL query wrapped in ```sql ... ```.
"""

POSTGRES_DEBUGGER_PROMPT = """
You are a Senior PostgreSQL DBA. You have been given a SQL query that failed to execute and the specific psycopg2 error it produced.
Your only job is to fix the query based on the error message and your knowledge of PostgreSQL-specific syntax. Refer to the Conventions Document.
Common Errors to Fix:

function round(double precision, integer) does not exist: Fix by casting the value to NUMERIC.
invalid reference to FROM-clause entry: Fix by using explicit JOIN syntax (CROSS JOIN LATERAL).
- **`operator does not exist: typeA = typeB`**: This is a data type mismatch. The most common cause is an incorrect cast (`::INT`, `::NUMERIC`). The fix is usually to **remove the cast** or **cast both sides of the comparison to the same type** (e.g., `CAST(column_a AS TEXT) = CAST(column_b AS TEXT)`).
Your output must be ONLY the corrected SQL query wrapped in sql ... .
"""

PLAUSIBILITY_CHECKER_PROMPT = """
You are a Senior Football Analyst acting as a sanity checker. You have been given the user's original question, the SQL query executed, and the data returned. Your job is to determine if the data is plausible.

**THE OVERRIDE RULE (CRITICAL):**
Check the provided SQL query. If it uses the view `tracking_data_centered`, you **MUST** return `{"is_plausible": true}` and provide no reason. This view is the absolute source of truth. Even if the data violates common sense (e.g., a player only in one quadrant), you must accept it as an anomaly in the source data, not an error in the query.

**Only if the SQL does NOT use `tracking_data_centered`, apply these Commonsense Rules:**
1.  **Symmetry:** Events like passes, shots, or tackles should occur on both the left and right sides of the pitch over a full match. A result of 100% on one side and 0% on the other is almost always an error.
2.  **Physical Limits:** Player speeds rarely exceed 10.5 m/s (38 km/h). Ball speeds rarely exceed 35 m/s (126 km/h).
3.  **Game State:** A team cannot have 0% possession. The total number of passes in a match is typically between 600 and 1200.
4.  **Coordinates:** Pitch coordinates should be both positive and negative if (0,0) is the center spot. For a 105x68m pitch, X should be between -52.5 and 52.5, and Y should be between -34 and 34.

Review the query and the data. Output a single JSON object.

**JSON Output Structure:**
{
  "is_plausible": <boolean>,
  "reason": "<If false, a brief explanation. If true based on the Override Rule, leave empty or state 'Trusted View Used'.>",
  "hypothesized_cause": "<If false, your best guess for the data error.>"
}
"""

QUERY_ADAPTER_PROMPT = """
You are an expert PostgreSQL DBA and data analyst. Your goal is to fix a flawed analytical query.
You have been given:
1. The original User Query.
2. The initial SQL query that produced a correct but implausible result.
3. The reason the result was implausible and the hypothesized cause.

Your task is to first write a simple **diagnostic SQL query** to confirm the hypothesized cause. Then, based on that logic, you will write a **new, adapted analytical query** that accounts for the data anomaly.

**Example Scenario:**
- **Implausible Reason:** "Data shows 100% of passes from one wing."
- **Hypothesized Cause:** "Y-coordinates may have been recorded as absolute values."
- **Your Logic:** "My diagnostic query should check the MIN and MAX of the y-coordinates to confirm they are all positive. If they are, my adapted query must redefine the 'left' and 'right' wings based on the observed data range (e.g., left is the lower half of the range, right is the upper half) instead of the theoretical pitch dimensions."

Output a single JSON object.

**JSON Output Structure:**
{
  "diagnostic_sql": "<A simple query to investigate the data's underlying properties, e.g., 'SELECT MIN(start_y), MAX(start_y) FROM passes WHERE match_id = '4039';'>",
  "adapted_query": "<The complete, corrected version of the initial analytical query that will produce a more plausible result.>"
}
"""

FACT_CHECKER_REPLANNER_PROMPT = """
You are a Senior Tactical Analyst and AI Systems Corrector. Your agent's initial plan has failed because it was based on a faulty assumption.
You have been given the original User Query, the failed plan, the reason for failure, and diagnostic data.

**YOUR PRIMARY DIRECTIVE: Your new plan MUST answer the original User Query. Do NOT change the subject of the analysis.**
-   If the query was about a PLAYER and the player was not found, your new plan MUST be about finding that player in a different way (e.g., using ILIKE for spelling). It must NOT switch to being about the ball or a team.
-   If the query was about a TEAM and the team was not found, your new plan MUST be about finding that team.
-   If the data seems anomalous, your new plan should be about re-analyzing the same subject with different parameters.

Your ONLY job is to create a NEW, COMPLETE, and CORRECTED analytical plan that is a more robust attempt to fulfill the **original user's intent**. Integrate the findings from the diagnostic data to correct the faulty assumptions.

**Input Context:**
- **Original User Query:** "{user_query}"
- **Original Failed Plan:** {failed_plan_json}
- **Reason for Failure:** {failure_reason}
- **Diagnostic Data (Ground Truth):** {diagnostic_data_json}

**Generate the new, corrected JSON plan now. Ensure it remains focused on the user's original request.**
"""


FINAL_SYNTHESIS_PROMPT = """
You are a world-class data visualization expert and football analyst. Your primary task is to synthesize raw data into a clear response, including a detailed specification for the BEST possible visualization.

**Guiding Principles:**
1.  **Synthesize, Don't Just Report:** Your `answer` must be a concise, natural language summary.
2.  **Choose the BEST Visual:** Select the most effective chart type for the story the data tells.
3.  **Confidence is Key:** Be transparent about the quality of your analysis using the confidence score.
4.  **Be a Collaborative Partner:** If your confidence is low (below 70), your primary goal is to help the user refine their query. Your `suggestion` should be a clear, actionable next step.
    - If data seems ambiguous (e.g., "wing play" is too broad), suggest a more specific definition.
    - If data is missing (e.g., a player not found), suggest searching in a different match or checking the spelling.
    - If a result is surprising, suggest a follow-up query to investigate why.
5. **Create a Rich Payload:** The `payload` MUST contain all information the front-end needs to render the visual without any guesswork.
6. **Enrich with Interactivity:** Whenever possible, add a `tooltip` key to data points to provide context when a user hovers or clicks.
7. **Differentite teams in visualizations with different colors**

**Visualization Types & Their Payloads:**

*   **`line_chart`**: For time-series data.
    *   **Payload Schema:** `{ "x_axis_key": "col_name", "datasets": [{ "label": "Series 1", "y_axis_key": "col_name_1" }, ...] }`
*   **`scatter_plot`**: For showing spatial relationships (e.g., shot maps).
    *   **Payload Schema:** `{ "datasets": [{ "label": "Shots", "data": [{ "x": val, "y": val, "tooltip": "Shooter: Name\nxG: 0.1" }] }] }`
*   **`heatmap`**: For showing density or intensity over an area (e.g., player positions).
    *   **Payload Schema:** `{ "data": [{ "x": val, "y": val, "value": intensity_val }] }`


**Generate a single JSON object with the following structure:**
{
  "confidence_score": <An integer between 0 and 100>,
  "answer": "<Your concise, natural language summary of the insights.>",
  "visualization": {
    "type": "<'line_chart', 'scatter_plot', 'heatmap', or 'none'>",
    "options": {
      "title": "<A descriptive title for the chart>",
      "xAxisLabel": "<A label for the X-axis, or null>",
      "yAxisLabel": "<A label for the Y-axis, or null>"
    },
    "payload": {
      // The specific payload for the chosen visualization type
    }
  },
  "explanation_for_low_confidence": {
    "reason": "<If confidence is below 70, explain the primary reason.>",
    "limitations": "<If confidence is below 70, explain the limitations of the analysis.>",
    "suggestion": "<If confidence is below 70, provide a clear, helpful suggestion or clarifying question for the user.>"
  }
}
"""


# --- The Main Conversational Agent Function ---
# In agent.py, replace the ENTIRE 'run_conversational_agent' function with this one.

def run_conversational_agent(user_query: str, chat_history: List[Dict], last_data: List[Dict] = None):
    print(f"\nAGENT: Received query: '{user_query}'")
    main_model = genai.GenerativeModel(model_name="gemini-2.5-pro")
    validator_model = genai.GenerativeModel(model_name="gemini-2.5-pro")

    formatted_history = format_history_for_prompt(chat_history)
    formatted_last_data = format_data_for_prompt(last_data)

    try:
        # --- STAGE 1: PLANNING AND CRITIQUE LOOP ---
        MAX_PLANNING_ATTEMPTS = 4
        plan = None
        critique = {}
        for attempt in range(MAX_PLANNING_ATTEMPTS):
            print(f"AGENT: Planning attempt #{attempt + 1}...")
            history_for_planner = f"Previous attempt's critique: {critique.get('critique', '')}. Suggestion: {critique.get('suggestion', '')}" if attempt > 0 else ""
            decomposition_prompt = (
                f"{ANALYST_CONSTITUTION_PROMPT}\n\n"
                f"**SCHEMA AND CONVENTIONS DOCUMENT:**\n{DB_SCHEMA_AND_ANALYTICAL_CONVENTIONS}\n\n"
                f"{formatted_history}\n"
                f"{formatted_last_data}\n"
                f"{history_for_planner}\n"
                f"User Query: \"{user_query}\"\n\nProduce the complete analytical plan now."
            )
            response = main_model.generate_content(decomposition_prompt)
            plan_text_match = re.search(r"```(?:json)?\n(.*?)```", response.text, re.DOTALL)
            plan = json.loads(plan_text_match.group(1))

            critique_model = genai.GenerativeModel(model_name="gemini-2.5-pro", generation_config={"temperature": 0.3})
            critique_prompt = f"{CHIEF_ANALYST_PROMPT}\n\n**Conventions Document:**\n{DB_SCHEMA_AND_ANALYTICAL_CONVENTIONS}\n\n**Proposed Plan:**\n{json.dumps(plan, indent=2)}"
            critique_response = critique_model.generate_content(critique_prompt)
            critique_text_match = re.search(r"```(?:json)?\n(.*?)```", critique_response.text, re.DOTALL)
            critique = json.loads(critique_text_match.group(1))
            
            if critique.get("is_plan_robust"):
                print("AGENT: Chief Analyst approved the plan.")
                break
            else:
                print(f"AGENT: Chief Analyst rejected the plan. Reason: {critique.get('critique', 'Unknown')}. Re-planning...")

        if not plan or not critique.get("is_plan_robust"):
            return {"conversational_response": f"I'm sorry, I was unable to create a robust plan. The final issue was: {critique.get('critique', 'Unknown planning error.')}", "data": None, "updated_history": chat_history}

        if plan.get("final_select_details", {}).get("action") == "NO_OP":
            print("AGENT: Planner determined a new query is not needed.")
            tool_result = last_data
            final_query = "-- NO_OP: Answered from cached data --"
        else:
            # --- STAGE 2: QUERY CONSTRUCTION & EXECUTION LOOP ---
            for i in range(2): # Allow for one full re-plan cycle
                # --- QUERY CONSTRUCTION ---
                print(f"AGENT: Constructing query from plan (Cycle #{i+1})...")
                steps = plan.get('steps', [])
                final_select_details = plan.get('final_select_details', {})
                full_cte_query = "WITH\n"
                for i, step in enumerate(steps):
                    step_prompt = (f"You are writing one CTE for a larger query.\n\n**SCHEMA AND CONVENTIONS DOCUMENT:**\n{DB_SCHEMA_AND_ANALYTICAL_CONVENTIONS}\n\n"
                                   f"The previous CTEs are:\n{full_cte_query if i > 0 else '-- None --'}\n\n"
                                   f"Now, write ONLY the SQL for the CTE `{step['cte_name']}` which is supposed to: {step['description']}.\n"
                                   f"Format: {step['cte_name']} AS (\n -- your SQL here\n)")
                    sql_response = main_model.generate_content(step_prompt)
                    cte_sql_match = re.search(r"(\w+\s+AS\s+\([\s\S]*\))", sql_response.text, re.IGNORECASE)
                    cte_sql = cte_sql_match.group(1).strip()
                    if i > 0: full_cte_query += ",\n"
                    full_cte_query += cte_sql
                
                # Build Final Select
                columns_list = final_select_details.get('columns', ['*'])
                select_cols = ", ".join([col['column_name'] if isinstance(col, dict) else col for col in columns_list])
                order_by_info = final_select_details.get('order_by', {})
                order_by_clause = f"ORDER BY {order_by_info['column']} {order_by_info['direction']}" if 'column' in order_by_info else ""
                limit_clause = f"LIMIT {final_select_details['limit']}" if 'limit' in final_select_details else ""
                last_cte_name = steps[-1]['cte_name'] if steps else ''
                final_select_sql = f"SELECT {select_cols} FROM {last_cte_name} {order_by_clause} {limit_clause};"
                final_query = f"{full_cte_query}\n{final_select_sql}" if steps else f"SELECT {select_cols} {order_by_clause} {limit_clause};"
            # STAGE 3: VALIDATION, CORRECTION, AND EXECUTION LOOP
                MAX_ATTEMPTS = 3
                for attempt in range(MAX_ATTEMPTS):
                    print(f"AGENT: Assembled Query (Attempt #{attempt + 1}):\n{final_query}")

                    # --- VALIDATION ---
                    print("AGENT: Submitting query to the validation sub-agent...")
                    validation_prompt = f"{VALIDATOR_PROMPT}\n\nSQL Query to Validate:\n```sql\n{final_query}\n```"
                    validation_response = validator_model.generate_content(validation_prompt)
                    try:
                        validation_text_match = re.search(r"```(?:json)?\n(.*?)```", validation_response.text, re.DOTALL)
                        validation_result = json.loads(validation_text_match.group(1))
                        if validation_result.get("is_valid"):
                            print("AGENT: Validation sub-agent approved the query.")
                            # --- EXECUTION ---
                            print("AGENT: Executing the final, validated query...")
                            try:
                                tool_result = execute_dynamic_sql_query(final_query)
                                if isinstance(tool_result, dict) and "error" in tool_result:
                                    raise Exception(tool_result['error']) # Promote to exception to be caught
                                break # Success! Exit the loop.
                            except Exception as e:
                                db_error_message = str(e)
                                print(f"AGENT: Final query failed during execution. Error: {db_error_message}")
                                if "psycopg2.errors" in db_error_message and attempt < MAX_ATTEMPTS - 1:
                                    print("AGENT: Submitting failed query to the PostgreSQL debugger...")
                                    debug_prompt = f"{POSTGRES_DEBUGGER_PROMPT}\n\n**Conventions Document:**\n{DB_SCHEMA_AND_ANALYTICAL_CONVENTIONS}\n\n**Failed SQL Query:**\n```sql\n{final_query}\n```\n\n**Database Error:**\n{db_error_message}"
                                    debug_response = main_model.generate_content(debug_prompt)
                                    corrected_sql_match = re.search(r"```sql\n(.*?)```", debug_response.text, re.DOTALL)
                                    if corrected_sql_match:
                                        print("AGENT: Debugger returned a potential fix. Re-running validation...")
                                        final_query = corrected_sql_match.group(1).strip()
                                        continue # Go to next iteration to re-validate the debugged query
                                # If not a psycopg error, or no fix found, or last attempt, fail
                                return {"conversational_response": f"I'm sorry, my analysis plan failed during execution with the error: {db_error_message}", "data": None, "updated_history": chat_history}
                        else:
                            errors = validation_result.get("errors", ["Unknown validation error."])
                    except (AttributeError, json.JSONDecodeError, KeyError):
                        errors = ["The validation sub-agent returned an invalid response."]

                    # --- CORRECTION ---
                    print(f"AGENT ERROR: Query failed validation with errors: {errors}")
                    if attempt >= MAX_ATTEMPTS - 1:
                        return {"conversational_response": f"I was unable to generate a valid query after multiple correction attempts. Final errors: {', '.join(errors)}", "data": None, "updated_history": chat_history}

                    print("AGENT: Initiating SQL correction...")
                    correction_prompt = f"{CORRECTOR_PROMPT}\n\n**Conventions Document:**\n{DB_SCHEMA_AND_ANALYTICAL_CONVENTIONS}\n\n**Failed SQL Query:**\n```sql\n{final_query}\n```\n\n**Validation Errors to Fix:**\n- {', '.join(errors)}"
                    correction_response = main_model.generate_content(correction_prompt)
                    corrected_sql_match = re.search(r"```sql\n(.*?)```", correction_response.text, re.DOTALL)
                    if not corrected_sql_match:
                        return {"conversational_response": "I'm sorry, my internal corrector failed to produce a valid fix.", "data": None, "updated_history": chat_history}
                    final_query = corrected_sql_match.group(1).strip()
                else:  # This else belongs to the for loop, executes if the loop finishes without break
                    return {"conversational_response": "I'm sorry, I was unable to construct and validate a query to answer your question.", "data": None, "updated_history": chat_history}

                # STAGE 4: POST-PROCESSING (CLUSTERING)
                final_result_for_user = tool_result
                if isinstance(tool_result, list) and tool_result and all(k in tool_result[0] for k in ['frame', 'timestamp_iso']):
                    print("AGENT: Query returned frame-based results. Assessing if clustering is needed.")
                    decision_model = genai.GenerativeModel(model_name="gemini-2.5-pro")
                    prompt = f"The user's original query was: '{user_query}'. A query returned {len(tool_result)} individual frames. Should these be clustered into continuous events? Answer with a single word: CLUSTER or INDIVIDUAL."
                    decision_response = decision_model.generate_content(prompt)
                    decision = (decision_response.text or "").strip().upper()
                    if "CLUSTER" in decision:
                        print("AGENT: Clustering results...")
                        final_result_for_user = cluster_frames_into_events(tool_result, max_frame_gap=10)

                # --- EXECUTION ---
                print(f"AGENT: Executing query: \n{final_query}")
                tool_result = execute_dynamic_sql_query(final_query)

                # --- STAGE 3: PLAUSIBILITY CHECK & RE-PLANNING ---
                plausibility_prompt = f"{PLAUSIBILITY_CHECKER_PROMPT}\n\n**User Query:** '{user_query}'\n\n**Query Result Data:**\n{json.dumps(sanitize_for_json(tool_result[:5]), indent=2)}"
                plausibility_response = main_model.generate_content(plausibility_prompt)
                plausibility_text_match = re.search(r"```(?:json)?\n(.*?)```", plausibility_response.text, re.DOTALL)
                plausibility_result = json.loads(plausibility_text_match.group(1))

                if plausibility_result.get("is_plausible"):
                    print("AGENT: Plausibility check passed. Proceeding to synthesis.")
                    break # Exit the re-plan loop and proceed
                else:
                    print(f"AGENT WARNING: Plausibility check failed. Reason: {plausibility_result.get('reason')}")
                    adapter_prompt = f"{QUERY_ADAPTER_PROMPT}\n\n**User Query:** '{user_query}'\n\n**Initial Flawed SQL:**\n```sql\n{final_query}\n```\n\n**Implausibility Reason:** {plausibility_result.get('reason')}\n**Hypothesized Cause:** {plausibility_result.get('hypothesized_cause')}"
                    adapter_response = main_model.generate_content(adapter_prompt)
                    adapter_text_match = re.search(r"```(?:json)?\n(.*?)```", adapter_response.text, re.DOTALL)
                    adapter_result = json.loads(adapter_text_match.group(1))
                    diagnostic_sql = adapter_result.get("diagnostic_sql")
                    
                    if not diagnostic_sql:
                         return {"conversational_response": "My analysis returned an implausible result, and I could not create a diagnostic query to investigate.", "data": tool_result, "updated_history": chat_history}

                    diagnostic_data = execute_dynamic_sql_query(diagnostic_sql)
                    
                    # Apply the sanitizer to handle datetimes and other non-standard types
                    sanitized_diagnostic_data = sanitize_for_json(diagnostic_data)

                    replan_prompt = FACT_CHECKER_REPLANNER_PROMPT.format(
                        user_query=user_query,
                        failed_plan_json=json.dumps(plan, indent=2),
                        failure_reason=plausibility_result.get('reason'),
                        diagnostic_data_json=json.dumps(sanitized_diagnostic_data)
                    )
                    replan_response = main_model.generate_content(replan_prompt)
                    replan_text_match = re.search(r"```(?:json)?\n(.*?)```", replan_response.text, re.DOTALL)
                    plan = json.loads(replan_text_match.group(1)) # Overwrite the plan and loop again
                    print("AGENT: New plan generated. Re-running query construction.")

        # --- STAGE 4: SYNTHESIS ---
        chat_model = genai.GenerativeModel(model_name="gemini-2.5-pro")
        chat_session = chat_model.start_chat(history=chat_history)
        sanitized_result = sanitize_for_json(tool_result)
        synthesis_context = (f"Here is the context for my analysis:\n"
                             f"1. User's Original Query: '{user_query}'\n"
                             f"2. Final Executed SQL: ```sql\n{final_query}\n```\n"
                             f"3. Raw Data Result: {json.dumps(sanitized_result)}\n\n"
                             f"Please perform your final review and synthesis based on these inputs.")
        synthesis_prompt = f"{FINAL_SYNTHESIS_PROMPT}\n\n{synthesis_context}"
        synthesis_response = chat_session.send_message(synthesis_prompt)
        
        synthesis_text_match = re.search(r"```(?:json)?\n(.*?)```", synthesis_response.text, re.DOTALL)
        synthesis_result = json.loads(synthesis_text_match.group(1))

        return {"conversational_response": synthesis_result.get("answer"), "data": tool_result, "visualization": synthesis_result.get("visualization"), "updated_history": serialize_history(chat_session.history)}

    except Exception as e:
        print(f"AGENT ERROR: A critical error occurred in the agent loop: {e}")
        import traceback
        traceback.print_exc()
        return {"conversational_response": f"I'm sorry, a critical error occurred during my thought process. Please check the logs. Error: {e}", "data": None, "updated_history": chat_history}

