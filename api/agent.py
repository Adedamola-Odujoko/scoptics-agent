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


# --- The Agent's "Brain": The Single Source of Truth ---
DB_SCHEMA_AND_ANALYTICAL_CONVENTIONS = """
**DATABASE SCHEMA AND ANALYTICAL CONVENTIONS**

This is the definitive guide to the database. All generated SQL MUST strictly adhere to these rules.

**--- STRATEGY: PRIORITIZE EVENT TABLES ---**
Your primary strategy is to answer questions using the high-level Event Tables (`possession_spells`, `shots`, `frame_spatial_metrics`) whenever possible. These tables are fast and contain pre-calculated, robust data. Only fall back to querying the raw `tracking_data` table for questions that cannot be answered by the event tables.

**1. High-Level Event Tables ("Silver" Layer)**
- `possession_spells`: (match_id, spell_id, period, start_frame, end_frame, start_time, duration_seconds, possessing_team, possessing_player_id)
- `shots`: (shot_id, match_id, period, frame, shooter_id, shooter_team, start_x, start_y, outcome, xg)
- `frame_spatial_metrics`: (match_id, period, frame, home_team_compactness, away_team_compactness)

**2. Core Data Tables ("Bronze" Layer)**
- `match_metadata`: (match_id, competition_name, home_team_name, away_team_name, pitch_length_m, pitch_width_m)
- `tracking_data`: (match_id, period, frame, timestamp_iso, tracked_objects JSONB, frame_metadata JSONB)
- `players`: (match_id, trackable_object, first_name, last_name, "number" INT, player_role)

**3. CRITICAL ANALYTICAL RULES & BEST PRACTICES**
- **Rule 0: Prioritize Event Tables:** Before planning a complex query on `tracking_data`, first check if the question can be answered with a simple `SELECT` from the event tables.
- **Rule 1: Finding the Match Half:** ALWAYS use the dedicated top-level `period` column.
- **Rule 2: Identifying Players (Raw Data):** In `tracking_data`, a player is an object where `(p.obj ->> 'group_name') IS NOT NULL`.
- **Rule 3: Identifying Outfield Players (Raw Data):** Exclude the goalkeeper using `(p.obj ->> 'trackable_object') <> '1'`.
- **Rule 4: SQL Join Syntax:** MUST use explicit `JOIN` keywords. For `LATERAL`, use `CROSS JOIN LATERAL`.
- **Rule 5: Getting Player Names:** `JOIN` the `players` table using `trackable_object` as the key.
- **Rule 6: Calculating Durations (Raw Data):** MUST use `timestamp_iso`. Do NOT rely on frame counts.
- **Rule 7: Rounding in PostgreSQL:** MUST cast values to `NUMERIC` before using `ROUND`.
- **Rule 8: Sprint Speed Filter (Raw Data):** Filter out unrealistic speeds (`WHERE speed_mps < 13.0`).
- **Rule 9: No Nested Window Functions:** Use a two-step CTE process if a window function result needs further calculation.
- **Rule 10: Possession (Raw Data):** To find the possessing team, get the `trackable_object` from `frame_metadata -> 'possession'` and look up its `group_name` in the `tracked_objects` array for that frame.
"""

# --- Agent Prompts ---

ANALYST_CONSTITUTION_PROMPT = """
You are ScopticsAI, a world-class football tactical analyst. Your primary purpose is to answer complex questions by creating a complete analytical plan.
Your plan MUST be a single JSON object. You MUST follow all rules in the SCHEMA and CONVENTIONS document.
```json
{
  "explanation": "A brief, one-sentence explanation of your overall plan.",
  "steps": [
    {"step_number": 1, "description": "A short description of what this step calculates.", "cte_name": "Step1CTE"}
  ],
  "final_select_details": {
    "columns": ["col1", "col2"],
    "order_by": {"column": "col1", "direction": "ASC"},
    "limit": 3
  }
}
```
"""

CHIEF_ANALYST_PROMPT = """
You are a pragmatic and experienced Chief Tactical Analyst. Your role is to be a helpful senior partner, not a pessimistic critic.
You are reviewing a plan from a junior analyst. Your goal is to catch major, obvious logical flaws while allowing reasonable plans to proceed.

**Your Guiding Principles:**
- **Pragmatism over Perfection:** The plan does not need to be the absolute most optimal query in the world. It just needs to be logically sound and directly answer the user's question according to the Conventions Document.
- **Trust the Rules:** If a plan clearly follows the rules in the Conventions Document (e.g., it uses `timestamp_iso` for durations, it uses the `period` column for halves), you should approve it.
- **Simple is Good:** If the user's request is simple and the plan is simple and direct, approve it. Do not over-complicate the review.

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
1.  **Table Names:** The ONLY valid base tables are `match_metadata`, `tracking_data`, `players`, `possession_spells`, `shots`, and `frame_spatial_metrics`. No other table names are allowed in a `FROM` or `JOIN` clause unless they are a Common Table Expression (CTE) defined within the query itself.
2.  **Column Names:** All column names used must exist in the table schemas provided below.
3.  **JSON Structure:** The `tracked_objects` JSONB array in the `tracking_data` table MUST be unpacked using `LATERAL jsonb_array_elements()`.
4.  **SQL Join Syntax:** Do not mix comma-style joins (e.g., `FROM table_a, table_b`) with the `JOIN` keyword. For `LATERAL` unnesting, the required syntax is `CROSS JOIN LATERAL`.
5.  **Window Functions:** Window functions (like `LAG` or `SUM() OVER(...)`) cannot be nested.
6.  **Quotes:** `match_id` is TEXT and must be in single quotes in `WHERE` clauses (e.g., `match_id = '4039'`).

**Schemas:**
- **Event Tables (High-Level):**
  - `possession_spells`: (match_id, spell_id, period, start_frame, end_frame, start_time, duration_seconds, possessing_team, possessing_player_id)
  - `shots`: (shot_id, match_id, period, frame, shooter_id, shooter_team, start_x, start_y, outcome, xg)
  - `frame_spatial_metrics`: (match_id, period, frame, home_team_compactness, away_team_compactness)
- **Core Data Tables (Low-Level):**
  - `match_metadata`: (match_id, competition_name, home_team_name, away_team_name, pitch_length_m, pitch_width_m)
  - `players`: (match_id, trackable_object, first_name, last_name, "number" INT, player_role TEXT)
  - `tracking_data`: (match_id, period, frame, timestamp_iso TIMESTAMPTZ, tracked_objects JSONB, frame_metadata JSONB)

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
- **Valid Tables:** The only tables you can query directly are `match_metadata`, `tracking_data`, `players`, `possession_spells`, `shots`, and `frame_spatial_metrics`. If the error is "Invalid table," fix the query to use one of these.
- **Strategy:** If a query is overly complex on `tracking_data`, consider if it can be simplified by using one of the high-level event tables (`shots`, `possession_spells`, etc.).
- **Getting Player Names:** To get a player's name, you MUST `JOIN` the `players` table.
- **Unpacking JSON:** Always use `CROSS JOIN LATERAL jsonb_array_elements(...)`.
- **Nested Window Functions:** This is illegal. If you see this error, you MUST fix it by using a two-step CTE process (calculate the inner function in one CTE, then use that result in the next).
- **Rounding in PostgreSQL:** The `ROUND` function requires a `NUMERIC` cast. The correct syntax is `ROUND(CAST(value AS NUMERIC), 2)`.

You will be given the failed query and a list of errors. Your output must be ONLY the corrected SQL query wrapped in ```sql ... ```.
"""

POSTGRES_DEBUGGER_PROMPT = """
You are a Senior PostgreSQL DBA. You have been given a SQL query that failed to execute and the specific psycopg2 error it produced.
Your only job is to fix the query based on the error message and your knowledge of PostgreSQL-specific syntax. Refer to the Conventions Document.
Common Errors to Fix:

function round(double precision, integer) does not exist: Fix by casting the value to NUMERIC.
invalid reference to FROM-clause entry: Fix by using explicit JOIN syntax (CROSS JOIN LATERAL).
Your output must be ONLY the corrected SQL query wrapped in sql ... .
"""
FINAL_SYNTHESIS_PROMPT = """
You are the final review layer of a sports data AI. Your task is to review the entire analytical process and formulate a user-facing response.
Evaluate the result and provide a confidence score.
You will be given the user's query, the final SQL, and the raw data result.
Generate a single JSON object.
{
  "confidence_score": <An integer between 0 and 100>,
  "answer": "<A concise, natural language summary of the data result for the user.",
  "explanation_for_low_confidence": {
    "reason": "<If confidence is below 70, explain the primary reason. E.g., 'The query returned no data.'>",
    "limitations": "<If confidence is below 70, explain the limitations. E.g., 'This only considers the first half.'>",
    "request_for_more_info": "<If confidence is below 70, ask a clarifying question. E.g., 'Would you like to broaden the search criteria?'>"
  }
}
Confidence Score Guide:

90-100%: Specific query, direct SQL, concrete non-empty data returned.
70-89%: Mostly answered, but assumptions were made or data was sparse.
Below 70%: Returned no results, request was ambiguous, or data was insufficient. You MUST fill out the explanation.
"""
# --- The Main Conversational Agent Function ---
def run_conversational_agent(user_query: str, chat_history: List[Dict]):
    print(f"\nAGENT: Received query: '{user_query}'")
    main_model = genai.GenerativeModel(model_name="gemini-2.5-pro")
    validator_model = genai.GenerativeModel(model_name="gemini-2.5-pro")

    # --- NEW STAGE 1: PLANNING AND CRITIQUE LOOP ---
    MAX_PLANNING_ATTEMPTS = 4
    plan = None
    for attempt in range(MAX_PLANNING_ATTEMPTS):
        print(f"AGENT: Planning attempt #{attempt + 1}...")

        # STAGE 1A: DECOMPOSITION & PLANNING
        # On subsequent attempts, we give the agent the critique to help it improve.
        history_for_planner = f"Previous attempt's critique: {critique['critique']}. Suggestion: {critique['suggestion']}" if attempt > 0 else ""
        decomposition_prompt = (
            f"{ANALYST_CONSTITUTION_PROMPT}\n\n"
            f"**SCHEMA AND CONVENTIONS DOCUMENT:**\n{DB_SCHEMA_AND_ANALYTICAL_CONVENTIONS}\n\n"
            f"{history_for_planner}\n\n"
            f"User Query: \"{user_query}\"\n\nProduce the complete analytical plan now."
        )
        response = main_model.generate_content(decomposition_prompt)
        generation_config={"temperature": 1.5}

        try:
            plan_text_match = re.search(r"```(?:json)?\n(.*?)```", response.text, re.DOTALL)
            plan = json.loads(plan_text_match.group(1))
        except (AttributeError, json.JSONDecodeError, KeyError) as e:
            if attempt >= MAX_PLANNING_ATTEMPTS - 1:
                print(f"AGENT ERROR: The planner failed to return a valid JSON plan. Error: {e}")
                return {"conversational_response": "I'm sorry, I was unable to create a valid plan to answer your question.", "data": None, "updated_history": chat_history}
            continue # Try planning again

        # STAGE 1B: INNER MONOLOGUE (PLAN CRITIQUE)
        print("AGENT: Submitting plan to the Chief Analyst for review...")
        # Create a dedicated, less-creative model for the critique
        critique_model = genai.GenerativeModel(
            model_name="gemini-2.5-pro",
            generation_config={"temperature": 1} # <-- THE CHANGE IS HERE
        )
        critique_prompt = f"{CHIEF_ANALYST_PROMPT}\n\n**Conventions Document:**\n{DB_SCHEMA_AND_ANALYTICAL_CONVENTIONS}\n\n**Proposed Plan:**\n{json.dumps(plan, indent=2)}"
        critique_response = critique_model.generate_content(critique_prompt)
        try:
            critique_text_match = re.search(r"```(?:json)?\n(.*?)```", critique_response.text, re.DOTALL)
            critique = json.loads(critique_text_match.group(1))
            if critique.get("is_plan_robust"):
                print("AGENT: Chief Analyst approved the plan.")
                break # The plan is good, exit the loop!
            else:
                print(f"AGENT: Chief Analyst rejected the plan. Reason: {critique['critique']}. Re-planning...")
                # The loop will continue to the next attempt
        except (AttributeError, json.JSONDecodeError, KeyError) as e:
            print(f"AGENT WARNING: The plan critique phase failed. Assuming plan is okay. Error: {e}")
            break # Exit loop and proceed with the current plan if critique fails

    # After the loop, check if we ever got a valid plan
    if not plan or not critique.get("is_plan_robust"):
        return {"conversational_response": f"I'm sorry, I was unable to create a robust plan after multiple attempts. The final issue was: {critique.get('critique', 'Unknown planning error.')}", "data": None, "updated_history": chat_history}

    # Now extract the final, approved plan details
    steps = plan['steps']
    final_select_details = plan['final_select_details']
    print(f"AGENT: Final plan approved. Explanation: {plan['explanation']}")
    # STAGE 2: QUERY CONSTRUCTION
    final_query = ""
    full_cte_query = "WITH\n"
    for i, step in enumerate(steps):
        print(f"AGENT: Generating SQL for Step {step['step_number']}: {step['description']}")
        step_prompt = (f"You are writing one CTE for a larger query.\n\n**SCHEMA AND CONVENTIONS DOCUMENT:**\n{DB_SCHEMA_AND_ANALYTICAL_CONVENTIONS}\n\n"
                       f"The previous CTEs are:\n{full_cte_query if i > 0 else '-- None --'}\n\n"
                       f"Now, write ONLY the SQL for the CTE `{step['cte_name']}` which is supposed to: {step['description']}.\n"
                       f"Format: {step['cte_name']} AS (\n -- your SQL here\n)")
        sql_response = main_model.generate_content(step_prompt)
        cte_sql_match = re.search(r"(\w+\s+AS\s+\([\s\S]*\))", sql_response.text, re.IGNORECASE)
        if not cte_sql_match:
            return {"conversational_response": f"I'm sorry, I failed to construct the query at step {step['step_number']}.", "data": None, "updated_history": chat_history}
        cte_sql = cte_sql_match.group(1).strip()
        if i > 0:
            full_cte_query += ",\n"
        full_cte_query += cte_sql

    try:
        columns_list = final_select_details['columns']
        select_cols = ", ".join([col['column_name'] if isinstance(col, dict) else col for col in columns_list])
        order_by_info = final_select_details.get('order_by') or {}
        order_by_clause = f"ORDER BY {order_by_info['column']} {order_by_info['direction']}" if 'column' in order_by_info else ""
        limit = final_select_details.get('limit')
        limit_clause = f"LIMIT {limit}" if limit is not None else ""
        last_cte_name = steps[-1]['cte_name']
        final_select_sql = f"SELECT {select_cols} FROM {last_cte_name} {order_by_clause} {limit_clause};"
        final_query = f"{full_cte_query}\n{final_select_sql}"
    except (KeyError, IndexError, TypeError) as e:
        return {"conversational_response": f"I'm sorry, my plan was incomplete and I could not build the final query. Error: {e}", "data": None, "updated_history": chat_history}


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

    # STAGE 5: SUMMARIZATION & CONFIDENCE SCORING
    chat_model = genai.GenerativeModel(model_name="gemini-2.5-pro")
    chat_session = chat_model.start_chat(history=chat_history)
    sanitized_result = sanitize_for_json(final_result_for_user)
    synthesis_context = (f"Here is the context for my analysis:\n"
                         f"1. User's Original Query: '{user_query}'\n"
                         f"2. Final Executed SQL: ```sql\n{final_query}\n```\n"
                         f"3. Raw Data Result: {json.dumps(sanitized_result)}\n\n"
                         f"Please perform your final review and synthesis based on these inputs.")
    synthesis_prompt = f"{FINAL_SYNTHESIS_PROMPT}\n\n{synthesis_context}"
    synthesis_response = chat_session.send_message(synthesis_prompt)

    try:
        synthesis_text_match = re.search(r"```(?:json)?\n(.*?)```", synthesis_response.text, re.DOTALL)
        synthesis_result = json.loads(synthesis_text_match.group(1))
        confidence = synthesis_result.get("confidence_score", 100)
        final_answer = synthesis_result.get("answer", "I have processed your request but encountered an issue generating a summary.")
        if confidence < 70:
            explanation = synthesis_result.get("explanation_for_low_confidence", {})
            reason = explanation.get("reason", "No specific reason was provided.")
            limitations = explanation.get("limitations", "No limitations were specified.")
            request = explanation.get("request_for_more_info", "")
            final_answer += f"\n\n**Note (Confidence: {confidence}%)**\n*   **Reason for Low Confidence:** {reason}\n*   **Limitations:** {limitations}\n*   **Suggestion:** {request}"
        return {"conversational_response": final_answer, "data": final_result_for_user, "updated_history": serialize_history(chat_session.history)}
    except (AttributeError, json.JSONDecodeError, KeyError) as e:
        print(f"AGENT ERROR: The synthesis model failed to return a valid JSON response. Error: {e}")
        return {"conversational_response": synthesis_response.text or "I have processed your request but had trouble formulating my final summary.", "data": final_result_for_user, "updated_history": chat_history}
