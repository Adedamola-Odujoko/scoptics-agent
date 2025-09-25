import os
import re
import json
from dotenv import load_dotenv
import google.generativeai as genai
from typing import List, Dict, Any
from datetime import datetime

# --- (All imports remain the same) ---
from google.generativeai.types import FunctionDeclaration
Part = genai.protos.Part
FunctionResponse = genai.protos.FunctionResponse
from scoptics_agent.events.clustering import cluster_frames_into_events
from .retrieval import execute_dynamic_sql_query

# --- (Configuration and Helper functions remain the same) ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY environment variable not set.")
genai.configure(api_key=GOOGLE_API_KEY)

# Helper functions
def sanitize_for_json(data: Any) -> Any:
    if isinstance(data, list):
        return [sanitize_for_json(item) for item in data]
    if isinstance(data, dict):
        return {key: sanitize_for_json(value) for key, value in data.items()}
    if isinstance(data, datetime):
        return data.isoformat()
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


def cleanup_schema(s_dict: Dict) -> Dict:
    keys_to_remove = ['title', 'additionalProperties', 'default']
    for key in keys_to_remove:
        if key in s_dict: del s_dict[key]
    if 'properties' in s_dict:
        for prop_value in s_dict['properties'].values():
            if isinstance(prop_value, dict): cleanup_schema(prop_value)
    return s_dict

# --- The NEW "Analyst's Constitution" Prompt - Now with Final Select Details ---
ANALYST_CONSTITUTION_PROMPT = """
You are ScopticsAI, a world-class football tactical analyst. Your primary purpose is to answer complex questions by creating a complete analytical plan, including breaking the problem into sequential steps (CTEs) and defining the final output.

**Your Task: Create a Complete Analytical Plan**
Given a user's query, you must produce a single JSON object that contains:
1.  A brief explanation of your overall strategy.
2.  A list of sequential steps, where each step will become a SQL Common Table Expression (CTE).
3.  The details for constructing the final SELECT statement that will use these CTEs.

**Core Principles:**
1.  **Simplify:** Each step should be as simple as possible.
2.  **Be Robust:** Acknowledge the data is from a sparse broadcast feed. Prefer reliable metrics over fragile ones.
3.  **Adhere to the Schema and SQL Rules:**
    - `match_metadata`: (match_id TEXT, ...)
    - `tracking_data`: (match_id TEXT, frame INT, tracked_objects JSONB, ...)
    - `match_id` is TEXT and must be in single quotes.
    - To unpack `tracked_objects`, you MUST use `LATERAL jsonb_array_elements(tracked_objects) AS obj`.
    - Do NOT nest window functions. Use a two-step CTE process if needed.
    - Each CTE you generate in the iterative process MUST be a complete, syntactically valid block, including the closing parenthesis `)`.

Your output MUST be a single JSON object with the following structure and nothing else:
```json
{
  "explanation": "A brief, one-sentence explanation of your overall plan.",
  "steps": [
    {"step_number": 1, "description": "A short description of what this step calculates.", "cte_name": "Step1CTE"},
    {"step_number": 2, "description": "Description of step 2.", "cte_name": "Step2CTE"}
  ],
  "final_select_details": {
    "columns": ["col1", "col2"],
    "order_by": {"column": "col1", "direction": "ASC"},
    "limit": 3
  }
}
```
"""
# --- The NEW "Validator Sub-Agent" Prompt ---
VALIDATOR_PROMPT = """
You are a Senior PostgreSQL Database Administrator. Your only job is to validate a given SQL query for correctness.
Check the query against the following rules and return a JSON object with your findings.

**Rules to Enforce:**
1.  **Table Names:** The ONLY valid tables are `match_metadata` and `tracking_data`. No other table names (like `Events`, `BallPositions`, etc.) are allowed in a `FROM` or `JOIN` clause unless they are a Common Table Expression (CTE) defined within the query itself.
2.  **Column Names:** All column names used must exist in the table schemas provided below.
3.  **JSON Structure:** The `tracked_objects` column is a JSONB **array of objects**. You MUST use `LATERAL jsonb_array_elements()` to unpack it. The `frame_metadata` column is a JSONB **object**.
4.  **Quotes:** `match_id` is TEXT and must be in single quotes in `WHERE` clauses.
5.  **Window Functions:** Do not nest window functions.

**Schemas:**
- `match_metadata`: (match_id TEXT, home_team_name TEXT, away_team_name TEXT, pitch_length_m FLOAT, pitch_width_m FLOAT)
- `tracking_data`: (match_id TEXT, frame INT, timestamp_iso TIMESTAMPTZ, tracked_objects JSONB, frame_metadata JSONB)
  - Inside `tracked_objects`: `[{"track_id": int, "trackable_object": str, "group_name": str, "x": float, "y": float, "z": float}, ...]`

Your output MUST be a single JSON object with the following structure:
```json
{
  "is_valid": boolean,
  "errors": ["A list of any validation errors found. This should be empty if is_valid is true."]
}
"""
# --- Main Conversational Agent Logic (Rebuilt with Deterministic Final Step) ---

def run_conversational_agent(user_query: str, chat_history: List[Dict]):
    print(f"\nAGENT: Received query: '{user_query}'")
    
    main_model = genai.GenerativeModel(model_name="gemini-2.5-pro")
    validator_model = genai.GenerativeModel(model_name="gemini-2.5-pro")

    # --- STAGE 1 & 2: PLANNING, VALIDATION, AND SELF-HEALING LOOP ---
    MAX_HEALING_ATTEMPTS = 2
    healing_attempts = 0
    final_query = ""
    is_valid = False
    plan = {} # Initialize plan outside the loop

    while healing_attempts <= MAX_HEALING_ATTEMPTS and not is_valid:
        # --- SUB-STAGE A: GET A PLAN (OR A CORRECTED PLAN) ---
        if healing_attempts == 0:
            print("AGENT: Decomposing the query into a multi-step plan...")
            plan_prompt = f"{ANALYST_CONSTITUTION_PROMPT}\n\nUser Query: \"{user_query}\"\n\nProduce the complete analytical plan now."
        else: # This is a healing attempt
            plan_prompt = (
                f"{ANALYST_CONSTITUTION_PROMPT}\n\n"
                f"The previous analytical plan for the query '{user_query}' produced a SQL query that failed validation with these errors:\n"
                f"- {', '.join(errors)}\n\n"
                f"Please review your entire plan and generate a new, corrected JSON plan from scratch that fixes these specific issues."
            )
        
        response = main_model.generate_content(plan_prompt)
        try:
            plan_text_match = re.search(r"```(?:json)?\n(.*?)```", response.text, re.DOTALL)
            plan = json.loads(plan_text_match.group(1))
            steps = plan['steps']
            final_select_details = plan['final_select_details']
            print(f"AGENT: Plan received. Explanation: {plan['explanation']}")
        except (AttributeError, json.JSONDecodeError, KeyError) as e:
            print(f"AGENT ERROR: The planner failed to return a valid JSON plan. Error: {e}")
            return {"conversational_response": "I'm sorry, I was unable to create a valid plan for your question.", "data": None, "updated_history": chat_history}

        # --- SUB-STAGE B: CONSTRUCT THE QUERY FROM THE PLAN ---
        DB_SCHEMA_CONTEXT = """
**DATABASE SCHEMA FOR REFERENCE:**
- **match_metadata** (match_id TEXT, home_team_name TEXT, away_team_name TEXT, pitch_length_m FLOAT, pitch_width_m FLOAT)
- **tracking_data** (match_id TEXT, frame INT, timestamp_iso TIMESTAMPTZ, tracked_objects JSONB, frame_metadata JSONB)
  - Inside `tracked_objects`: `[{"track_id": int, "trackable_object": str, "group_name": str, "x": float, "y": float, "z": float}, ...]`
  - Inside `frame_metadata`: `{"possession": {"trackable_object": str, "group": str}}`
""" # Re-pasting the schema here for brevity in the final script
        full_cte_query = "WITH\n"
        for i, step in enumerate(steps):
            print(f"AGENT: Generating SQL for Step {step['step_number']}: {step['description']}")
            step_prompt = (f"You are writing one part of a larger SQL query. The user's goal is: '{user_query}'.\n\n{DB_SCHEMA_CONTEXT}\n\nThe previous CTEs are:\n{full_cte_query if i > 0 else '-- None --'}\n\nNow, write ONLY the SQL for the CTE `{step['cte_name']}` which is supposed to: {step['description']}.\nFormat: `{step['cte_name']} AS (\n  -- your SQL here\n)`")
            sql_response = main_model.generate_content(step_prompt)
            cte_sql_match = re.search(r"(\w+\s+AS\s+\([\s\S]*\))", sql_response.text, re.IGNORECASE)
            if not cte_sql_match:
                return {"error": f"Failed at step {step['step_number']}: Could not generate parsable SQL."}
            cte_sql = cte_sql_match.group(1)
            if i > 0: full_cte_query += ",\n"
            full_cte_query += cte_sql
        
        try:
            select_cols = ", ".join(final_select_details['columns'])
            order_by_info = final_select_details.get('order_by', {})
            order_by_clause = f"ORDER BY {order_by_info['column']} {order_by_info['direction']}" if 'column' in order_by_info else ""
            limit_clause = f"LIMIT {final_select_details['limit']}" if 'limit' in final_select_details else ""
            last_cte_name = steps[-1]['cte_name']
            final_select_sql = f"SELECT {select_cols} FROM {last_cte_name} {order_by_clause} {limit_clause};"
            final_query = f"{full_cte_query}\n{final_select_sql}"
            print(f"AGENT: Assembled Query (Attempt #{healing_attempts + 1}):\n{final_query}")
        except KeyError as e:
            return {"conversational_response": "I'm sorry, the analytical plan I created was incomplete.", "data": None, "updated_history": chat_history}

        # --- SUB-STAGE C: VALIDATE THE CONSTRUCTED QUERY ---
        print("AGENT: Submitting query to the validation sub-agent...")
        validation_prompt = f"{VALIDATOR_PROMPT}\n\nSQL Query to Validate:\n```sql\n{final_query}\n```"
        validation_response = validator_model.generate_content(validation_prompt)
        
        try:
            validation_text_match = re.search(r"```(?:json)?\n(.*?)```", validation_response.text, re.DOTALL)
            validation_result = json.loads(validation_text_match.group(1))
            
            if validation_result.get("is_valid"):
                print("AGENT: Validation sub-agent approved the query.")
                is_valid = True
            else:
                errors = validation_result.get("errors", ["Unknown validation error."])
                print(f"AGENT ERROR: Validation sub-agent found errors: {errors}")
                healing_attempts += 1
                if healing_attempts <= MAX_HEALING_ATTEMPTS:
                    print(f"AGENT: Initiating self-healing (Attempt #{healing_attempts})...")
                else:
                    print("AGENT ERROR: Max healing attempts reached. Aborting.")
                    return {"conversational_response": f"I was unable to generate a valid query after multiple attempts. The final errors were: {', '.join(errors)}", "data": None, "updated_history": chat_history}
        except (AttributeError, json.JSONDecodeError, KeyError) as e:
            print(f"AGENT ERROR: The validation or healing sub-agent returned an invalid response. Error: {e}")
            return {"conversational_response": "I'm sorry, my internal validation or healing step failed.", "data": None, "updated_history": chat_history}

    if not is_valid:
        return {"conversational_response": "I'm sorry, I was unable to construct a valid query to answer your question.", "data": None, "updated_history": chat_history}
        
    # --- STAGE 3: EXECUTION ---
    print("AGENT: Executing the final, validated query...")
    tool_result = execute_dynamic_sql_query(final_query)

    if isinstance(tool_result, dict) and "error" in tool_result:
        print(f"AGENT: Final query failed during execution. Error: {tool_result['error']}")
        return {"conversational_response": f"I'm sorry, my analysis plan failed during execution with the error: {tool_result['error']}", "data": None, "updated_history": chat_history}

    # --- STAGE 4: POST-PROCESSING (CLUSTERING) ---
    final_result_for_user = tool_result
    if isinstance(tool_result, list) and tool_result and all(k in tool_result[0] for k in ['frame', 'timestamp_iso']):
        print("AGENT: Query returned frame-based results. Assessing if clustering is needed.")
        decision_model = genai.GenerativeModel(model_name="gemini-2.5-pro")
        prompt = (f"The user's original query was: '{user_query}'. A query returned {len(tool_result)} individual frames. Should these be clustered into continuous events? Answer with a single word: CLUSTER or INDIVIDUAL.")
        decision_response = decision_model.generate_content(prompt)
        decision = (decision_response.text or "").strip().upper()
        print(f"AGENT: Clustering decision: '{decision}'")
        if "CLUSTER" in decision:
            print("AGENT: Clustering results...")
            clustered_result = cluster_frames_into_events(tool_result, max_frame_gap=10)
            final_result_for_user = clustered_result

    # --- STAGE 5: SUMMARIZATION ---
    chat_model = genai.GenerativeModel(model_name="gemini-2.5-pro")
    chat_session = chat_model.start_chat(history=chat_history)

    sanitized_result = sanitize_for_json(final_result_for_user)
    summarization_prompt = (
        f"The user's query was: '{user_query}'.\n"
        f"I executed an analysis and the final data result is: {json.dumps(sanitized_result)}\n"
        f"Please provide a concise, natural language summary of this result for the user. If the data is empty, explain that no events matching the criteria were found."
    )
    summary_response = chat_session.send_message(summarization_prompt)

    return {
        "conversational_response": summary_response.text,
        "data": final_result_for_user,
        "updated_history": serialize_history(chat_session.history)
    }