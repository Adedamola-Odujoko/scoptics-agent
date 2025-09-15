import os
import re
import json
from dotenv import load_dotenv
import google.generativeai as genai
from typing import List, Dict
from datetime import datetime

from scoptics_agent.events.clustering import cluster_frames_into_events
from .agent_tools import (
    find_events_with_filters, StructuredFilterArgs,
    find_similar_events, SemanticSearchArgs,
    run_dynamic_query, DynamicSqlArgs
)
from .retrieval import execute_dynamic_sql_query # We need this for the retry

# (Keep all configuration and helper functions: project_root, GOOGLE_API_KEY, AVAILABLE_TOOLS, cleanup_schema, sanitize_for_json)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
genai.configure(api_key=GOOGLE_API_KEY)
def sanitize_for_json(data):
    if isinstance(data, list): return [sanitize_for_json(item) for item in data]
    if isinstance(data, dict): return {key: sanitize_for_json(value) for key, value in data.items()}
    if isinstance(data, datetime): return data.isoformat()
    return data
AVAILABLE_TOOLS = {
    "find_events_with_filters": {"function": find_events_with_filters, "schema": StructuredFilterArgs},
    "find_similar_events": {"function": find_similar_events, "schema": SemanticSearchArgs},
    "run_dynamic_query": {"function": run_dynamic_query, "schema": DynamicSqlArgs},
}

def run_conversational_agent(user_query: str, chat_history: List[Dict]):
    print(f"\nAGENT: Received query: '{user_query}'")

    # (Keep the cleanup_schema helper function as is)
    def cleanup_schema(s_dict):
        keys_to_remove = ['title', 'additionalProperties', 'default']
        for key in keys_to_remove:
            if key in s_dict: del s_dict[key]
        if 'properties' in s_dict:
            for prop_value in s_dict['properties'].values():
                if isinstance(prop_value, dict): cleanup_schema(prop_value)
        return s_dict

    gemini_tools = [{"function_declarations": []}]
    for name, tool in AVAILABLE_TOOLS.items():
        schema_dict = tool["schema"].model_json_schema()
        cleaned_schema = cleanup_schema(schema_dict)
        gemini_tools[0]["function_declarations"].append({
            "name": name, "description": tool["function"].__doc__, "parameters": cleaned_schema
        })

    model = genai.GenerativeModel(model_name="gemini-1.5-pro-latest", tools=gemini_tools)
    chat_session = model.start_chat(history=chat_history)
    
    print("AGENT: Sending query to Gemini to select a tool...")
    response = chat_session.send_message(user_query)
    
    function_call = None
    try:
        if response.candidates[0].content.parts[0].function_call:
            function_call = response.candidates[0].content.parts[0].function_call
    except (IndexError, AttributeError):
        pass

    if not function_call:
        print("AGENT: Model returned a conversational response without a tool call.")
        updated_history = [{'role': message.role, 'parts': [part.text for part in message.parts]} for message in chat_session.history]
        return {"conversational_response": response.text, "data": None, "updated_history": updated_history}

    tool_name = function_call.name
    tool_args = dict(function_call.args)
    print(f"AGENT: Gemini chose tool '{tool_name}' with arguments: {tool_args}")

    if tool_name not in AVAILABLE_TOOLS:
        return {"error": f"Model chose an unavailable tool: {tool_name}"}

    selected_tool = AVAILABLE_TOOLS[tool_name]
    tool_function = selected_tool["function"]
    arg_schema = selected_tool["schema"]

    try:
        validated_args = arg_schema(**tool_args)
    except Exception as e:
        return {"error": f"Invalid arguments for tool {tool_name}: {e}"}

    print(f"AGENT: Executing tool '{tool_name}'...")
    tool_result = tool_function(validated_args)

    if tool_name == "run_dynamic_query" and isinstance(tool_result, dict) and "error" in tool_result:
        # (Self-healing logic remains the same)
        # ... (code for self-healing)
        pass # Placeholder for brevity, your existing code is fine here

    # --- THE FIX IS HERE: RE-INTRODUCING THE CLUSTERING LOGIC ---
    # --------------------------------------------------------------------------
    final_result_for_llm = tool_result # Start with the original result

    if tool_name == "run_dynamic_query" and isinstance(tool_result, list) and tool_result:
        print("AGENT: Dynamic query returned results. Assessing if clustering is needed.")
        decision_model = genai.GenerativeModel(model_name="gemini-1.5-flash-latest")
        prompt = (
            f"The user's original query was: '{user_query}'.\n"
            f"A query returned {len(tool_result)} individual frames.\n"
            f"Based on the user's query, should these frames be treated as a set of distinct, "
            f"individual moments, or as one or more continuous events that should be "
            f"grouped together (clustered)?\n"
            f"Answer with a single word: CLUSTER or INDIVIDUAL."
        )
        decision_response = decision_model.generate_content(prompt)
        decision = decision_response.text.strip().upper()
        
        print(f"AGENT: Clustering decision from LLM: '{decision}'")
        
        if "CLUSTER" in decision:
            print("AGENT: Clustering results...")
            clustered_result = cluster_frames_into_events(tool_result, max_frame_gap=10)
            final_result_for_llm = clustered_result # IMPORTANT: We update the result

    # --------------------------------------------------------------------------
    
    sanitized_result = sanitize_for_json(final_result_for_llm)
    print("AGENT: Sending tool result back to Gemini for summarization...")
    response = chat_session.send_message(
        content={"function_response": {"name": tool_name, "response": {"result": sanitized_result}}}
    )
    
    return {
        "conversational_response": response.text,
        "data": final_result_for_llm, # Return the final, possibly-clustered data
        "updated_history": [{'role': message.role, 'parts': [part.text for part in message.parts]} for message in chat_session.history]
    }