import sys
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List

from api.agent import run_conversational_agent

# Create the FastAPI app instance
app = FastAPI(
    title="Scoptics AI Agent API",
    description="API for querying football match events and analysis.",
    version="2.0.0" # Version bump!
)

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- NEW: CONVERSATIONAL AGENT ENDPOINT ---
class AgentQueryRequest(BaseModel):
    query: str
    # The chat history is a list of dictionaries, optional for the first turn.
    chat_history: List[Dict] = []

@app.post("/agent/query")
def agent_query(request: AgentQueryRequest):
    """
    The main endpoint for the conversational AI agent. Receives a natural language
    query and the chat history, then returns a conversational response,
    structured data, and the updated history.
    """
    if not request.query:
        raise HTTPException(status_code=400, detail="Query cannot be empty.")
    
    try:
        result = run_conversational_agent(request.query, request.chat_history)
        return {"response": result}
    except Exception as e:
        print(f"ERROR in agent execution: {e}")
        # It's helpful to print the full traceback for debugging
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred in the agent: {e}")