import sys
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List

# Add the 'src' directory to the path to allow imports from our package
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import the query engines
from .retrieval import query_events_by_filters, query_events_by_semantic
# Import the agent function
from .agent import run_conversational_agent 

# Create the FastAPI app instance
app = FastAPI(
    title="Scoptics AI Agent API",
    description="API for querying football match events and analysis.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    # This allows all origins for simplicity in local development.
    # In production, you would restrict this to your actual frontend domain.
    # e.g., ["https://www.scoptics.com"]
    allow_origins=["*"],
    allow_credentials=True,
    # This allows all HTTP methods (GET, POST, etc.).
    allow_methods=["*"],
    # This allows all HTTP headers.
    allow_headers=["*"],
)

# ====================
# ROOT ENDPOINT
# ====================
@app.get("/")
def read_root():
    """A simple root endpoint to confirm the API is running."""
    return {"message": "Welcome to the Scoptics AI Agent API"}

# ====================
# STRUCTURED QUERY
# ====================
class StructuredQueryRequest(BaseModel):
    filters: Dict[str, Any]

@app.post("/query/structured")
def query_structured_events(request: StructuredQueryRequest):
    """
    Endpoint for running a structured query against the events table.
    """
    try:
        results = query_events_by_filters(request.filters)
        if not results:
            return {"message": "Query executed successfully, but no events matched the filters.", "data": []}
        return {"data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

# ====================
# SEMANTIC QUERY
# ====================
class SemanticQueryRequest(BaseModel):
    query_text: str
    top_k: int = 5

@app.post("/query/semantic")
def query_semantic_events(request: SemanticQueryRequest):
    """
    Endpoint for running a semantic search against the events in the vector database.
    """
    try:
        results = query_events_by_semantic(request.query_text, request.top_k)
        if not results:
            return {"message": "Query executed successfully, but no similar events were found.", "data": []}
        return {"data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

# ====================
# AGENT QUERY
# ====================
class AgentQueryRequest(BaseModel):
    query: str
    # The history is a list of dictionaries, and it's optional.
    chat_history: List[Dict] = []

# --- UPDATE THE ENDPOINT ---
@app.post("/agent/query")
def agent_query(request: AgentQueryRequest):
    """
    The main endpoint for the AI agent. Now supports conversational history.
    """
    if not request.query:
        raise HTTPException(status_code=400, detail="Query cannot be empty.")
    
    try:
        # Pass both the query and the history to the agent
        result = run_conversational_agent(request.query, request.chat_history)
        return {"response": result}
    except Exception as e:
        print(f"ERROR in agent execution: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred in the agent: {e}")