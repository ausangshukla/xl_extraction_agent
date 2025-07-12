import os
from dotenv import load_dotenv
from langgraph.graph import StateGraph, END
from typing import TypedDict, Annotated, List
import operator
from agent import agent_node, tool_node, decide_next_step

# Load environment variables
load_dotenv()

# Define the agent's state
class AgentState(TypedDict):
    """
    Represents the state of our graph.

    Attributes:
        messages : A list of messages exchanged between the agent and the user.
    """
    messages: Annotated[List[str], operator.add]

# Build the graph
workflow = StateGraph(AgentState)

# Add nodes
workflow.add_node("agent", agent_node)
workflow.add_node("tool", tool_node)

# Set entry point
workflow.set_entry_point("agent")

# Add edges
workflow.add_conditional_edges(
    "agent",
    decide_next_step,
    {"tool": "tool", "end": END}
)
workflow.add_edge("tool", "agent") # After tool execution, return to agent for next step

# Compile the graph
app = workflow.compile()

if __name__ == "__main__":
    # Example usage
    # Ensure GOOGLE_API_KEY is set in your environment or .env file
    if not os.getenv("GOOGLE_API_KEY"):
        print("Please set the GOOGLE_API_KEY environment variable.")
    else:
        inputs = {"messages": [("user", "What is the capital of France?")]}
        for s in app.stream(inputs):
            print(s)