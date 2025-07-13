import os
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import HumanMessage, AIMessage, FunctionMessage
from langchain_core.tools import tool
from langchain_core.utils.function_calling import convert_to_openai_function
from langgraph.prebuilt import ToolExecutor, ToolInvocation
import json

# Load environment variables
# Assuming GOOGLE_API_KEY is set in your environment or .env file
# from dotenv import load_dotenv
# load_dotenv()

# Initialize the Gemini model
llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash-preview-05-20", temperature=0)

# Define the tools available to the agent
# In a real scenario, you would import your tools from tools.py
# For this scaffolding, we'll define a dummy tool or import from tools.py if it exists
try:
    from tools import example_tool
    tools = [example_tool]
except ImportError:
    print("Warning: tools.py not found or example_tool not defined. Using a dummy tool.")
    @tool
    def dummy_tool(query: str) -> str:
        """A dummy tool for demonstration."""
        return f"Dummy tool received: {query}"
    tools = [dummy_tool]

# Convert tools to OpenAI function format for Gemini
functions = [convert_to_openai_function(t) for t in tools]
llm_with_tools = llm.bind_functions(functions)

# Create a ToolExecutor
tool_executor = ToolExecutor(tools)

# Agent node function
def agent_node(state):
    messages = state["messages"]
    # The last message is the user's input
    last_message = messages[-1]

    # If the last message is a tool output, it means the agent needs to decide what to do next
    if isinstance(last_message, FunctionMessage):
        # If the tool was called, the agent should respond to the user or call another tool
        # For now, we'll just respond with a generic message after a tool call
        return {"messages": [AIMessage(content="Tool executed. What's next?")]}
    else:
        # If it's a human message, the agent should generate a response or tool call
        response = llm_with_tools.invoke(messages)
        return {"messages": [response]}

# Tool node function
def tool_node(state):
    messages = state["messages"]
    # Get the last message, which should be an AIMessage with a tool_calls attribute
    last_message = messages[-1]

    # Extract tool calls
    tool_calls = last_message.tool_calls
    if not tool_calls:
        raise ValueError("No tool calls found in the last message.")

    # Execute each tool call
    tool_outputs = []
    for tool_call in tool_calls:
        tool_invocation = ToolInvocation(
            tool=tool_call["name"],
            tool_input=tool_call["args"]
        )
        output = tool_executor.invoke(tool_invocation)
        tool_outputs.append(FunctionMessage(content=str(output), name=tool_call["name"]))

    return {"messages": tool_outputs}

# Decide next step function
def decide_next_step(state):
    messages = state["messages"]
    last_message = messages[-1]

    # If the last message is an AIMessage and it has tool calls, then execute tools
    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        return "tool"
    # If the last message is a FunctionMessage (tool output), then the agent should respond
    elif isinstance(last_message, FunctionMessage):
        return END # For now, we end after a tool execution
    # If it's an AIMessage without tool calls, it's a final response
    elif isinstance(last_message, AIMessage) and not last_message.tool_calls:
        return END
    else:
        # Default to agent if no specific condition met (e.g., initial human message)
        return "agent"