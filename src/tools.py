from langchain_core.tools import tool

@tool
def example_tool(query: str) -> str:
    """
    This is an example tool that echoes the query.
    In a real application, this would perform a specific action.
    """
    return f"Echoing your query: {query}"

# You can add more tools here
# @tool
# def another_tool(param1: str, param2: int) -> str:
#     """
#     Description of another tool.
#     """
#     return f"Another tool called with {param1} and {param2}"