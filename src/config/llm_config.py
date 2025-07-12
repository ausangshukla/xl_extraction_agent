import os
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_google_vertexai import ChatVertexAI

def get_gemini_llm():
    """
    Initializes and returns a ChatGoogleGenerativeAI instance configured for Gemini 2.5 Flash.
    Ensures the GOOGLE_API_KEY environment variable is set.
    """
    google_api_key = os.getenv("GOOGLE_API_KEY")
    if not google_api_key:
        raise ValueError("GOOGLE_API_KEY environment variable not set.")
    
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash-preview-05-20", temperature=0.7, google_api_key=google_api_key)
    return llm

if __name__ == "__main__":
    # Example usage (for testing purposes)
    # Make sure to set GOOGLE_API_KEY in your environment before running this.
    try:
        llm = get_gemini_llm()
        print("Gemini 2.5 Flash LLM initialized successfully.")
        # You can add a simple invocation here to test
        # response = llm.invoke("Hello, how are you?")
        # print(response.content)
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")