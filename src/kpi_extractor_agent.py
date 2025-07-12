import os
import pandas as pd
from typing import List, Dict, Any, TypedDict
from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage
from src.utlis.excel_to_csv_utils import convert_excel_to_csv
from src.config.llm_config import get_gemini_llm
import json
import openpyxl

# Define the state for the LangGraph agent
from typing import TypedDict, List, Dict, Any

class AgentState(TypedDict):
    folder_path: str
    kpis: List[str]
    extracted_data: List[Dict[str, Any]]
    file_paths: List[str]
    current_file_index: int

# Define the nodes (functions) for the LangGraph agent
def get_excel_files(state: AgentState) -> AgentState:
    excel_files = []
    for root, _, files in os.walk(state["folder_path"]):
        for file in files:
            if file.endswith((".xlsx", ".xls")):
                excel_files.append(os.path.join(root, file))
    state["file_paths"] = excel_files
    state["current_file_index"] = 0
    return state

def convert_and_process_file(state: AgentState) -> AgentState:
    if state["current_file_index"] < len(state["file_paths"]):
        excel_file_path = state["file_paths"][state["current_file_index"]]
        print(f"DEBUG: Processing Excel file: {excel_file_path}")
        csv_file_path = excel_file_path.replace(".xlsx", ".csv").replace(".xls", ".csv")
        
        # Convert Excel to CSV
        output_dir = os.path.join(os.path.dirname(excel_file_path), "converted_csvs")
        os.makedirs(output_dir, exist_ok=True)
        convert_excel_to_csv(excel_file_path, output_dir)
        
        # Read CSV content
        base_name = os.path.splitext(os.path.basename(excel_file_path))[0]
        
        try:
            workbook = openpyxl.load_workbook(excel_file_path)
            sheet_names = workbook.sheetnames
            workbook.close()
            print(f"DEBUG: Found sheets in Excel file: {sheet_names}")
        except Exception as e:
            print(f"ERROR: Error getting sheet names from {excel_file_path}: {e}")
            sheet_names = ["Sheet1"] # Fallback
            
        base_name = os.path.splitext(os.path.basename(excel_file_path))[0]
        
        kpis_to_find_in_current_excel = set(state["kpis"])
        print(f"DEBUG: KPIs to find for current Excel file: {kpis_to_find_in_current_excel}")

        for sheet_name in sheet_names:
            csv_file_name = f"{base_name}_{sheet_name}.csv"
            csv_file_path_full = os.path.join(output_dir, csv_file_name)
            print(f"DEBUG: Attempting to process CSV: {csv_file_path_full}")
            
            if os.path.exists(csv_file_path_full):
                df = pd.read_csv(csv_file_path_full)
                file_content = df.to_string()
                print(f"DEBUG: Read CSV content (length: {len(file_content)} chars) from {csv_file_path_full}")
                
                if not kpis_to_find_in_current_excel:
                    print("DEBUG: All KPIs found for this Excel file, breaking from sheet loop.")
                    break

                print(f"DEBUG: Calling LLM for KPIs: {list(kpis_to_find_in_current_excel)}")
                extracted_kpis = extract_kpis_with_llm(file_content, list(kpis_to_find_in_current_excel))
                print(f"DEBUG: LLM returned raw extracted_kpis: {extracted_kpis}")
                
                for extracted_kpi_data in extracted_kpis:
                    kpi_name = extracted_kpi_data.get("kpi")
                    kpi_value = extracted_kpi_data.get("value")
                    print(f"DEBUG: Processing extracted KPI: {kpi_name}, Value: {kpi_value}")
                    if kpi_name and kpi_name in kpis_to_find_in_current_excel and kpi_value is not None:
                        state["extracted_data"].append(extracted_kpi_data)
                        print(f"DEBUG: Added KPI: {kpi_name}. Current extracted data count: {len(state['extracted_data'])}")
                    else:
                        print(f"DEBUG: Skipping KPI: {kpi_name} (not in target list or value is None)")
                
                # The loop should continue to process all extracted_kpis from the LLM for the current sheet
                # The check for kpis_to_find_in_current_excel being empty is removed as we want all instances
                # of the target KPIs, not just one per KPI name.
            else:
                print(f"WARNING: CSV file not found for sheet '{sheet_name}': {csv_file_path_full}")
        
        state["current_file_index"] += 1
        print(f"DEBUG: Incremented current_file_index to {state['current_file_index']}")
    return state

def extract_kpis_with_llm(file_content: str, kpis: List[str]) -> List[Dict[str, Any]]:
    llm = get_gemini_llm()
    
    prompt = f"""
    Given the following text content from an Excel/CSV file, extract the specified KPIs.
    For each KPI, identify its value and the corresponding date or period from the header row.
    
    KPIs to extract: {", ".join(kpis)}
    
    File Content:
    {file_content}
    
    Provide the output *only* as a JSON array of objects, where each object has 'kpi', 'period', and 'value' fields.
    Do NOT include any additional text, explanations, or code blocks outside of the JSON.
    If a KPI is not found or a period cannot be determined, use null for that field.
    Example:
    [
        {{
            "kpi": "Revenue",
            "period": "2023-Q1",
            "value": "1000000"
        }},
        {{
            "kpi": "Profit",
            "period": "2023-Q1",
            "value": "200000"
        }}
    ]
    """
    print(f"DEBUG: LLM Prompt (first 500 chars): {prompt[:500]}...")
    
    response = llm.invoke([HumanMessage(content=prompt)])
    print(f"DEBUG: Raw LLM Response Content: {response.content}")
    try:
        json_string = response.content.strip()
        if json_string.startswith("```json"):
            json_string = json_string[len("```json"):].strip()
        if json_string.endswith("```"):
            json_string = json_string[:-len("```")].strip()
        
        parsed_json = json.loads(json_string)
        print(f"DEBUG: Parsed JSON from LLM: {parsed_json}")
        return parsed_json
    except json.JSONDecodeError as e:
        print(f"ERROR: Error decoding JSON from LLM response: {e}")
        print(f"ERROR: Problematic content: {response.content}")
        return []

def should_continue(state: AgentState) -> str:
    if state["current_file_index"] < len(state["file_paths"]):
        return "convert_and_process_file"
    else:
        return "end"

# Build the LangGraph
def create_kpi_extractor_agent():
    workflow = StateGraph(AgentState)
    
    workflow.add_node("get_excel_files", get_excel_files)
    workflow.add_node("convert_and_process_file", convert_and_process_file)
    
    workflow.set_entry_point("get_excel_files")
    
    workflow.add_edge("get_excel_files", "convert_and_process_file")
    workflow.add_conditional_edges(
        "convert_and_process_file",
        should_continue,
        {
            "convert_and_process_file": "convert_and_process_file",
            "end": END
        }
    )
    
    app = workflow.compile()
    return app

if __name__ == "__main__":
    # Example Usage
    # Make sure to replace with your actual folder path and KPIs
    folder_to_scan = "test_excel_files"
    # kpis_to_extract = ["Revenue from operations", "Gross Profit", "Profit After Tax"]
    kpis_to_extract = ["Revenue from operations"]
    agent = create_kpi_extractor_agent()
    
    initial_state = AgentState(
        folder_path=folder_to_scan,
        kpis=kpis_to_extract,
        extracted_data=[],
        file_paths=[],
        current_file_index=0
    )
    
    final_state = agent.invoke(initial_state)
    
    print("Extracted KPIs:")
    for data in final_state["extracted_data"]:
        print(data)
