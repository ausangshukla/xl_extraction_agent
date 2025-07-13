import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json # Added for JSON output
from typing import List, Dict, Any
from langgraph.graph import StateGraph, END
from src.utlis.validation_utils import validate_kpi_data
from src.utlis.file_processing_utils import convert_and_process_file # Import the moved function
from src.common.types import AgentState # Import AgentState from common types

# Define the state for the LangGraph agent

# Define the nodes (functions) for the LangGraph agent
def get_excel_files(state: AgentState) -> AgentState:
    excel_files = []
    absolute_folder_path = os.path.abspath(state["folder_path"])
    print(f"DEBUG: Scanning absolute folder path: {absolute_folder_path}")
    for root, _, files in os.walk(absolute_folder_path):
        for file in files:
            if file.endswith((".xlsx", ".xls")):
                excel_files.append(os.path.join(root, file))
    state["file_paths"] = excel_files
    state["current_file_index"] = 0
    print(f"DEBUG: Found {len(state['file_paths'])} Excel files.")
    return state


def validate_extracted_kpis(state: AgentState) -> AgentState:
    print(f"DEBUG: validate_extracted_kpis called. current_file_index: {state['current_file_index']}, file_paths length: {len(state['file_paths'])}")
    
    if not state["extracted_data"]:
        print("WARNING: No extracted KPIs to validate. Skipping validation.")
        return state

    # Get the current Excel file path that was just processed
    # current_file_index is already incremented in convert_and_process_file for the *next* file
    # So, we need to use current_file_index - 1 to get the *current* Excel file.
    current_excel_file_path = state["file_paths"][state["current_file_index"] - 1]
    current_excel_file_basename = os.path.basename(current_excel_file_path)

    # Filter extracted_data to only include KPIs from the current Excel file
    # These KPIs should have their 'source_file' set to the original Excel filename
    extracted_kpis_for_current_excel = [
        kpi_data for kpi_data in state["extracted_data"]
        if kpi_data.get("source_file") == current_excel_file_basename
    ]

    if not extracted_kpis_for_current_excel:
        print(f"WARNING: No KPIs found for current Excel file '{current_excel_file_basename}' in extracted_data. Skipping validation for this file.")
        return state

    # Group these KPIs by their CSV file path for validation
    kpis_by_csv_file_for_current_excel = {}
    for kpi_data in extracted_kpis_for_current_excel:
        csv_file_path = kpi_data.get("csv_file_path")
        if csv_file_path:
            if csv_file_path not in kpis_by_csv_file_for_current_excel:
                kpis_by_csv_file_for_current_excel[csv_file_path] = []
            kpis_by_csv_file_for_current_excel[csv_file_path].append(kpi_data)
        else:
            print(f"WARNING: KPI data for '{kpi_data.get('kpi')}' missing 'csv_file_path'. Cannot validate.")

    # Initialize validation results for the current Excel file
    if current_excel_file_basename not in state["validation_results"]:
        state["validation_results"][current_excel_file_basename] = {
            "validated_kpis": [],
            "unextracted_numbers": []
        }

    for csv_file_path, kpis_to_validate in kpis_by_csv_file_for_current_excel.items():
        csv_file_basename = os.path.basename(csv_file_path)
        print(f"DEBUG: Validating KPIs for CSV file: {csv_file_basename} (derived from {current_excel_file_basename})")
        
        # Pass the CSV file path to validate_kpi_data
        validation_output = validate_kpi_data(kpis_to_validate, csv_file_path, state["kpis"])
        
        # Aggregate results into the validation_results keyed by the original Excel file
        state["validation_results"][current_excel_file_basename]["validated_kpis"].extend(validation_output["validated_kpis"])
        state["validation_results"][current_excel_file_basename]["unextracted_numbers"].extend(validation_output["unextracted_numbers"])
        
        print(f"DEBUG: KPI validation completed for CSV file: {csv_file_basename}.")
        
    print(f"DEBUG: All KPI validation for Excel file '{current_excel_file_basename}' completed.")
    return state


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
    workflow.add_node("validate_extracted_kpis", validate_extracted_kpis)
    
    workflow.set_entry_point("get_excel_files")
    
    workflow.add_edge("get_excel_files", "convert_and_process_file")
    workflow.add_edge("convert_and_process_file", "validate_extracted_kpis")
    workflow.add_conditional_edges(
        "validate_extracted_kpis",
        should_continue,
        {
            "convert_and_process_file": "convert_and_process_file", # Loop back to process next file
            "end": END
        }
    )
    
    app = workflow.compile()
    return app

if __name__ == "__main__":
    # Example Usage
    # Make sure to replace with your actual folder path and KPIs
    folder_to_scan = "test_excel_files"
    kpis_to_extract = ["Revenue from operations", "Gross Profit", "Profit After Tax"]
    # kpis_to_extract = ["Revenue from operations"]
    agent = create_kpi_extractor_agent()
    
    initial_state = AgentState(
        folder_path=folder_to_scan,
        kpis=kpis_to_extract,
        extracted_data=[],
        file_paths=[],
        current_file_index=0,
        validation_results={}
    )
    
    final_state = agent.invoke(initial_state)
    
    print("\n--- Final Extracted KPIs ---")
    for data in final_state["extracted_data"]:
        print(f"KPI: {data.get('kpi')}, Value: {data.get('value')}, Period: {data.get('period')}, Source File: {data.get('source_file')}, Row: {data.get('row_number')}, Col: {data.get('column_number')}")
        
    print("\n--- Validation Results ---")
    if final_state["validation_results"]:
        for file_name, results in final_state["validation_results"].items():
            print(f"\n--- Validation Results for {file_name} ---")
            print("Validated KPIs:")
            for kpi_result in results["validated_kpis"]:
                print(f"  KPI: {kpi_result.get('kpi')}, Value: {kpi_result.get('value')}, Period: {kpi_result.get('period')}, Status: {kpi_result.get('validation_status')}, Notes: {kpi_result.get('notes')}, Row: {kpi_result.get('row_number')}, Col: {kpi_result.get('column_number')}")
            print("\nUnextracted Numbers:")
            for unextracted_num in results["unextracted_numbers"]:
                print(f"  - {unextracted_num}")
    else:
        print("No validation results available.")

    print("\n--- Raw KPI JSON Output ---")
    # To ensure all data is serializable, especially if there are complex objects
    # We'll create a simplified dictionary for JSON output
    json_output = {
        "extracted_kpis_before_validation": final_state["extracted_data"],
        "validation_summary": final_state["validation_results"],
        "validated_kpis_detailed": [] # Initialize an empty list for detailed validated KPIs
    }

    # Populate validated_kpis_detailed from validation_results
    for file_name, results in final_state["validation_results"].items():
        for kpi_result in results["validated_kpis"]:
            json_output["validated_kpis_detailed"].append(kpi_result)

    print(json.dumps(json_output, indent=2))
