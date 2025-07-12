# KPI Extractor Agent Flow

This document explains the operational flow of the `kpi_extractor_agent.py` script, which is designed to extract Key Performance Indicators (KPIs) from Excel files. The script utilizes LangGraph to orchestrate a multi-step process, handling file conversion, content extraction, and LLM-based KPI identification.

## Overall Flow

The agent processes Excel files one by one. For each Excel file, it converts all sheets into separate CSV files and then iterates through these CSVs to extract the specified KPIs using a Large Language Model (LLM).

## Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant Main Script
    participant KPI Extractor Agent
    participant get_excel_files Node
    participant convert_and_process_file Node
    participant convert_excel_to_csv
    participant openpyxl
    participant pandas
    participant extract_kpis_with_llm Function
    participant get_gemini_llm
    participant LLM Gemini
    participant should_continue Node

    User->>Main Script: Run kpi_extractor_agent.py
    Main Script->>KPI Extractor Agent: create_kpi_extractor_agent
    KPI Extractor Agent->>get_excel_files Node: set_entry_point "get_excel_files"
    Main Script->>KPI Extractor Agent: invoke initial_state

    activate KPI Extractor Agent
    activate get_excel_files Node
    get_excel_files Node->>get_excel_files Node: Scan folder_path for .xlsx/.xls files
    get_excel_files Node-->>KPI Extractor Agent: Update state with file_paths, current_file_index = 0
    deactivate get_excel_files Node

    loop For each Excel file in file_paths
        KPI Extractor Agent->>convert_and_process_file Node: Call convert_and_process_file state
        activate convert_and_process_file Node
        convert_and_process_file Node->>convert_excel_to_csv: convert_excel_to_csv excel_file_path, output_dir
        activate convert_excel_to_csv
        convert_excel_to_csv-->>convert_and_process_file Node: Converted sheets to CSVs in output_dir
        deactivate convert_excel_to_csv

        convert_and_process_file Node->>openpyxl: load_workbook excel_file_path
        activate openpyxl
        openpyxl-->>convert_and_process_file Node: Get sheet_names
        deactivate openpyxl

        loop For each sheet_name in sheet_names
            convert_and_process_file Node->>convert_and_process_file Node: Construct csv_file_path_full
            convert_and_process_file Node->>pandas: read_csv csv_file_path_full
            activate pandas
            pandas-->>convert_and_process_file Node: DataFrame df
            deactivate pandas
            convert_and_process_file Node->>convert_and_process_file Node: Convert df to file_content string

            alt KPIs still to find for current Excel file
                convert_and_process_file Node->>extract_kpis_with_llm Function: extract_kpis_with_llm file_content, kpis_to_find
                activate extract_kpis_with_llm Function
                extract_kpis_with_llm Function->>get_gemini_llm: get_gemini_llm
                activate get_gemini_llm
                get_gemini_llm-->>extract_kpis_with_llm Function: LLM instance
                deactivate get_gemini_llm
                extract_kpis_with_llm Function->>LLM Gemini: invoke HumanMessage prompt
                activate LLM Gemini
                LLM Gemini-->>extract_kpis_with_llm Function: JSON response with extracted KPIs
                deactivate LLM Gemini
                extract_kpis_with_llm Function-->>convert_and_process_file Node: List of extracted_kpis
                deactivate extract_kpis_with_llm Function

                convert_and_process_file Node->>convert_and_process_file Node: Append extracted_kpi_data to state["extracted_data"]
                convert_and_process_file Node->>convert_and_process_file Node: Remove found KPIs from kpis_to_find_in_current_excel
            else All KPIs found for current Excel file
                convert_and_process_file Node->>convert_and_process_file Node: Break from sheet loop
            end
        end

        convert_and_process_file Node->>convert_and_process_file Node: Increment state["current_file_index"]
        convert_and_process_file Node-->>KPI Extractor Agent: Updated state
        deactivate convert_and_process_file Node

        KPI Extractor Agent->>should_continue Node: Call should_continue state
        activate should_continue Node
        alt current_file_index < len file_paths
            should_continue Node-->>KPI Extractor Agent: "convert_and_process_file" (continue loop)
        else All files processed
            should_continue Node-->>KPI Extractor Agent: "end" (exit loop)
        end
        deactivate should_continue Node
    end

    KPI Extractor Agent-->>Main Script: Final state
    deactivate KPI Extractor Agent

    Main Script->>Main Script: Print extracted KPIs from final_state["extracted_data"]
```

## Explanation of Components and Flow:

1.  **User Initiation**: The process begins when the user runs the `kpi_extractor_agent.py` script.
2.  **Agent Initialization**: The `create_kpi_extractor_agent()` function sets up the LangGraph workflow, defining the nodes (`get_excel_files`, `convert_and_process_file`) and the entry point.
3.  **`get_excel_files` Node**:
    *   This is the starting point of the workflow.
    *   It scans the `folder_path` (provided in the initial state) for all Excel files (`.xlsx`, `.xls`).
    *   It updates the agent's state with a list of `file_paths` and initializes `current_file_index` to 0.
4.  **`convert_and_process_file` Node**:
    *   This node is responsible for handling a single Excel file at a time.
    *   It checks if there are more Excel files to process based on `current_file_index`.
    *   **Excel to CSV Conversion**: It calls `convert_excel_to_csv` (from `src/utlis/excel_to_csv_utils.py`) to convert all sheets of the current Excel file into separate CSV files within a `converted_csvs` subdirectory.
    *   **Sheet Name Retrieval**: It uses `openpyxl` to load the Excel workbook and get all sheet names. This is crucial because `convert_excel_to_csv` creates one CSV per sheet, named `original_filename_sheetname.csv`.
    *   **Iterating through CSVs (Sheets)**: It then loops through each `sheet_name` obtained from the Excel file.
        *   For each sheet, it constructs the full path to the corresponding CSV file.
        *   It reads the CSV content into a Pandas DataFrame and converts it to a string (`file_content`) for the LLM.
        *   **KPI Extraction**: It calls the `extract_kpis_with_llm` function, passing the CSV content and the list of KPIs to find.
            *   `extract_kpis_with_llm` initializes the LLM (Gemini) using `get_gemini_llm()` (from `src/config/llm_config.py`).
            *   It constructs a detailed prompt for the LLM, including the file content and the KPIs to extract.
            *   It invokes the LLM and parses the JSON response to get the extracted KPI data.
        *   The extracted KPIs are appended to the `extracted_data` list in the agent's state. KPIs that have been found are removed from `kpis_to_find_in_current_excel` to optimize subsequent LLM calls for the same Excel file.
    *   After processing all sheets (or finding all required KPIs) for the current Excel file, `current_file_index` is incremented.
5.  **`should_continue` Node (Conditional Edge)**:
    *   This node acts as a decision point in the LangGraph workflow.
    *   It checks if `current_file_index` is less than the total number of `file_paths`.
    *   If true, it returns `"convert_and_process_file"`, causing the workflow to loop back and process the next Excel file.
    *   If false (all Excel files have been processed), it returns `"end"`, terminating the workflow.
6.  **Final Output**: Once the workflow ends, the main script prints all the collected `extracted_data`.