import pandas as pd
import openpyxl
import re
from typing import List, Dict, Any, Union

def validate_kpi_data(extracted_kpis: List[Dict[str, Any]], excel_file_path: str, target_kpis: List[str]) -> Dict[str, Any]:
    """
    Validates extracted KPI data against the original Excel file.
    Uses LLM-provided row and column numbers for cross-validation.

    Args:
        extracted_kpis (List[Dict[str, Any]]): List of KPIs extracted by the LLM, including row_number and column_number.
        excel_file_path (str): Path to the original Excel file.
        target_kpis (List[str]): The list of KPIs the agent was asked to extract.

    Returns:
        Dict[str, Any]: A dictionary containing validated KPIs and unextracted numbers.
    """
    print(f"DEBUG: Starting KPI validation for file: {excel_file_path}")
    
    validated_kpis = []
    unextracted_numbers = []

    # --- Tier 1: Structural Validation ---
    structurally_valid_kpis = []
    for kpi_data in extracted_kpis:
        kpi_name = kpi_data.get("kpi")
        kpi_value = kpi_data.get("value")
        kpi_period = kpi_data.get("period")
        source_file = kpi_data.get("source_file")
        row_number = kpi_data.get("row_number")
        column_number = kpi_data.get("column_number")

        validation_status = "Valid"
        notes = []

        if not kpi_name or not isinstance(kpi_name, str) or not kpi_name.strip():
            validation_status = "INVALID_STRUCTURE"
            notes.append("KPI name is missing or invalid.")
        
        if kpi_value is None:
            validation_status = "INVALID_STRUCTURE"
            notes.append("Value is missing.")
        elif not isinstance(kpi_value, (int, float, str)) or (isinstance(kpi_value, str) and not str(kpi_value).replace(",", "").replace(".", "").isdigit()):
            validation_status = "INVALID_STRUCTURE"
            notes.append("Value is not a valid number format.")
        
        if not kpi_period or not isinstance(kpi_period, str) or not kpi_period.strip():
            validation_status = "INVALID_STRUCTURE"
            notes.append("Period is missing or invalid.")
        
        if row_number is None or not isinstance(row_number, int) or row_number <= 0:
            validation_status = "INVALID_STRUCTURE"
            notes.append("Row number is missing or invalid.")

        if column_number is None or not isinstance(column_number, int) or column_number <= 0:
            validation_status = "INVALID_STRUCTURE"
            notes.append("Column number is missing or invalid.")

        if validation_status == "INVALID_STRUCTURE":
            validated_kpis.append({
                "kpi": kpi_name,
                "value": kpi_value,
                "period": kpi_period,
                "source_file": source_file,
                "row_number": row_number,
                "column_number": column_number,
                "validation_status": validation_status,
                "notes": notes
            })
        else:
            structurally_valid_kpis.append(kpi_data)

    if not structurally_valid_kpis:
        print("DEBUG: No structurally valid KPIs to cross-validate.")
        return {
            "validated_kpis": validated_kpis,
            "unextracted_numbers": []
        }

    try:
        workbook = openpyxl.load_workbook(excel_file_path, data_only=True)
    except Exception as e:
        print(f"ERROR: Could not load Excel workbook {excel_file_path}: {e}")
        for kpi_data in structurally_valid_kpis:
            kpi_data["validation_status"] = "EXCEL_LOAD_ERROR"
            kpi_data["notes"].append(f"Could not load Excel file: {e}")
            validated_kpis.append(kpi_data)
        return {
            "validated_kpis": validated_kpis,
            "unextracted_numbers": []
        }

    potential_unextracted_kpi_values = set()

    for sheet_name in workbook.sheetnames:
        sheet = workbook[sheet_name]
        
        # Find header row (assuming first row for now, can be made more robust)
        # header_row_values = [cell.value for cell in sheet[1]] # Not directly used for unextracted numbers anymore
        
        for target_kpi_name in target_kpis:
            # This part is still relevant for finding potential unextracted numbers related to target KPIs
            kpi_row_index = -1
            for r_idx, row in enumerate(sheet.iter_rows()):
                for cell in row:
                    if cell.value and isinstance(cell.value, str) and target_kpi_name.lower() == str(cell.value).lower():
                        kpi_row_index = r_idx
                        break
                if kpi_row_index != -1:
                    break
            
            if kpi_row_index != -1:
                for c_idx, cell in enumerate(sheet[kpi_row_index + 1]):
                    if isinstance(cell.value, (int, float)):
                        potential_unextracted_kpi_values.add(str(cell.value).replace(",", ""))
                    elif isinstance(cell.value, str):
                        num_str_cleaned = cell.value.replace(",", "")
                        if re.match(r'^\d+(\.\d+)?$', num_str_cleaned):
                            potential_unextracted_kpi_values.add(num_str_cleaned)

    for kpi_data in structurally_valid_kpis:
        if kpi_data.get("validation_status") == "Valid":
            kpi_name = kpi_data["kpi"]
            kpi_value = kpi_data["value"]
            kpi_period = kpi_data["period"]
            row_number = kpi_data["row_number"]
            column_number = kpi_data["column_number"]
            extracted_value_str = str(kpi_value).replace(",", "")

            try:
                # Access the sheet using the name from the extracted data's source_file
                # This assumes sheet_name can be derived from source_file or is consistent
                # For now, we'll iterate through all sheets to find the cell
                source_value_found = False
                for sheet_name_in_workbook in workbook.sheetnames:
                    sheet = workbook[sheet_name_in_workbook]
                    try:
                        source_value_cell = sheet.cell(row=row_number, column=column_number).value
                        if source_value_cell is not None:
                            source_value_str_from_excel = str(source_value_cell).replace(",", "")
                            
                            # Compare values
                            if source_value_str == source_value_str_from_excel:
                                kpi_data["validation_status"] = "Valid"
                            else:
                                # Attempt numeric comparison if both are numeric
                                try:
                                    if abs(float(extracted_value_str) - float(source_value_str_from_excel)) < 1e-9:
                                        kpi_data["validation_status"] = "Valid"
                                    else:
                                        kpi_data["validation_status"] = "VALUE_MISMATCH"
                                        kpi_data["notes"].append(f"Value mismatch. Expected: '{source_value_str_from_excel}', Got: '{extracted_value_str}' at R{row_number}C{column_number}.")
                                except ValueError:
                                    # If not numeric, or conversion fails, compare as strings
                                    kpi_data["validation_status"] = "VALUE_MISMATCH"
                                    kpi_data["notes"].append(f"Value mismatch. Expected: '{source_value_str_from_excel}', Got: '{extracted_value_str}' at R{row_number}C{column_number}.")
                            source_value_found = True
                            break # Value found in this sheet, no need to check others
                        else:
                            kpi_data["validation_status"] = "VALUE_MISSING_IN_SOURCE"
                            kpi_data["notes"].append(f"Value cell is empty in source Excel at R{row_number}C{column_number}.")
                            source_value_found = True
                            break
                    except Exception as e:
                        # This catch is for sheet.cell access errors, not for sheet not found
                        print(f"WARNING: Error accessing cell R{row_number}C{column_number} in sheet {sheet_name_in_workbook}: {e}")
                        # Do not set status here, let the outer loop determine if not found in any sheet
                        pass
                
                if not source_value_found:
                    kpi_data["validation_status"] = "LOCATION_NOT_FOUND_IN_EXCEL"
                    kpi_data["notes"].append(f"Could not find value at R{row_number}C{column_number} in any sheet of the source Excel file.")

            except Exception as e:
                kpi_data["validation_status"] = "CROSS_VALIDATION_ERROR"
                kpi_data["notes"].append(f"Error during cross-validation for KPI '{kpi_name}': {e}")
            
            validated_kpis.append(kpi_data)

    extracted_values_from_current_file = {
        str(kpi_data.get("value")).replace(",", "")
        for kpi_data in extracted_kpis
        if kpi_data.get("value") is not None
    }

    for num_str in potential_unextracted_kpi_values:
        if num_str not in extracted_values_from_current_file:
            unextracted_numbers.append(num_str)

    print(f"DEBUG: KPI validation completed for file: {excel_file_path}.")
    return {
        "validated_kpis": validated_kpis,
        "unextracted_numbers": list(unextracted_numbers)
    }
