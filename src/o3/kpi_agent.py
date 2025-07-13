# kpi_agent.py
from __future__ import annotations

import io
import math
import logging # Import logging module
from typing import List, Dict, TypedDict, Any, Optional

import pandas as pd
from pydantic import BaseModel, Field, ValidationError, validator

from src.config.llm_config import get_gemini_llm
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langgraph.graph import StateGraph, START, END

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --------------------------------------------------------------------------- #
# 1. ----------  STATE SCHEMAS  --------------------------------------------- #
# --------------------------------------------------------------------------- #

class KPIRecord(BaseModel):
    """Schema returned by the LLM."""
    name: str
    value: float
    header: str
    row: int
    col: int


class ExtractionState(TypedDict, total=False):
    csv_dir: str                 # NEW – directory path containing CSVs
    target_kpis: List[str]
    dfs: List[pd.DataFrame]      # NEW – many DataFrames, one per CSV
    table_chunks: List[str]
    chunk_row_offsets: List[int] # NEW - starting row index for each chunk
    extracted: List[Dict[str, Any]]


class VerificationState(TypedDict, total=False):
    """Shared state passed between verification nodes."""
    df: pd.DataFrame
    target_kpis: List[str]
    extracted: List[Dict[str, Any]]
    issues: List[str]


# --------------------------------------------------------------------------- #
# 2. ----------  EXTRACTION NODES  ------------------------------------------ #
# --------------------------------------------------------------------------- #

def load_csvs_from_dir(state: ExtractionState) -> ExtractionState:
    """Reads every *.csv file in `csv_dir` into a DataFrame."""
    logging.info(f"Starting to load CSVs from directory: {state['csv_dir']}")
    dir_path = Path(state["csv_dir"])
    if not dir_path.is_dir():
        logging.error(f"Directory not found: {dir_path}")
        raise FileNotFoundError(f"{dir_path} is not a directory")

    dfs = []
    for fp in sorted(dir_path.glob("*.csv")):           # deterministic order
        logging.info(f"Loading CSV file: {fp}")
        df = pd.read_csv(fp, dtype=str).fillna("")
        dfs.append(df)

    if not dfs:
        logging.error(f"No CSV files found in {dir_path}")
        raise RuntimeError(f"No CSV files found in {dir_path}")

    state["dfs"] = dfs
    logging.info(f"Successfully loaded {len(dfs)} CSV files.")
    return state


def _ensure_first_col_is_kpi(df: pd.DataFrame) -> pd.DataFrame:
    """Makes sure the first column is named 'KPI' for clarity."""
    if df.columns[0].lower() != "kpi":
        df = df.rename(columns={df.columns[0]: "KPI"})
    return df.reset_index(drop=True)


def orient_and_chunk_all(state: ExtractionState,
                         chunk_rows: int = 200) -> ExtractionState:
    """Convert each DataFrame to markdown, then chunk rows for LLM."""
    logging.info("Starting to orient and chunk DataFrames.")
    table_chunks: List[str] = []
    chunk_row_offsets: List[int] = []

    for i, df in enumerate(state["dfs"]):
        logging.info(f"Processing DataFrame {i+1}/{len(state['dfs'])}")
        df = _ensure_first_col_is_kpi(df)

        # Break big tables into ≤chunk_rows KPI rows each
        for start in range(0, len(df), chunk_rows):
            sub_df = df.iloc[start:start + chunk_rows]
            # Disable column width limit so LLM sees the full table
            table_chunks.append(
                sub_df.to_markdown(
                    index=True, # Change to True to include DataFrame index as a column
                    tablefmt="pipe",
                    maxcolwidths=[None] * len(sub_df.columns)
                )
            )
            chunk_row_offsets.append(start) # Store the starting row index of this chunk
            logging.debug(f"Created chunk from row {start} to {start + chunk_rows - 1}")

    state["table_chunks"] = table_chunks
    state["chunk_row_offsets"] = chunk_row_offsets
    logging.info(f"Finished orienting and chunking. Total chunks created: {len(table_chunks)}")
    return state


# 2-C · Call the LLM on every chunk and collect outputs
EXTRACT_TOOL_SCHEMA = {
    "name": "extract_kpis",
    "description": "Return KPI values with sheet coordinates",
    "parameters": {
        "type": "object",
        "properties": {
            "kpis": {
                "type": "array",
                "items": KPIRecord.model_json_schema(),
            }
        },
        "required": ["kpis"],
    },
}

SYSTEM_PROMPT = (
    "You are a finance assistant. "
    "You receive (1) a markdown table and (2) a list of KPI names to extract.\n"
    "The markdown table includes an index column (the first column) which represents the original row number from the source data. "
    "Ensure all columns in the provided markdown table are considered for extraction.\n"
    "Return ONLY a JSON array that matches the schema of the provided tool. "
    "For the 'row' field, use the value from the index column (the first column) of the markdown table, adjusted to be 1-based."
)

def extract_kpis_llm(state: ExtractionState, model: Optional[BaseChatModel] = None
                     ) -> ExtractionState:
    logging.info("Starting KPI extraction using LLM.")
    if model is None:
        llm = get_gemini_llm()
        model = llm.bind_tools(tools=[EXTRACT_TOOL_SCHEMA])

    target_list = ", ".join(state["target_kpis"])
    all_records: List[Dict[str, Any]] = []

    for i, chunk in enumerate(state["table_chunks"]):
        logging.info(f"Processing chunk {i+1}/{len(state['table_chunks'])} for LLM extraction.")
        user_prompt = (
            f"**KPI list**: {target_list}\n"
            f"**Table**:\n```markdown\n{chunk}\n```\n\n"
            "Respond via the extract_kpis tool."
        )
        msg = model.invoke(
            [SystemMessage(content=SYSTEM_PROMPT),
             HumanMessage(content=user_prompt)],
            tool_choice="extract_kpis"
        )

        # The model MUST call the tool. Grab its arguments.
        if not msg.tool_calls:
            logging.warning("LLM failed to call the tool. Skipping chunk.")
            continue

        call_args = msg.tool_calls[0]["args"]
        try:
            # The schema expects an object with a "kpis" key, which is a list
            parsed = [KPIRecord.model_validate(rec).model_dump() for rec in call_args["kpis"]]
            
            all_records.extend(parsed)
            logging.info(f"Successfully extracted {len(parsed)} records from chunk {i+1}.")
        except (ValidationError, KeyError) as e:
            logging.error(f"LLM returned invalid schema or arguments for chunk {i+1}: {e}")
            raise RuntimeError(f"LLM returned invalid schema or arguments: {e}") from e

    state["extracted"] = all_records
    logging.info(f"Finished LLM extraction. Total records extracted: {len(all_records)}")
    return state


# 2-D · Combine step is trivial here (already merged)
def combine(state: ExtractionState) -> ExtractionState:
    logging.info("Starting combination and deduplication of extracted records.")
    # Deduplicate by (name, header) keeping first occurrence
    unique: Dict[tuple, Dict[str, Any]] = {}
    initial_count = len(state["extracted"])
    for rec in state["extracted"]:
        key = (rec["name"].lower(), rec["header"])
        if key not in unique:
            unique[key] = rec
    state["extracted"] = list(unique.values())
    final_count = len(state["extracted"])
    logging.info(f"Finished combination. Deduplicated {initial_count - final_count} records. Total unique records: {final_count}")
    return state


# --------------------------------------------------------------------------- #
# 3. ----------  VERIFICATION NODES  ---------------------------------------- #
# --------------------------------------------------------------------------- #

def schema_validator(state: VerificationState) -> VerificationState:
    logging.info("Starting schema validation of extracted records.")
    issues = state.get("issues", [])
    for i, rec in enumerate(state["extracted"]):
        try:
            KPIRecord.model_validate(rec)
            logging.debug(f"Record {i} passed schema validation.")
        except ValidationError as ve:
            issue_msg = f"Invalid record structure for record {i}: {ve}"
            issues.append(issue_msg)
            logging.warning(issue_msg)
    state["issues"] = issues
    logging.info(f"Finished schema validation. Found {len(issues) - len(state.get('issues', []))} new issues.")
    return state


def missing_kpi_checker(state: VerificationState) -> VerificationState:
    logging.info("Starting missing KPI check.")
    extracted_names = {r["name"].lower() for r in state["extracted"]}
    missing = [k for k in state["target_kpis"] if k.lower() not in extracted_names]
    if missing:
        issue_msg = f"Missing KPIs: {missing}"
        state.setdefault("issues", []).append(issue_msg)
        logging.warning(issue_msg)
    else:
        logging.info("No missing KPIs found.")
    return state


def duplicate_checker(state: VerificationState) -> VerificationState:
    logging.info("Starting duplicate KPI check.")
    seen = set()
    dups = []
    for i, rec in enumerate(state["extracted"]):
        key = (rec["name"].lower(), rec["header"])
        if key in seen:
            dups.append(key)
            logging.warning(f"Duplicate KPI/period pair found: {key} at record {i}.")
        seen.add(key)
    if dups:
        state.setdefault("issues", []).append(f"Duplicate KPI/period pairs: {dups}")
    else:
        logging.info("No duplicate KPI/period pairs found.")
    return state


def _normalize_number_string(s: str) -> float:
    """Cleans and converts a string to a float, handling common financial formats, including percentages."""
    s = s.strip().replace(",", "").replace("$", "")
    
    is_percentage = False
    if s.endswith("%"):
        s = s[:-1] # Remove the '%' sign
        is_percentage = True

    try:
        value = float(s)
        if is_percentage:
            value /= 100.0 # Convert percentage to decimal
        return value
    except ValueError:
        return math.nan


def cross_reference_checker(state: VerificationState,
                            tolerance: float = 1e-6) -> VerificationState:
    logging.info("Starting cross-reference check with source DataFrame.")
    issues = state.get("issues", [])
    df = state["df"]
    
    # Create a mapping from column header to column index for efficient lookup
    header_to_col_idx = {col: i for i, col in enumerate(df.columns)}

    for i, rec in enumerate(state["extracted"]):
        kpi_name = rec["name"]
        extracted_value = rec["value"]
        # Adjust row and column indices from 1-based (LLM output) to 0-based (pandas iloc)
        row_idx_0based = rec["row"] - 1
        col_idx_0based = rec["col"] - 1 # Assuming 'col' is also 1-based from LLM

        header = rec["header"]
        
        logging.info(f"Checking record {i}: KPI='{kpi_name}', Extracted Value='{extracted_value}', Original Row={rec['row']}, Original Col={rec['col']}, Header='{header}'")

        # Check if header exists in DataFrame
        if header not in header_to_col_idx:
            issue_msg = f"Extracted KPI '{kpi_name}' has unknown header '{header}' at row {rec['row']}."
            issues.append(issue_msg)
            logging.warning(issue_msg)
            continue

        # Use the 0-based column index from the header mapping
        col_idx_from_header = header_to_col_idx[header]

        # It's possible LLM's 'col' is not consistent with 'header' mapping, prioritize header
        if col_idx_0based != col_idx_from_header:
            logging.debug(f"LLM's col ({rec['col']}) differs from header's col ({col_idx_from_header + 1}). Using header's column.")
            col_idx_0based = col_idx_from_header


        # Check if row index is valid
        if not (0 <= row_idx_0based < len(df)):
            issue_msg = f"Extracted KPI '{kpi_name}' has invalid row index {rec['row']} (0-based: {row_idx_0based}) for header '{header}'. DataFrame has {len(df)} rows."
            issues.append(issue_msg)
            logging.warning(issue_msg)
            continue

        # Get the value from the DataFrame
        try:
            df_value_str = df.iloc[row_idx_0based, col_idx_0based]
            df_value = _normalize_number_string(str(df_value_str)) # Ensure it's a string before normalizing
            logging.debug(f"Source value for '{kpi_name}' at ({row_idx_0based}, {col_idx_0based}): '{df_value_str}' (normalized: {df_value})")
        except IndexError:
            issue_msg = f"Could not access cell at row {row_idx_0based}, col {col_idx_0based} for KPI '{kpi_name}'. Index out of bounds."
            issues.append(issue_msg)
            logging.error(issue_msg)
            continue
        except Exception as e:
            issue_msg = f"Error processing DataFrame value for KPI '{kpi_name}' at ({rec['row']}, {header}): {e}"
            issues.append(issue_msg)
            logging.error(issue_msg)
            continue

        # Compare values
        if math.isnan(extracted_value) and math.isnan(df_value):
            logging.debug(f"Both extracted and source values for '{kpi_name}' are NaN. Considered a match.")
            continue # Both are NaN, consider them a match
        elif math.isnan(extracted_value) or math.isnan(df_value):
            issue_msg = f"Value mismatch for KPI '{kpi_name}' at ({rec['row']}, {header}): Extracted '{extracted_value}', Source '{df_value_str}' (one is NaN)."
            issues.append(issue_msg)
            logging.warning(issue_msg)
        elif abs(extracted_value - df_value) > tolerance:
            issue_msg = f"Value mismatch for KPI '{kpi_name}' at ({rec['row']}, {header}): Extracted '{extracted_value}', Source '{df_value_str}'. Difference: {abs(extracted_value - df_value):.2e}"
            issues.append(issue_msg)
            logging.warning(issue_msg)
        else:
            logging.debug(f"Value for '{kpi_name}' at ({rec['row']}, {header}) matches source within tolerance.")
            
    state["issues"] = issues
    logging.info(f"Finished cross-reference check. Found {len(issues) - len(state.get('issues', []))} new issues.")
    return state


def value_sanity_checker(state: VerificationState,
                         large_threshold: float = 1e12) -> VerificationState:
    logging.info("Starting value sanity check.")
    weirds = []
    for i, rec in enumerate(state["extracted"]):
        val = rec["value"]
        if math.isnan(val) or abs(val) > large_threshold:
            weirds.append((rec["name"], rec["header"], val))
            logging.warning(f"Implausible number found for record {i}: KPI='{rec['name']}', Header='{rec['header']}', Value='{val}'")
    if weirds:
        state.setdefault("issues", []).append(f"Implausible numbers: {weirds}")
    else:
        logging.info("No implausible numbers found.")
    return state


def report_generator(state: VerificationState) -> VerificationState:
    issues = state.get("issues", [])
    if issues:
        mismatch_count = sum(1 for issue in issues if "Value mismatch" in issue)
        logging.info(f"Verification completed with {len(issues)} issues found. Total value mismatches: {mismatch_count}")
        print("⚠️  Verification failed:")
        for issue in issues:
            print("-", issue)
    else:
        logging.info("Verification completed with no issues.")
        print("✅ Verification passed with no issues.")
    return state


# --------------------------------------------------------------------------- #
# 4. ----------  GRAPH BUILDERS  -------------------------------------------- #
# --------------------------------------------------------------------------- #

def build_extraction_graph(llm: Optional[ChatGoogleGenerativeAI] = None):
    g = StateGraph(ExtractionState)

    g.add_node("load_dir",   load_csvs_from_dir)
    g.add_node("chunk_all",  orient_and_chunk_all)
    g.add_node("llm_extract", lambda s: extract_kpis_llm(s, model=llm))
    g.add_node("combine",    combine)

    g.add_edge(START, "load_dir")
    g.add_edge("load_dir", "chunk_all")
    g.add_edge("chunk_all", "llm_extract")
    g.add_edge("llm_extract", "combine")
    g.add_edge("combine", END)

    return g.compile()

def build_verification_graph():
    g = StateGraph(VerificationState)
    g.add_node("schema", schema_validator)
    g.add_node("missing", missing_kpi_checker)
    g.add_node("dups", duplicate_checker)
    g.add_node("sanity", value_sanity_checker)
    g.add_node("cross_ref", cross_reference_checker) # New node
    g.add_node("report", report_generator)

    g.add_edge(START, "schema")
    g.add_edge("schema", "missing")
    g.add_edge("missing", "dups")
    g.add_edge("dups", "sanity")
    g.add_edge("sanity", "cross_ref") # Connect sanity to cross_ref
    g.add_edge("cross_ref", "report") # Connect cross_ref to report
    g.add_edge("report", END)

    return g.compile()


# --------------------------------------------------------------------------- #
# 5. ----------  DEMO ------------------------------------------------------- #
# --------------------------------------------------------------------------- #

# kpi_agent.py  – replace just the block under  `if __name__ == "__main__":`
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import argparse, sys
    from pathlib import Path

    parser = argparse.ArgumentParser(
        description="Run KPI-extraction + verification on all CSVs in a folder")
    parser.add_argument(
        "csv_dir",
        help="Path to the directory that contains one or more *.csv files")
    parser.add_argument(
        "--kpis",
        required=True,
        help="Comma-separated list of KPI names to extract, e.g. "
             "'Revenue, EBITDA, Gross Profit'")
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir).expanduser().resolve()
    if not csv_dir.is_dir():
        sys.exit(f"❌  {csv_dir} is not a directory")

    target_kpis = [k.strip() for k in args.kpis.split(",") if k.strip()]
    if not target_kpis:
        sys.exit("❌  --kpis argument must contain at least one KPI name")

    # ---------- Build graphs ------------------------------------------------
    extraction_graph   = build_extraction_graph()
    verification_graph = build_verification_graph()

    # ---------- Run extraction across the whole folder ----------------------
    extract_state = extraction_graph.invoke({
        "csv_dir": str(csv_dir),
        "target_kpis": target_kpis
    })
    extracted_json = extract_state["extracted"]
    all_dfs        = extract_state["dfs"]          # list of DataFrames

    print("\n=== Extracted KPI records ===")
    for rec in extracted_json:
        print(rec)

    # ---------- Run verification -------------------------------------------
    verify_state = verification_graph.invoke({
        "df": pd.concat(all_dfs, ignore_index=True),
        "target_kpis": target_kpis,
        "extracted":   extracted_json
    })
