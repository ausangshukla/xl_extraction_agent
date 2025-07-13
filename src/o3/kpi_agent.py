# kpi_agent.py
from __future__ import annotations

import io
import math
from typing import List, Dict, TypedDict, Any, Optional

import pandas as pd
from pydantic import BaseModel, Field, ValidationError, validator

from src.config.llm_config import get_gemini_llm
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langgraph.graph import StateGraph, START, END


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
    dir_path = Path(state["csv_dir"])
    if not dir_path.is_dir():
        raise FileNotFoundError(f"{dir_path} is not a directory")

    dfs = []
    for fp in sorted(dir_path.glob("*.csv")):           # deterministic order
        df = pd.read_csv(fp, dtype=str).fillna("")
        dfs.append(df)

    if not dfs:
        raise RuntimeError(f"No CSV files found in {dir_path}")

    state["dfs"] = dfs
    return state


def _ensure_first_col_is_kpi(df: pd.DataFrame) -> pd.DataFrame:
    """Makes sure the first column is named 'KPI' for clarity."""
    if df.columns[0].lower() != "kpi":
        df = df.rename(columns={df.columns[0]: "KPI"})
    return df.reset_index(drop=True)


def orient_and_chunk_all(state: ExtractionState,
                         chunk_rows: int = 40) -> ExtractionState:
    """Convert each DataFrame to markdown, then chunk rows for LLM."""
    table_chunks: List[str] = []

    for df in state["dfs"]:
        df = _ensure_first_col_is_kpi(df)

        # Break big tables into ≤chunk_rows KPI rows each
        for start in range(0, len(df), chunk_rows):
            sub_df = df.iloc[start:start + chunk_rows]
            table_chunks.append(sub_df.to_markdown(index=False))

    state["table_chunks"] = table_chunks
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
    "Return ONLY a JSON array that matches the schema of the provided tool."
)

def extract_kpis_llm(state: ExtractionState, model: Optional[BaseChatModel] = None
                     ) -> ExtractionState:
    if model is None:
        llm = get_gemini_llm()
        model = llm.bind_tools(tools=[EXTRACT_TOOL_SCHEMA])

    target_list = ", ".join(state["target_kpis"])
    all_records: List[Dict[str, Any]] = []

    for chunk in state["table_chunks"]:
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
            print("WARNING: LLM failed to call the tool. Skipping chunk.")
            continue

        call_args = msg.tool_calls[0]["args"]
        try:
            # The schema expects an object with a "kpis" key, which is a list
            parsed = [KPIRecord.model_validate(rec).model_dump() for rec in call_args["kpis"]]
            all_records.extend(parsed)
        except (ValidationError, KeyError) as e:
            raise RuntimeError(f"LLM returned invalid schema or arguments: {e}") from e

    state["extracted"] = all_records
    return state


# 2-D · Combine step is trivial here (already merged)
def combine(state: ExtractionState) -> ExtractionState:
    # Deduplicate by (name, header) keeping first occurrence
    unique: Dict[tuple, Dict[str, Any]] = {}
    for rec in state["extracted"]:
        key = (rec["name"].lower(), rec["header"])
        if key not in unique:
            unique[key] = rec
    state["extracted"] = list(unique.values())
    return state


# --------------------------------------------------------------------------- #
# 3. ----------  VERIFICATION NODES  ---------------------------------------- #
# --------------------------------------------------------------------------- #

def schema_validator(state: VerificationState) -> VerificationState:
    issues = state.get("issues", [])
    for rec in state["extracted"]:
        try:
            KPIRecord.model_validate(rec)
        except ValidationError as ve:
            issues.append(f"Invalid record structure: {ve}")
    state["issues"] = issues
    return state


def missing_kpi_checker(state: VerificationState) -> VerificationState:
    extracted_names = {r["name"].lower() for r in state["extracted"]}
    missing = [k for k in state["target_kpis"] if k.lower() not in extracted_names]
    if missing:
        state.setdefault("issues", []).append(f"Missing KPIs: {missing}")
    return state


def duplicate_checker(state: VerificationState) -> VerificationState:
    seen = set()
    dups = []
    for rec in state["extracted"]:
        key = (rec["name"].lower(), rec["header"])
        if key in seen:
            dups.append(key)
        seen.add(key)
    if dups:
        state.setdefault("issues", []).append(f"Duplicate KPI/period pairs: {dups}")
    return state


def value_sanity_checker(state: VerificationState,
                         large_threshold: float = 1e12) -> VerificationState:
    weirds = []
    for rec in state["extracted"]:
        val = rec["value"]
        if math.isnan(val) or abs(val) > large_threshold:
            weirds.append((rec["name"], rec["header"], val))
    if weirds:
        state.setdefault("issues", []).append(f"Implausible numbers: {weirds}")
    return state


def report_generator(state: VerificationState) -> VerificationState:
    if state.get("issues"):
        print("⚠️  Verification failed:")
        for issue in state["issues"]:
            print("-", issue)
    else:
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
    g.add_node("report", report_generator)

    g.add_edge(START, "schema")
    g.add_edge("schema", "missing")
    g.add_edge("missing", "dups")
    g.add_edge("dups", "sanity")
    g.add_edge("sanity", "report")
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
