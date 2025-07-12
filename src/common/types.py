from typing import List, Dict, Any, TypedDict

class AgentState(TypedDict):
    folder_path: str
    kpis: List[str]
    extracted_data: List[Dict[str, Any]]
    file_paths: List[str]
    current_file_index: int
    validation_results: Dict[str, Dict[str, Any]]