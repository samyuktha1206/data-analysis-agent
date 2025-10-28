# tools.py
"""
Core tools:
- validate_data_tool
- calculate_total_tool
- get_top_n_tool
- filter_by_value_tool

Each tool returns JSON-serializable dicts and avoids raising unhandled exceptions.
"""

from typing import Any, Dict, Optional
import os
import pandas as pd

# SDK helpers
from claude_agent_sdk import tool, create_sdk_mcp_server

ToolResult = Dict[str, Any]

DATA_PATH = os.environ.get("DATA_PATH", "data/sample_data.csv")

# lazy-loaded dataframe reference
_df: Optional[pd.DataFrame] = None

def get_df() -> pd.DataFrame:
    """Lazy-load and cache the dataset. Raises FileNotFoundError if DATA_PATH missing."""
    global _df
    if _df is not None:
        return _df
    print(os.environ.get("DATA_PATH"))
    data_path = os.environ.get("DATA_PATH", "data/sample_data.csv")
    abs_path = os.path.abspath(data_path)
    print(f"DEBUG get_df: using DATA_PATH={data_path} abs={abs_path} exists={os.path.exists(data_path)}")
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found at {abs_path}")
    _df = pd.read_csv(data_path)
    _df.columns = _df.columns.str.lower() 
    # if not os.path.exists(DATA_PATH):
    #     raise FileNotFoundError(f"Data file not found at {DATA_PATH}")
    # _df = pd.read_csv(DATA_PATH)
    return _df

# -----------------------
# Tool Implementations
# -----------------------

@tool(
    name="validate_data_tool",
    description="Check dataset for missing columns or rows.",
    input_schema={}
)
async def validate_data_tool(args: Dict[str, Any]) -> ToolResult:
    try:
        df = get_df()
    except Exception as e:
        return {"ok": False, "error": f"Could not load dataset: {e}"}

    required = {"product", "revenue"}
    missing = required - set(df.columns)

    if missing:
        return {"ok": False, "status": "insufficient", "message": f"Missing columns: {', '.join(sorted(missing))}"}

    if len(df) == 0:
        return {"ok": False, "status": "insufficient", "message": "No rows in dataset."}

    issues: list[str] = []
    if df.isnull().any().any():
        issues.append("Dataset contains missing values.")
    if "revenue" in df.columns:
        negs = (pd.to_numeric(df["revenue"], errors="coerce") < 0).sum()
        if negs:
            issues.append(f"{negs} rows have negative revenue.")

    return {"ok": True, "status": "valid", "issues": issues, "rows": int(len(df))}


@tool(
    name="calculate_total_tool",
    description="Calculate the total of a numeric column. Input: {'column': str}",
    input_schema={"column": str}
)
async def calculate_total_tool(args: Dict[str, Any]) -> ToolResult:
    print("DEBUG: calculate_total_tool called with", args)
    try:
        df = get_df()
        print(df)
    except Exception as e:
        return {"ok": False, "error": f"Could not load dataset: {e}"}

    column = args.get("column", "revenue")
    print(column)
    if column not in df.columns:
        return {"ok": False, "error": f"Column '{column}' not found. Available: {', '.join(df.columns)}"}

    total = float(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())
    print(total)
    # return {"ok": True, "result": {"column": column, "total": total}}
    return {
        "content": [
            {"type": "text", "text": f"{column}, {total}"}
        ]
    }


@tool(
    name="get_top_n_tool",
    description="Return top-N rows sorted by a numeric column. Input: {'column': str, 'n': int}",
    input_schema={"column": str, "n": int}
)
async def get_top_n_tool(args: Dict[str, Any]) -> ToolResult:
    try:
        df = get_df()
    except Exception as e:
        return {"ok": False, "error": f"Could not load dataset: {e}"}

    column = args.get("column", "revenue")
    n = args.get("n", 5)
    try:
        n = int(n)
    except Exception:
        return {"ok": False, "error": "'n' must be an integer."}

    if column not in df.columns:
        return {"ok": False, "error": f"Column '{column}' not found. Available: {', '.join(df.columns)}"}

    df_sorted = df.sort_values(by=column, ascending=False).head(n)
    rows = df_sorted.to_dict(orient="records")
    for row in rows:
        print(row)
    # return {"ok": True, "result": {"n": n, "rows": rows}}
    # return {
    #     "content": [
    #         {"type": "content", "content": {rows}}
    #     ]
    # }
    return rows


@tool(
    name="filter_by_value_tool",
    description="Filter rows by column == value. Input: {'column': str, 'value': str}",
    input_schema={"column": str, "value": str}
)
async def filter_by_value_tool(args: Dict[str, Any]) -> ToolResult:
    try:
        df = get_df()
    except Exception as e:
        return {"ok": False, "error": f"Could not load dataset: {e}"}

    column = args.get("column")
    value = args.get("value")

    if not column or value is None:
        return {"ok": False, "error": "Both 'column' and 'value' are required."}
    if column not in df.columns:
        return {"ok": False, "error": f"Column '{column}' not found. Available: {', '.join(df.columns)}"}

    filtered = df[df[column].astype(str).str.lower() == str(value).lower()]
    rows = filtered.to_dict(orient="records")
    total = float(pd.to_numeric(filtered["revenue"], errors="coerce").fillna(0).sum()) if "revenue" in filtered.columns and len(filtered) > 0 else 0.0

    return {"ok": True, "result": {"count": len(rows), "total": total, "rows": rows}}


# ---------------------
# Create MCP server exposing tools
# ---------------------

# dataAnalysis = create_sdk_mcp_server(
#     name="dataAnalysis",
#     version="2.0.0",
#     tools=[
#         validate_data_tool,
#         calculate_total_tool,
#         get_top_n_tool,
#         filter_by_value_tool,
#     ],
# )
