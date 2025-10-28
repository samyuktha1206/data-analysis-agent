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
import json

# SDK helpers
from claude_agent_sdk import tool, create_sdk_mcp_server

ToolResult = Dict[str, Any]

DATA_PATH = os.environ.get("DATA_PATH", "data/sample_data.csv")

# lazy-loaded dataframe reference
_df: Optional[pd.DataFrame] = None

def get_df() -> pd.DataFrame:
    """Lazy-load and cache the dataset. Raises FileNotFoundError if DATA_PATH missing."""
    global _df
    # if _df is not None:
    #     return _df
    data_path = os.environ.get("DATA_PATH", "data/sample_data.csv")
    abs_path = os.path.abspath(data_path)
    # print(f"DEBUG get_df: using DATA_PATH={data_path} abs={abs_path} exists={os.path.exists(data_path)}")
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
        return {
            "content": [
                {"type": "text", "text": json.dumps({
                    "ok": False,
                    "status": "error",
                    "error": f"Could not load dataset: {e}"
                })}
            ]
        }

    required = {"products", "revenue"}
    missing = required - set(df.columns)

    if missing:
        return {
            "content": [
                {"type": "text", "text": json.dumps({
                    "ok": False,
                    "status": "insufficient",
                    "error": f"Missing columns: {', '.join(sorted(missing))}"
                })}
            ]
        }
    
    if len(df) == 0:
        return {
            "content": [
                {"type": "text", "text": json.dumps({
                    "ok": False,
                    "status": "insufficient",
                    "error": "No rows in dataset."
                })}
            ]
        }
    
    issues: list[str] = []
    if df.isnull().any().any():
        issues.append("Dataset contains missing values.")
    if "revenue" in df.columns:
        negs = (pd.to_numeric(df["revenue"], errors="coerce") < 0).sum()
        if negs:
            issues.append(f"{negs} rows have negative revenue.")

    return {"content":[{"type":"text","text": json.dumps({
    "ok": True,
    "status": "valid",
    "columns": df.columns.tolist(),
    "rows": len(df),
    "issues": issues
})}]}


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
        return {
            "content": [
                {"type": "text", "text": json.dumps({
                    "ok": False,
                    "status": "error",
                    "error": f"Could not load dataset: {e}"
                })}
            ]
        }

    column = args.get("column", "revenue")
    print("DEBUG: column =", column)

    if column not in df.columns:
        return {
            "content": [
                {"type": "text", "text": json.dumps({
                    "ok": False,
                    "status": "error",
                    "error": f"Column '{column}' not found. Available: {', '.join(df.columns)}"
                })}
            ]
        }
    
    try:
        total = float(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())
    except Exception as e:
        return {
            "content": [
                {"type": "text", "text": json.dumps({
                    "ok": False,
                    "status": "error",
                    "error": f"Failed to compute total for column '{column}': {e}"
                })}
            ]
        }

    print("DEBUG: total =", total)
    
    result = {
        "ok": True,
        "status": "success",
        "result": {
            "intent": "aggregation",
            "column": column,
            "total": total
        },
        "metadata": {
            "rows_analyzed": len(df),
            "non_null_values": int(df[column].notna().sum())
        }
    }

    return {
        "content": [
            {"type": "text", "text": json.dumps(result, indent=2)}
        ]
    }


@tool(
    name="get_top_n_tool",
    description="Return top-N rows sorted by a numeric column. Input: {'column': str, 'n': int}",
    input_schema={"column": str, "n": int}
)
async def get_top_n_tool(args: Dict[str, Any]) -> ToolResult:
    print("DEBUG: get_top_n_tool called with", args)
    try:
        df = get_df()
    except Exception as e:
        return {
            "content": [
                {"type": "text", "text": json.dumps({
                    "ok": False,
                    "status": "error",
                    "error": f"Could not load dataset: {e}"
                })}
            ]
        }

    # --- Parse inputs ---
    column = args.get("column", "revenue")
    n = args.get("n", 5)
    try:
        n = int(n)
    except Exception:
        return {
            "content": [
                {"type": "text", "text": json.dumps({
                    "ok": False,
                    "status": "error",
                    "error": "'n' must be an integer."
                })}
            ]
        }

    if column not in df.columns:
        return {
            "content": [
                {"type": "text", "text": json.dumps({
                    "ok": False,
                    "status": "error",
                    "error": f"Column '{column}' not found. Available columns: {', '.join(df.columns)}"
                })}
            ]
        }
    
    # --- Compute top N ---
    try:
        df_sorted = df.sort_values(by=column, ascending=False).head(n)
        rows = df_sorted.to_dict(orient="records")
        print(f"DEBUG: returning top {n} rows by '{column}'")
    except Exception as e:
        return {
            "content": [
                {"type": "text", "text": json.dumps({
                    "ok": False,
                    "status": "error",
                    "error": f"Failed to compute top {n} rows for '{column}': {e}"
                })}
            ]
        }

    # for row in rows:
    #     print(row)
    # --- Prepare clean JSON result ---
    result = {
        "ok": True,
        "status": "success",
        "result": {
            "intent": "top_n",
            "column": column,
            "n": n,
            "rows": rows
        },
        "metadata": {
            "rows_analyzed": len(df),
            "non_null_values": int(df[column].notna().sum())
        }
    }

    # --- Return in Claude-SDK-standard format ---
    return {
        "content": [
            {"type": "text", "text": json.dumps(result, indent=2)}
        ]
    }

    # return {
    #     "content": [{
    #         "type": "text",
    #         "text": f"Found {len(rows)} rows:\n{json.dumps(rows, indent=2)}"
    #     }]
    # }


@tool(
    name="filter_by_value_tool",
    description="Filter columns by row == value. Input: {'column': str, 'value': str}",
    input_schema={"column": str, "value": str}
)
async def filter_by_value_tool(args: Dict[str, Any]) -> ToolResult:
    print("DEBUG: filter_by_value_tool called with", args)

    try:
        df = get_df()
    except Exception as e:
        err = {"ok": False, "status": "error", "error": f"Could not load dataset: {e}"}
        return {"content": [{"type": "text", "text": json.dumps(err)}]}

    column = args.get("column","products")
    value = args.get("value")
    
    print("DEBUG: requested column:", column, "value:", value)


    if not column or value is None:
        err = {"ok": False, "status": "error", "error": "Both 'column' and 'value' are required."}
        return {"content": [{"type": "text", "text": json.dumps(err)}]}
    
    # common auto-corrections for singular/plural
    col_candidates = set(df.columns)
    if column not in col_candidates:
        # try plural/singular swap
        if column.endswith("s") and column[:-1] in col_candidates:
            print(f"DEBUG: auto-correcting column '{column}' -> '{column[:-1]}'")
            column = column[:-1]
        elif (column + "s") in col_candidates:
            print(f"DEBUG: auto-correcting column '{column}' -> '{column}s'")
            column = column + "s"
    
    if column not in df.columns:
        err = {"ok": False, "status": "error", "error": f"Column '{column}' not found. Available: {', '.join(df.columns)}"}
        return {"content": [{"type": "text", "text": json.dumps(err)}]}

    # Perform filtering (case-insensitive string compare)
    try:
        filtered = df[df[column].astype(str).str.lower() == str(value).lower()]
        rows = filtered.to_dict(orient="records")
        # Ensure JSON-serializable: stringify non-serializable types then parse back to native types
        rows_json = json.dumps(rows, default=str)
        safe_rows = json.loads(rows_json)
        total = float(pd.to_numeric(filtered.get("revenue", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if "revenue" in filtered.columns and len(filtered) > 0 else 0.0

        result = {
            "ok": True,
            "status": "success",
            "result": {
                "intent": "filter",
                "column": column,
                "value": value,
                "count": len(safe_rows),
                "total": total,
                "rows": safe_rows  # at most however many matched; caller can truncate if needed
            },
            "metadata": {
                "rows_analyzed": len(df),
                "non_null_values": int(df[column].notna().sum())
            }
        }

        return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}

    except Exception as e:
        err = {"ok": False, "status": "error", "error": f"Processing/filter error: {e}"}
        return {"content": [{"type": "text", "text": json.dumps(err)}]}
    