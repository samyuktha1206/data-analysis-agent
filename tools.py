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
    """
    Returns a JSON object inside content text block with:
    {
      ok: bool,
      status: "valid"|"insufficient"|"error",
      columns: [...],
      rows: n,
      issues: [...],
      missing_summary: {
         "<col>": {"count": int, "examples": [{"index": i, "products": "name", "value": "<raw>"} ...]}
      },
      negative_revenue: [{"index": i, "products": "name", "revenue": -123.0}, ...]
    }
    """
    MAX_EXAMPLES = 5

    try:
        df = get_df()
        print("DEBUG validate_data_tool: loading df from", DATA_PATH)
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
                    "error": f"Missing columns: {', '.join(sorted(missing))}",
                    "columns": df.columns.tolist(),
                    "rows": len(df),
                })}
            ]
        }
    
    if len(df) == 0:
        print("DEBUG validate_data_tool: df.columns=", df.columns.tolist(), "rows=", len(df))
        print("DEBUG validate_data_tool: returning payload:", payload)
        return {
            "content": [
                {"type": "text", "text": json.dumps({
                    "ok": False,
                    "status": "insufficient",
                    "error": "No rows in dataset."
                })}
            ]
        }
    
   
    empty_mask = df.isnull() | df.astype(str).apply(lambda col: col.str.strip() == "")

    missing_summary = {}
    for col in df.columns:
        col_mask = empty_mask[col]
        count = int(col_mask.sum())
        if count > 0:
            # Gather up to MAX_EXAMPLES example rows for this column
            examples = []
            # Use .loc to keep original index; convert index to int if possible
            for i, val in df.loc[col_mask, [col, "products"]].head(MAX_EXAMPLES).iterrows():
                examples.append({
                    "index": int(i) if isinstance(i, (int,)) or (str(i).isdigit()) else str(i),
                    "products": (val.get("products") if "products" in df.columns else None),
                    "value": None if pd.isna(val[col]) else str(val[col])
                })
            missing_summary[col] = {"count": count, "examples": examples}

    # Negative revenue detection
    negative_revenue = []
    if "revenue" in df.columns:
        # coerce to numeric, keep original index and products
        rev_numeric = pd.to_numeric(df["revenue"], errors="coerce")
        neg_mask = rev_numeric < 0
        neg_count = int(neg_mask.sum())
        if neg_count > 0:
            # collect examples (all or up to MAX_EXAMPLES)
            for i, row in df.loc[neg_mask, ["products", "revenue"]].head(MAX_EXAMPLES).iterrows():
                # coerce revenue to float safely
                try:
                    rev_val = float(row["revenue"])
                except Exception:
                    rev_val = None
                negative_revenue.append({
                    "index": int(i) if isinstance(i, (int,)) or (str(i).isdigit()) else str(i),
                    "products": row.get("products"),
                    "revenue": rev_val
                })

    issues = []
    if len(missing_summary) > 0:
        issues.append(f"Missing values present in columns: {', '.join(missing_summary.keys())}")
    if len(negative_revenue) > 0:
        issues.append(f"{len(negative_revenue)} rows have negative revenue (examples included)")

    ok = (len(issues) == 0)
    status = "valid" if ok else "insufficient"

    payload = {
        "ok": ok,
        "status": status,
        "columns": df.columns.tolist(),
        "rows": int(len(df)),
        "issues": issues,
        "missing_summary": missing_summary,
        "negative_revenue_examples": negative_revenue
    }

    return {"content": [{"type": "text", "text": json.dumps(payload, indent=2, default=str)}]}



@tool(
    name="calculate_total_tool",
    description="Calculate the total of a numeric column. Input: {'column': str}",
    input_schema={"column": str}
)
async def calculate_total_tool(args: Dict[str, Any]) -> ToolResult:
    print("DEBUG: calculate_total_tool called with", args)
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

    column = args.get("column", "revenue")

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
    