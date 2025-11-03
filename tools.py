# # tools.py
# """
# Core tools:
# - validate_data_tool
# - calculate_total_tool
# - get_top_n_tool
# - filter_by_value_tool

# Each tool returns JSON-serializable dicts and avoids raising unhandled exceptions.
# """

# from typing import Any, Dict, Optional
# import os
# import pandas as pd
# import json

# # SDK helpers
# from claude_agent_sdk import tool

# ToolResult = Dict[str, Any]

# DATA_PATH = os.environ.get("DATA_PATH", "data/sample_data.csv")

# # lazy-loaded dataframe reference
# #df: Optional[pd.DataFrame] = None

# def get_df() -> pd.DataFrame:
#     """Lazy-load and cache the dataset. Raises FileNotFoundError if DATA_PATH missing.
#     We intentionally reload the dataset each time to reflect any changes on disk.
#     If desired, caching can be enabled by uncommenting the relevant lines.
#     """
#     global _df
#     # If performing data validation once per session is enough then use caching:
#     # if _df is not None: 
#         # return _df
#     abs_path = os.path.abspath(DATA_PATH)
#     # print(f"DEBUG get_df: using DATA_PATH={DATA_PATH} abs={abs_path} exists={os.path.exists(DATA_PATH)}")
#     if not os.path.exists(abs_path):
#         raise FileNotFoundError(f"Data file not found at {abs_path}")
#     _df = pd.read_csv(abs_path)
#     _df.columns = _df.columns.str.lower() # normalize to lowercase
#     return _df

# # -----------------------
# # Tool Implementations
# # -----------------------

# @tool(
#     name="validate_data_tool",
#     description=(
#         "Performs a thorough validation of the loaded dataset. It checks for missing or empty values "
#         "in any column, identifies rows with negative revenue values, and verifies that key columns like "
#         "'products' and 'revenue' exist. It should be used as the first step before any aggregation or "
#         "filtering to confirm the data is clean and ready for analysis. The tool returns structured "
#         "JSON describing missing columns, number of rows, empty cells per column (with examples), "
#         "and rows with negative revenue. It should not be used to compute totals or filter data."
#     ),
#     input_schema={}
# )
# async def validate_data_tool(args: Dict[str, Any]) -> ToolResult:
#     """
#     Returns a JSON object inside content text block with:
#     {
#       ok: bool,
#       status: "valid"|"insufficient"|"error",
#       columns: [...],
#       rows: n,
#       issues: [...],
#       missing_summary: {
#          "<col>": {"count": int, "examples": [{"index": i, "products": "name", "value": "<raw>"} ...]}
#       },
#       negative_revenue: [{"index": i, "products": "name", "revenue": -123.0}, ...]
#     }
#     """
#     MAX_EXAMPLES = 5

#     try:
#         df = get_df()
#         # print("DEBUG validate_data_tool: loading df from", DATA_PATH)
#     except Exception as e:
#         return {
#             "content": [
#                 {"type": "text", "text": json.dumps({
#                     "ok": False,
#                     "status": "error",
#                     "error": f"Could not load dataset: {e}"
#                 })}
#             ]
#         }

#     required = {"products", "revenue"}
#     missing = required - set(df.columns)

#     if missing:
#         return {
#             "content": [
#                 {"type": "text", "text": json.dumps({
#                     "ok": False,
#                     "status": "insufficient",
#                     "error": f"Missing columns: {', '.join(sorted(missing))}",
#                     "columns": df.columns.tolist(),
#                     "rows": len(df),
#                 })}
#             ]
#         }
    
#     if len(df) == 0:
#         print("DEBUG validate_data_tool: df.columns=", df.columns.tolist(), "rows=", len(df))
#         return {
#             "content": [
#                 {"type": "text", "text": json.dumps({
#                     "ok": False,
#                     "status": "insufficient",
#                     "error": "No rows in dataset."
#                 })}
#             ]
#         }
    
   
#     empty_mask = df.isnull() | df.astype(str).apply(lambda col: col.str.strip() == "")

#     missing_summary = {}
#     for col in df.columns:
#         col_mask = empty_mask[col]
#         count = int(col_mask.sum())
#         if count > 0:
#             # Gather up to MAX_EXAMPLES example rows for this column
#             examples = []
#             # Use .loc to keep original index; convert index to int if possible
#             for i, val in df.loc[col_mask, [col, "products"]].head(MAX_EXAMPLES).iterrows():
#                 examples.append({
#                     "index": int(i) if isinstance(i, (int,)) or (str(i).isdigit()) else str(i),
#                     "products": (val.get("products") if "products" in df.columns else None),
#                     "value": None if pd.isna(val[col]) else str(val[col])
#                 })
#             missing_summary[col] = {"count": count, "examples": examples}

#     # Negative revenue detection
#     negative_revenue = []
#     if "revenue" in df.columns:
#         # coerce to numeric, keep original index and products
#         rev_numeric = pd.to_numeric(df["revenue"], errors="coerce")
#         neg_mask = rev_numeric < 0
#         neg_count = int(neg_mask.sum())
#         if neg_count > 0:
#             # collect examples (all or up to MAX_EXAMPLES)
#             for i, row in df.loc[neg_mask, ["products", "revenue"]].head(MAX_EXAMPLES).iterrows():
#                 # coerce revenue to float safely
#                 try:
#                     rev_val = float(row["revenue"])
#                 except Exception:
#                     rev_val = None
#                 negative_revenue.append({
#                     "index": int(i) if isinstance(i, (int,)) or (str(i).isdigit()) else str(i),
#                     "products": row.get("products"),
#                     "revenue": rev_val
#                 })

#     issues = []
#     if len(missing_summary) > 0:
#         issues.append(f"Missing values present in columns: {', '.join(missing_summary.keys())}")
#     if len(negative_revenue) > 0:
#         issues.append(f"{len(negative_revenue)} rows have negative revenue (examples included)")

#     ok = (len(issues) == 0)
#     status = "valid" if ok else "insufficient"

#     payload = {
#         "ok": ok,
#         "status": status,
#         "columns": df.columns.tolist(),
#         "rows": int(len(df)),
#         "issues": issues,
#         "missing_summary": missing_summary,
#         "negative_revenue_examples": negative_revenue
#     }

#     return {"content": [{"type": "text", "text": json.dumps(payload, indent=2, default=str)}]}



# @tool(
#     name="calculate_total_tool",
#     description=(
#         "Calculates the total (sum) of a specified numeric column, usually 'revenue'. It is used when the user "
#         "asks for totals, sums, or aggregate revenue values. The column name must exist and contain numeric data; "
#         "non-numeric cells are ignored. The tool returns the total, the number of rows analyzed, and the count "
#         "of non-null entries. It should not be used for ranking or filtering tasks."
#     ),
#     input_schema={"column": str}
# )
# async def calculate_total_tool(args: Dict[str, Any]) -> ToolResult:
#     print("DEBUG: calculate_total_tool called with", args)
#     try:
#         df = get_df()
#     except Exception as e:
#         return {
#             "content": [
#                 {"type": "text", "text": json.dumps({
#                     "ok": False,
#                     "status": "error",
#                     "error": f"Could not load dataset: {e}"
#                 })}
#             ]
#         }

#     column = args.get("column", "revenue")

#     if column not in df.columns:
#         return {
#             "content": [
#                 {"type": "text", "text": json.dumps({
#                     "ok": False,
#                     "status": "error",
#                     "error": f"Column '{column}' not found. Available: {', '.join(df.columns)}"
#                 })}
#             ]
#         }
    
#     try:
#         total = float(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())
#     except Exception as e:
#         return {
#             "content": [
#                 {"type": "text", "text": json.dumps({
#                     "ok": False,
#                     "status": "error",
#                     "error": f"Failed to compute total for column '{column}': {e}"
#                 })}
#             ]
#         }
    
#     result = {
#         "ok": True,
#         "status": "success",
#         "result": {
#             "intent": "aggregation",
#             "column": column,
#             "total": total
#         },
#         "metadata": {
#             "rows_analyzed": len(df),
#             "non_null_values": int(df[column].notna().sum())
#         }
#     }

#     return {
#         "content": [
#             {"type": "text", "text": json.dumps(result, indent=2)}
#         ]
#     }


# @tool(
#     name="get_top_n_tool",
#     description=(
#         "Retrieves the top-N rows from the dataset based on the values in a given numeric column, Input: {'column': str, 'n': int},"
#         "such as 'revenue'. It sorts the column in descending order and returns the N highest rows. "
#         "This tool is ideal for queries like 'top 5 products by revenue' or 'highest earning categories'. "
#         "If the column contains non-numeric or missing data, those rows are ignored or appear at the bottom. "
#         "It should not be used for totals or filtering by conditions."
#     ),
#     input_schema={"column": str, "n": int}
# )
# async def get_top_n_tool(args: Dict[str, Any]) -> ToolResult:
#     print("DEBUG: get_top_n_tool called with", args)
#     try:
#         df = get_df()
#     except Exception as e:
#         return {
#             "content": [
#                 {"type": "text", "text": json.dumps({
#                     "ok": False,
#                     "status": "error",
#                     "error": f"Could not load dataset: {e}"
#                 })}
#             ]
#         }

#     # --- Parse inputs ---
#     column = args.get("column", "revenue")
#     n = args.get("n", 5)
#     try:
#         n = int(n)
#     except Exception:
#         return {
#             "content": [
#                 {"type": "text", "text": json.dumps({
#                     "ok": False,
#                     "status": "error",
#                     "error": "'n' must be an integer."
#                 })}
#             ]
#         }

#     if column not in df.columns:
#         return {
#             "content": [
#                 {"type": "text", "text": json.dumps({
#                     "ok": False,
#                     "status": "error",
#                     "error": f"Column '{column}' not found. Available columns: {', '.join(df.columns)}"
#                 })}
#             ]
#         }
    
#     # --- Compute top N ---
#     try:
#         df_sorted = df.sort_values(by=column, ascending=False).head(n)
#         rows = df_sorted.to_dict(orient="records")
#         print(f"DEBUG: returning top {n} rows by '{column}'")
#     except Exception as e:
#         return {
#             "content": [
#                 {"type": "text", "text": json.dumps({
#                     "ok": False,
#                     "status": "error",
#                     "error": f"Failed to compute top {n} rows for '{column}': {e}"
#                 })}
#             ]
#         }

#     # --- Prepare clean JSON result ---
#     result = {
#         "ok": True,
#         "status": "success",
#         "result": {
#             "intent": "top_n",
#             "column": column,
#             "n": n,
#             "rows": rows
#         },
#         "metadata": {
#             "rows_analyzed": len(df),
#             "non_null_values": int(df[column].notna().sum())
#         }
#     }

#     # --- Return in Claude-SDK-standard format ---
#     return {
#         "content": [
#             {"type": "text", "text": json.dumps(result, indent=2)}
#         ]
#     }



# @tool(
#     name="filter_by_value_tool",
#     description=(
#         "Filters rows of the dataset where a specified column matches a target value, Input: {'column': str, 'value': str},"
#         "performing a case-insensitive comparison. It is useful when the user asks questions "
#         "like 'show data for Mango products' or 'get revenue for a specific region'. "
#         "The tool also computes the total revenue for the filtered subset if a 'revenue' column exists. "
#         "It will not perform pattern matching or numeric comparisons beyond equality."
#     ),
#     input_schema={"column": str, "value": str}
# )
# async def filter_by_value_tool(args: Dict[str, Any]) -> ToolResult:
#     print("DEBUG: filter_by_value_tool called with", args)

#     try:
#         df = get_df()
#     except Exception as e:
#         err = {"ok": False, "status": "error", "error": f"Could not load dataset: {e}"}
#         return {"content": [{"type": "text", "text": json.dumps(err)}]}

#     column = args.get("column","products")
#     value = args.get("value")
    
#     print("DEBUG: requested column:", column, "value:", value)


#     if not column or value is None:
#         err = {"ok": False, "status": "error", "error": "Both 'column' and 'value' are required."}
#         return {"content": [{"type": "text", "text": json.dumps(err)}]}
    
#     # common auto-corrections for singular/plural
#     col_candidates = set(df.columns)
#     if column not in col_candidates:
#         # try plural/singular swap
#         if column.endswith("s") and column[:-1] in col_candidates:
#             print(f"DEBUG: auto-correcting column '{column}' -> '{column[:-1]}'")
#             column = column[:-1]
#         elif (column + "s") in col_candidates:
#             print(f"DEBUG: auto-correcting column '{column}' -> '{column}s'")
#             column = column + "s"
    
#     if column not in df.columns:
#         err = {"ok": False, "status": "error", "error": f"Column '{column}' not found. Available: {', '.join(df.columns)}"}
#         return {"content": [{"type": "text", "text": json.dumps(err)}]}

#     # Perform filtering (case-insensitive string compare)
#     try:
#         filtered = df[df[column].astype(str).str.lower() == str(value).lower()]
        
#         # Convert NaN -> None and produce list-of-dicts (JSON-friendly)
#         safe_rows = filtered.where(pd.notnull(filtered), None).to_dict(orient="records")
#         total = float(pd.to_numeric(filtered.get("revenue", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if "revenue" in filtered.columns and len(filtered) > 0 else 0.0

#         result = {
#             "ok": True,
#             "status": "success",
#             "result": {
#                 "intent": "filter",
#                 "column": column,
#                 "value": value,
#                 "count": len(safe_rows),
#                 "total": total,
#                 "rows": safe_rows  # at most however many matched; caller can truncate if needed
#             },
#             "metadata": {
#                 "rows_analyzed": len(df),
#                 "non_null_values": int(df[column].notna().sum())
#             }
#         }

#         return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}

#     except Exception as e:
#         err = {"ok": False, "status": "error", "error": f"Processing/filter error: {e}"}
#         return {"content": [{"type": "text", "text": json.dumps(err)}]}
    

# tools.py
"""
Core tools:
- validate_data_tool
- calculate_total_tool
- get_top_n_tool
- filter_by_value_tool

Each tool returns JSON-serializable dicts and avoids raising unhandled exceptions.
"""

from typing import Any, Dict, Optional, List
import os
import json
import logging
from logging.handlers import RotatingFileHandler

import pandas as pd

# SDK helpers
from claude_agent_sdk import tool

ToolResult = Dict[str, Any]

# --- Logging: detailed file logs, minimal console noise ---
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOGFILE = os.path.join(LOG_DIR, "tools.log")

logger = logging.getLogger("agents.tools")
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    fh = RotatingFileHandler(LOGFILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s %(message)s"))

    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = False

# Config / constants
DATA_PATH = os.environ.get("DATA_PATH", "data/sample_data.csv")

# lazy-loaded dataframe reference. Set to None to always reload; set caching if desired.
_df: Optional[pd.DataFrame] = None
# To enable caching across calls, uncomment caching logic in get_df().

def get_df() -> pd.DataFrame:
    """
    Lazy-load (or reload) and return the dataset as a pandas.DataFrame.
    Raises FileNotFoundError or pd.errors.EmptyDataError on failure.

    Note: currently the function reloads the CSV on each call to reflect changes on disk.
    To enable caching, uncomment the _df check at the top of this function.
    """
    global _df
    # Uncomment the following lines to enable simple in-memory caching:
    # if _df is not None:
    #     return _df

    abs_path = os.path.abspath(DATA_PATH)
    logger.debug("get_df: using DATA_PATH=%s abs=%s", DATA_PATH, abs_path)

    if not os.path.exists(abs_path):
        logger.error("Data file not found at %s", abs_path)
        raise FileNotFoundError(f"Data file not found at {abs_path}")

    try:
        df = pd.read_csv(abs_path)
    except pd.errors.EmptyDataError as e:
        logger.error("CSV at %s is empty or invalid: %s", abs_path, e)
        raise
    except pd.errors.ParserError as e:
        logger.error("Failed to parse CSV at %s: %s", abs_path, e)
        raise
    except OSError as e:
        logger.exception("OS error reading CSV at %s: %s", abs_path, e)
        raise

    # Normalize column names to lowercase for downstream code expectations
    df.columns = df.columns.str.lower()

    # Cache assignment (optional)
    _df = df
    return df

# -----------------------
# Tool Implementations
# -----------------------

@tool(
    name="validate_data_tool",
    description=(
        "Performs a thorough validation of the loaded dataset. It checks for missing or empty values "
        "in any column, identifies rows with negative revenue values, and verifies that key columns like "
        "'products' and 'revenue' exist. It should be used as the first step before any aggregation or "
        "filtering to confirm the data is clean and ready for analysis. The tool returns structured "
        "JSON describing missing columns, number of rows, empty cells per column (with examples), "
        "and rows with negative revenue. It should not be used to compute totals or filter data."
    ),
    input_schema={}
)
async def validate_data_tool(args: Dict[str, Any]) -> ToolResult:
    MAX_EXAMPLES = 5
    try:
        df = get_df()
        logger.debug("validate_data_tool: loaded dataframe with %d rows, columns=%s", len(df), df.columns.tolist())
    except FileNotFoundError as e:
        payload = {"ok": False, "status": "error", "error": str(e)}
        logger.warning("validate_data_tool: dataset not found: %s", e)
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        payload = {"ok": False, "status": "error", "error": f"Failed to read dataset: {e}"}
        logger.warning("validate_data_tool: csv read error: %s", e)
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}
    except Exception as e:
        payload = {"ok": False, "status": "error", "error": f"Unexpected: {e}"}
        logger.exception("validate_data_tool: unexpected error: %s", e)
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

    required = {"products", "revenue"}
    missing = required - set(df.columns)

    if missing:
        payload = {
            "ok": False,
            "status": "insufficient",
            "error": f"Missing columns: {', '.join(sorted(missing))}",
            "columns": df.columns.tolist(),
            "rows": len(df),
        }
        logger.info("validate_data_tool: missing required columns: %s", missing)
        return {"content": [{"type": "text", "text": json.dumps(payload, default=str)}]}

    if len(df) == 0:
        payload = {"ok": False, "status": "insufficient", "error": "No rows in dataset."}
        logger.info("validate_data_tool: dataset has no rows")
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

    # Detect empty / blank strings in any column
    try:
        empty_mask = df.isnull() | df.astype(str).apply(lambda col: col.str.strip() == "")
    except Exception as e:
        # Defensive: if conversion to str fails for some exotic dtype
        logger.exception("validate_data_tool: failed to compute empty_mask: %s", e)
        payload = {"ok": False, "status": "error", "error": f"Failed validation: {e}"}
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

    missing_summary: Dict[str, Any] = {}
    for col in df.columns:
        col_mask = empty_mask[col]
        count = int(col_mask.sum())
        if count > 0:
            examples: List[Dict[str, Any]] = []
            # Use .loc to keep original index; convert index to int when possible
            sample = df.loc[col_mask, [col, "products"]].head(MAX_EXAMPLES)
            for i, val in sample.iterrows():
                try:
                    idx = int(i) if isinstance(i, (int,)) or (str(i).isdigit()) else str(i)
                except Exception:
                    idx = str(i)
                prod_val = (val.get("products") if "products" in df.columns else None)
                examples.append({
                    "index": idx,
                    "products": prod_val,
                    "value": None if pd.isna(val[col]) else str(val[col])
                })
            missing_summary[col] = {"count": count, "examples": examples}

    # Negative revenue detection
    negative_revenue: List[Dict[str, Any]] = []
    if "revenue" in df.columns:
        try:
            rev_numeric = pd.to_numeric(df["revenue"], errors="coerce")
            neg_mask = rev_numeric < 0
            neg_count = int(neg_mask.sum())
            if neg_count > 0:
                for i, row in df.loc[neg_mask, ["products", "revenue"]].head(MAX_EXAMPLES).iterrows():
                    try:
                        rev_val = float(row["revenue"])
                    except Exception:
                        rev_val = None
                    try:
                        idx = int(i) if isinstance(i, (int,)) or (str(i).isdigit()) else str(i)
                    except Exception:
                        idx = str(i)
                    negative_revenue.append({
                        "index": idx,
                        "products": row.get("products"),
                        "revenue": rev_val
                    })
        except Exception as e:
            logger.exception("validate_data_tool: error detecting negative revenue: %s", e)

    issues = []
    if missing_summary:
        issues.append(f"Missing values present in columns: {', '.join(missing_summary.keys())}")
    if negative_revenue:
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

    logger.debug("validate_data_tool: payload prepared: ok=%s issues=%s", ok, issues)
    return {"content": [{"type": "text", "text": json.dumps(payload, indent=2, default=str)}]}


@tool(
    name="calculate_total_tool",
    description=(
        "Calculates the total (sum) of a specified numeric column, usually 'revenue'. It is used when the user "
        "asks for totals, sums, or aggregate revenue values. The column name must exist and contain numeric data; "
        "non-numeric cells are ignored. The tool returns the total, the number of rows analyzed, and the count "
        "of non-null entries. It should not be used for ranking or filtering tasks."
    ),
    input_schema={"column": str}
)
async def calculate_total_tool(args: Dict[str, Any]) -> ToolResult:
    logger.debug("calculate_total_tool called with args=%s", args)
    try:
        df = get_df()
    except FileNotFoundError as e:
        payload = {"ok": False, "status": "error", "error": str(e)}
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        payload = {"ok": False, "status": "error", "error": f"Failed to read dataset: {e}"}
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}
    except Exception as e:
        logger.exception("calculate_total_tool: unexpected error loading df: %s", e)
        payload = {"ok": False, "status": "error", "error": f"Unexpected: {e}"}
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

    column = args.get("column", "revenue")

    if column not in df.columns:
        payload = {"ok": False, "status": "error", "error": f"Column '{column}' not found. Available: {', '.join(df.columns)}"}
        logger.info("calculate_total_tool: requested missing column '%s'", column)
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

    try:
        total = float(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())
    except Exception as e:
        logger.exception("calculate_total_tool: failed to compute total for %s: %s", column, e)
        payload = {"ok": False, "status": "error", "error": f"Failed to compute total for column '{column}': {e}"}
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

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

    logger.debug("calculate_total_tool: returning total=%s for column=%s", total, column)
    return {"content": [{"type": "text", "text": json.dumps(result, indent=2, default=str)}]}


@tool(
    name="get_top_n_tool",
    description=(
        "Retrieves the top-N rows from the dataset based on the values in a given numeric column, Input: {'column': str, 'n': int},"
        "such as 'revenue'. It sorts the column in descending order and returns the N highest rows. "
        "This tool is ideal for queries like 'top 5 products by revenue' or 'highest earning categories'. "
        "If the column contains non-numeric or missing data, those rows are ignored or appear at the bottom. "
        "It should not be used for totals or filtering by conditions."
    ),
    input_schema={"column": str, "n": int}
)
async def get_top_n_tool(args: Dict[str, Any]) -> ToolResult:
    logger.debug("get_top_n_tool called with args=%s", args)
    try:
        df = get_df()
    except FileNotFoundError as e:
        payload = {"ok": False, "status": "error", "error": str(e)}
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        payload = {"ok": False, "status": "error", "error": f"Failed to read dataset: {e}"}
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}
    except Exception as e:
        logger.exception("get_top_n_tool: unexpected error loading df: %s", e)
        payload = {"ok": False, "status": "error", "error": f"Unexpected: {e}"}
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

    column = args.get("column", "revenue")
    n = args.get("n", 5)
    try:
        n = int(n)
    except (ValueError, TypeError):
        payload = {"ok": False, "status": "error", "error": "'n' must be an integer."}
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

    if column not in df.columns:
        payload = {"ok": False, "status": "error", "error": f"Column '{column}' not found. Available columns: {', '.join(df.columns)}"}
        logger.info("get_top_n_tool: missing column request '%s'", column)
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

    try:
        # Coerce to numeric for sorting if appropriate; non-numeric will become NaN and sort last.
        df_sorted = df.sort_values(by=column, ascending=False).head(n)
        rows = df_sorted.where(pd.notnull(df_sorted), None).to_dict(orient="records")
        logger.debug("get_top_n_tool: returning top %d rows by '%s'", n, column)
    except Exception as e:
        logger.exception("get_top_n_tool: failed to compute top %d rows for '%s': %s", n, column, e)
        payload = {"ok": False, "status": "error", "error": f"Failed to compute top {n} rows for '{column}': {e}"}
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

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

    return {"content": [{"type": "text", "text": json.dumps(result, indent=2, default=str)}]}


@tool(
    name="filter_by_value_tool",
    description=(
        "Filters rows of the dataset where a specified column matches a target value, Input: {'column': str, 'value': str},"
        "performing a case-insensitive comparison. It is useful when the user asks questions "
        "like 'show data for Mango products' or 'get revenue for a specific region'. "
        "The tool also computes the total revenue for the filtered subset if a 'revenue' column exists. "
        "It will not perform pattern matching or numeric comparisons beyond equality."
    ),
    input_schema={"column": str, "value": str}
)
async def filter_by_value_tool(args: Dict[str, Any]) -> ToolResult:
    logger.debug("filter_by_value_tool called with args=%s", args)
    try:
        df = get_df()
    except FileNotFoundError as e:
        err = {"ok": False, "status": "error", "error": str(e)}
        return {"content": [{"type": "text", "text": json.dumps(err)}]}
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        err = {"ok": False, "status": "error", "error": f"Failed to read dataset: {e}"}
        return {"content": [{"type": "text", "text": json.dumps(err)}]}
    except Exception as e:
        logger.exception("filter_by_value_tool: unexpected error loading df: %s", e)
        err = {"ok": False, "status": "error", "error": f"Unexpected: {e}"}
        return {"content": [{"type": "text", "text": json.dumps(err)}]}

    column = args.get("column", "products")
    value = args.get("value")
    logger.debug("filter_by_value_tool: requested column=%s value=%s", column, value)

    if not column or value is None:
        err = {"ok": False, "status": "error", "error": "Both 'column' and 'value' are required."}
        logger.info("filter_by_value_tool: missing column or value")
        return {"content": [{"type": "text", "text": json.dumps(err)}]}

    # common auto-corrections for singular/plural
    col_candidates = set(df.columns)
    if column not in col_candidates:
        if column.endswith("s") and column[:-1] in col_candidates:
            logger.debug("auto-correcting column '%s' -> '%s'", column, column[:-1])
            column = column[:-1]
        elif (column + "s") in col_candidates:
            logger.debug("auto-correcting column '%s' -> '%s'", column, column + "s")
            column = column + "s"

    if column not in df.columns:
        err = {"ok": False, "status": "error", "error": f"Column '{column}' not found. Available: {', '.join(df.columns)}"}
        logger.info("filter_by_value_tool: column not found after autocorrect '%s'", column)
        return {"content": [{"type": "text", "text": json.dumps(err)}]}

    # Perform filtering (case-insensitive string compare)
    try:
        filtered = df[df[column].astype(str).str.lower() == str(value).lower()]

        # Convert NaN -> None and produce list-of-dicts (JSON-friendly)
        safe_rows = filtered.where(pd.notnull(filtered), None).to_dict(orient="records")

        if "revenue" in filtered.columns and len(filtered) > 0:
            try:
                total = float(pd.to_numeric(filtered.get("revenue", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
            except Exception:
                logger.exception("filter_by_value_tool: failed to coerce revenue to numeric; defaulting total to 0")
                total = 0.0
        else:
            total = 0.0

        result = {
            "ok": True,
            "status": "success",
            "result": {
                "intent": "filter",
                "column": column,
                "value": value,
                "count": len(safe_rows),
                "total": total,
                "rows": safe_rows
            },
            "metadata": {
                "rows_analyzed": len(df),
                "non_null_values": int(df[column].notna().sum())
            }
        }

        logger.debug("filter_by_value_tool: matched %d rows total=%s", len(safe_rows), total)
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2, default=str)}]}

    except (KeyError, TypeError, ValueError) as e:
        logger.exception("filter_by_value_tool: processing error: %s", e)
        err = {"ok": False, "status": "error", "error": f"Processing/filter error: {e}"}
        return {"content": [{"type": "text", "text": json.dumps(err)}]}
    except Exception as e:
        logger.exception("filter_by_value_tool: unexpected error: %s", e)
        err = {"ok": False, "status": "error", "error": f"Unexpected processing error: {e}"}
        return {"content": [{"type": "text", "text": json.dumps(err)}]}
