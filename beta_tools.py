# beta_tools.py
import json
from typing import Any, Dict, Optional

# Beta tool decorator and client come from anthropic
from anthropic import beta_tool

# Import your existing tool implementations (these are async functions decorated with @tool in tools.py)
# We will call them directly (wrap them synchronously for the beta tool runner)
# If your functions require async invocation, you can run them via asyncio in the wrapper.
from tools import (
    validate_data_tool,
    calculate_total_tool,
    get_top_n_tool,
    filter_by_value_tool,
)

# Helper wrapper to call your async tools (they follow signature async def tool(args: Dict) -> ToolResult)
import asyncio

def run_async_tool(tool_callable, args: Dict[str, Any]) -> Any:
    """Run an async tool callable from synchronous beta_tool wrapper."""
    # Some tool callables may expect a dict argument even if empty
    coro = tool_callable(args or {})
    try:
        return asyncio.get_event_loop().run_until_complete(coro)
    except RuntimeError:
        # no running loop => create new
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        finally:
            loop.close()
            asyncio.set_event_loop(None)

# Each @beta_tool wrapper returns a JSON string. The tool runner will convert to a content block.
@beta_tool
def validate_data(args: Optional[Dict[str, Any]] = None) -> str:
    """Run validate_data_tool and return JSON string result."""
    try:
        result = run_async_tool(validate_data_tool, args or {})
        # The tools.py returns {"content":[{"type":"text","text": "<json>"}]}
        # Normalize: extract text if present and return as-is, otherwise dump result.
        if isinstance(result, dict) and "content" in result:
            for part in result["content"]:
                if isinstance(part, dict) and part.get("type") == "text":
                    return part.get("text", "")
        return json.dumps(result, default=str)
    except Exception as e:
        return json.dumps({"ok": False, "status": "error", "error": str(e)})

@beta_tool
def calculate_total(args: Dict[str, Any]) -> str:
    """Call calculate_total_tool. Expects {'column': 'revenue'}"""
    try:
        result = run_async_tool(calculate_total_tool, args or {})
        if isinstance(result, dict) and "content" in result:
            for part in result["content"]:
                if isinstance(part, dict) and part.get("type") == "text":
                    return part.get("text", "")
        return json.dumps(result, default=str)
    except Exception as e:
        return json.dumps({"ok": False, "status": "error", "error": str(e)})

@beta_tool
def get_top_n(args: Dict[str, Any]) -> str:
    """Call get_top_n_tool. Expects {'column': 'revenue', 'n': 5}"""
    try:
        result = run_async_tool(get_top_n_tool, args or {})
        if isinstance(result, dict) and "content" in result:
            for part in result["content"]:
                if isinstance(part, dict) and part.get("type") == "text":
                    return part.get("text", "")
        return json.dumps(result, default=str)
    except Exception as e:
        return json.dumps({"ok": False, "status": "error", "error": str(e)})

@beta_tool
def filter_by_value(args: Dict[str, Any]) -> str:
    """Call filter_by_value_tool. Expects {'column':'products','value':'Onions'}"""
    try:
        result = run_async_tool(filter_by_value_tool, args or {})
        if isinstance(result, dict) and "content" in result:
            for part in result["content"]:
                if isinstance(part, dict) and part.get("type") == "text":
                    return part.get("text", "")
        return json.dumps(result, default=str)
    except Exception as e:
        return json.dumps({"ok": False, "status": "error", "error": str(e)})
