# agents/one_shot.py
"""
One-shot runner for the Data Analysis Agent.

Behavior:
- Reads a prompt from sys.argv, runs the agent in "one-shot" mode (limited turns),
  streams responses, extracts tool outputs, and saves a timestamped + latest state
  file under state/one-shot/.
- Uses an in-process MCP server for local tools.
- Uses structured logging (debug -> file; console shows warnings+).
"""

import tempfile
import shutil
import pathlib
import datetime
import os
import sys
import json
import asyncio
import logging
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List, Optional

# Claude SDK imports
from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    create_sdk_mcp_server,
    AssistantMessage,
    ToolUseBlock,
    ToolResultBlock,
    TextBlock,
)

# SDK-specific error class (if available)
try:
    from claude_agent_sdk import ClaudeSDKError  # type: ignore
except Exception:
    ClaudeSDKError = Exception

# import your tools
from tools import (
    validate_data_tool,
    calculate_total_tool,
    get_top_n_tool,
    filter_by_value_tool,
)

# ---------------------------
# Logging configuration
# ---------------------------
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logfile = os.path.join(LOG_DIR, "one_shot.log")

logger = logging.getLogger("agents.one_shot")
logger.setLevel(logging.DEBUG)

# File handler: debug -> file
from logging.handlers import RotatingFileHandler
fh = RotatingFileHandler(logfile, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))

# Console handler: warnings and above
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

if not logger.handlers:
    logger.addHandler(fh)
    logger.addHandler(ch)

# ---------------------------
# Persistent state helpers
# ---------------------------
DEFAULT_STATE_PATH = os.environ.get("AGENT_STATE_PATH", "state/one-shot/agent_state_latest.json")

@dataclass
class AgentState:
    query: str
    intent: Optional[str] = None
    results: Optional[Dict[str, Any]] = None
    insights: Optional[str] = None
    data_issues: List[Any] = field(default_factory=list)
    timestamp: Optional[str] = None

    def __post_init__(self):
        if self.data_issues is None:
            self.data_issues = []

    def to_dict(self):
        d = asdict(self)
        d["data_issues"] = d.get("data_issues") or []
        return d

def save_state(state: AgentState, path: str = DEFAULT_STATE_PATH) -> None:
    """
    Atomically write `state` to `path`. If parent dir does not exist it will be created.
    """
    # set timestamp (UTC ISO)
    try:
        state.timestamp = datetime.datetime.utcnow().isoformat() + "Z"
    except Exception:
        state.timestamp = None

    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    # Write to a temp file in same dir then move (atomic on most OSes)
    fd = None
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(dir=str(p.parent))
        with os.fdopen(fd, "w", encoding="utf-8") as tf:
            json.dump(state.to_dict(), tf, indent=2, default=str)
            tf.flush()
            os.fsync(tf.fileno())
        shutil.move(tmp_path, str(p))
        logger.debug("Saved state atomically to %s", path)
    except (OSError, IOError) as e:
        logger.warning("Atomic save_state failed: %s; falling back to simple write.", e, exc_info=True)
        try:
            with open(str(p), "w", encoding="utf-8") as f:
                json.dump(state.to_dict(), f, indent=2, default=str)
            logger.debug("Saved state with fallback write to %s", path)
        except Exception:
            logger.exception("Fallback save_state also failed; state not persisted")
    finally:
        # ensure no stray temp file
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            logger.debug("Failed to remove tmp save file: %s", tmp_path, exc_info=True)

# ---------------------------
# Agent config / prompt
# ---------------------------
ALLOWED_TOOLS = [
    "mcp__dataAnalysis__validate_data_tool",
    "mcp__dataAnalysis__calculate_total_tool",
    "mcp__dataAnalysis__get_top_n_tool",
    "mcp__dataAnalysis__filter_by_value_tool",
]

SYSTEM_PROMPT = """
ENVIRONMENT & DEFAULTS
- The dataset path is provided to the Python runtime via the DATA_PATH environment variable.
- The dataset schema (read-only) is:
    - products : string  (product name)
    - revenue  : numeric (sales amount)
- Column names in the dataset are normalized to lower-case by the code (so use 'products' and 'revenue').
- If the user asks for "total sales" and does not specify a column, assume column = "revenue".
- If the user asks for "top N" items and does not specify N, assume N = 5.

AVAILABLE TOOLS (calls to tools must use the MCP names)
- mcp__dataAnalysis__validate_data_tool
  Input: none
  Behavior: validate dataset exists; return columns, row count, and any issues (missing cols, nulls, negatives).
  Use when you need to discover columns or confirm data quality.

- mcp__dataAnalysis__calculate_total_tool
  Input: {"column": "<column_name>"}
  Behavior: return {"column": "<column_name>", "total": <number>}.

- mcp__dataAnalysis__get_top_n_tool
  Input: {"column": "<column_name>", "n": <int>}
  Behavior: return the top-n rows sorted by the numeric column (returns up to n rows).

- mcp__dataAnalysis__filter_by_value_tool
  Input: {"column": "<column_name>", "value": "<value>"}
  Behavior: return rows matching value and the sum of the revenue for those rows.

RULES (tool use, ambiguity, and failures)
1. DATA VALIDATION RULE (MANDATORY)
    - BEFORE answering any user query that requires dataset content, you MUST call mcp__dataAnalysis__validate_data_tool.
    - The validate_data_tool must return a JSON object (inside a single content text block) containing at minimum:
        {"ok": <bool>, "status": "<valid|insufficient|error>", "columns": [...], "rows": <int>, "issues": [...]}
    - Interpretation:
        * If validate_data_tool returns {"ok": true, "status": "valid"} then proceed to call the appropriate analysis tool (calculate_total_tool, get_top_n_tool, or filter_by_value_tool).
        * If validate_data_tool returns ok:false OR status in ["insufficient","error"] OR issues is non-empty (nulls, missing cols, negative values), STOP: do not call any further tools. Immediately return a structured response with intent="error", include the validate tool payload in data_issues, and one short recommendation for remediation.
    - Always prefer calling validate_data_tool even if the user previously validated the dataset, because the dataset may have changed between requests.

2. If validate_data_tool returns {"ok": true, "status": "valid"}, for any query that requires numbers or data from the dataset you MUST call the appropriate tool.
   - Aggregation => call calculate_total_tool.
   - Top-N      => call get_top_n_tool.
   - Filtering  => call filter_by_value_tool.
3. If you do not know which column to use, first call validate_data_tool to learn column names.
4. If a tool returns an error (dataset missing, column missing, file unreadable), include that error in the structured output's "data_issues" list.
5. Do not call any tool unless the query requires it.

OUTPUT FORMAT (human + machine)
After you run tools and compute results, produce **two parts** in this exact order:

A) Short human-readable summary (1-2 sentences) and an optional small table (up to 5 rows) or numbers for quick reading.

B) A JSON object **inside a single fenced code block** (only JSON in that block) matching the schema below. The JSON is the definitive machine-readable result.

JSON SCHEMA (required keys)
{
  "intent": "<aggregation|top_n|filter|ambiguous|error>",
  "supporting_data": { ... },     // numbers or up to 5 rows, small table representation
  "recommendation": "<one actionable sentence>",
  "reasoning": "<short explanation of how you computed/selected this>",
  "data_issues": [ "<issue string>", ... ],
  "clarifying_question": "<string>"  // only present when intent == "ambiguous"
}

OUTPUT RULES (strict)
- "supporting_data" must contain either a numeric summary (e.g. {"column":"revenue","total":12345}) or {"rows":[{...},...]} with at most 5 rows.
- "recommendation" must be exactly one actionable sentence (no lists).
- "reasoning" must be short (1-2 sentences) explaining which tool was called and why.
- If the tool returned an error, set intent="error" and include the tool error text in data_issues.

EXAMPLE QUERIES & EXPECTED TOOL USAGE
- Aggregation: "What's the total revenue?"
  -> call calculate_total_tool({"column":"revenue"}), then return total in supporting_data.

- Top N: "Show top 5 products by revenue"
  -> call get_top_n_tool({"column":"revenue","n":5}), then return rows (up to 5) in supporting_data.

- Filtering: "What's revenue for Product A?" or "Filter products by onions"
  -> call filter_by_value_tool({"column":"products","value":"product a"})

EXAMPLES (final JSON-only block shown)
Aggregation example:
Human summary: Total revenue for the dataset is 45,234.75.
```json
{
  "intent": "aggregation",
  "supporting_data": {"column": "revenue", "total": 45234.75},
  "recommendation": "Increase promotion on the top-selling product to grow revenue by focusing on repeat buyers.",
  "reasoning": "Used calculate_total_tool on 'revenue' to sum all sales.",
  "data_issues": []
}
"""


# ---------------------------
# Helpers
# ---------------------------
def safe_parse_tool_text(txt: str) -> Optional[dict]:
    """
    Try to parse the given tool text as JSON. Return dict or None on failure.
    Keep parse errors narrow so we don't swallow unexpected issues.
    """
    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        return None
    except Exception:
        logger.debug("Unexpected error while parsing tool text", exc_info=True)
        return None

# ---------------------------
# One-shot runner
# ---------------------------
async def main() -> None:
    """
    One-shot runner: read prompt from sys.argv, run the assistant for up to max_turns,
    capture tool outputs and assistant text, save state to disk.
    """
    if len(sys.argv) < 2:
        print('Usage: python main.py "What\'s the total revenue?"')
        return

    prompt = " ".join(sys.argv[1:]).strip()
    state = AgentState(query=prompt)

    # Create in-process MCP server (tools are python callables)
    dataAnalysis = create_sdk_mcp_server(
        name="dataAnalysis",
        version="1.0.0",
        tools=[validate_data_tool, calculate_total_tool, get_top_n_tool, filter_by_value_tool],
    )

    options = ClaudeAgentOptions(
        system_prompt=SYSTEM_PROMPT,
        allowed_tools=ALLOWED_TOOLS,
        mcp_servers={"dataAnalysis": dataAnalysis},
        max_turns=3,
    )

    client = ClaudeSDKClient(options=options)

    # Connect to client
    try:
        await client.connect()
    except ClaudeSDKError as e:
        print("Failed to connect ClaudeSDKClient (SDK error):", e, flush=True)
        logger.exception("Failed to connect ClaudeSDKClient (SDK error): %s", e)
        return
    except (OSError, IOError) as e:
        print("Failed to connect ClaudeSDKClient (OS error):", e, flush=True)
        logger.exception("Failed to connect ClaudeSDKClient (OS): %s", e)
        return
    except Exception as e:
        logger.exception("Unexpected error during client.connect(): %s", e)
        print("Failed to connect ClaudeSDKClient (unexpected error). See logs.", flush=True)
        return

    # Send the prompt (one-shot)
    try:
        await client.query(prompt)
    except (ClaudeSDKError, OSError) as e:
        print("Failed to send prompt:", e, flush=True)
        logger.exception("Failed to send prompt: %s", e)
        try:
            await client.disconnect()
        except Exception:
            logger.debug("Failed to disconnect after query failure", exc_info=True)
        return
    except Exception as e:
        logger.exception("Unexpected error while sending prompt: %s", e)
        try:
            await client.disconnect()
        except Exception:
            pass
        print("Failed to send prompt (unexpected). See logs.", flush=True)
        return

    final_text_parts: List[str] = []

    # Stream responses and populate AgentState
    try:
        async for message in client.receive_response():
            # only handle assistant messages
            if isinstance(message, AssistantMessage):
                for block in getattr(message, "content", []):
                    # Tool use block — model requested a tool call
                    if isinstance(block, ToolUseBlock):
                        tool_name = getattr(block, "name", getattr(block, "id", "<unknown>"))
                        print(f"[Tool use] {tool_name}", flush=True)
                        if getattr(block, "input", None):
                            try:
                                print("  input:", json.dumps(block.input, ensure_ascii=False), flush=True)
                            except (TypeError, ValueError):
                                print("  input (raw):", repr(block.input)[:1000], flush=True)
                            except Exception:
                                logger.debug("Unexpected error printing tool input", exc_info=True)

                    # Tool result block — extract JSON/text from tool
                    elif isinstance(block, ToolResultBlock):
                        content = getattr(block, "content", None)
                        # print a short preview for CLI feedback
                        try:
                            preview = (json.dumps(content, ensure_ascii=False)[:1000]
                                       if isinstance(content, (dict, list)) else str(content)[:1000])
                            print("[Tool result] preview:", preview, flush=True)
                        except Exception:
                            print("[Tool result] (unprintable content)", flush=True)
                            logger.debug("Unable to preview tool result content", exc_info=True)

                        # If content is a list of blocks, try to extract a text block
                        parsed_result = None
                        if isinstance(content, list):
                            for part in content:
                                if isinstance(part, dict) and part.get("type") == "text":
                                    txt = (part.get("text") or "").strip()
                                    parsed_result = safe_parse_tool_text(txt) or {"raw_text": txt}
                                    break
                        elif isinstance(content, str):
                            parsed_result = safe_parse_tool_text(content) or {"raw_text": content}
                        elif isinstance(content, dict):
                            parsed_result = content
                        else:
                            parsed_result = {"repr": repr(content)}

                        # Inspect parsed_result shape and update state accordingly
                        try:
                            if isinstance(parsed_result, dict):
                                # validate tool shape
                                if parsed_result.get("ok") is False or parsed_result.get("status") in ("insufficient", "error"):
                                    state.data_issues.append(parsed_result)
                                else:
                                    if "result" in parsed_result:
                                        state.results = parsed_result["result"]
                                    elif "total" in parsed_result:
                                        state.results = {"column": parsed_result.get("column"), "total": parsed_result.get("total")}
                                    else:
                                        state.results = parsed_result
                            else:
                                # store fallback
                                state.results = {"tool_result": parsed_result}
                        except Exception:
                            logger.debug("Error processing parsed tool result", exc_info=True)

                    # Plain assistant text block
                    elif isinstance(block, TextBlock):
                        txt = getattr(block, "text", "").strip()
                        if txt:
                            print("Claude:", txt, flush=True)
                            final_text_parts.append(txt)

    except ClaudeSDKError as e:
        logger.exception("SDK error while receiving responses: %s", e)
        print("Error while reading responses (SDK). See logs.", flush=True)
    except Exception as e:
        logger.exception("Unexpected error while receiving responses: %s", e)
        print("Error while reading responses (unexpected). See logs.", flush=True)
    finally:
        try:
            await client.disconnect()
        except Exception:
            logger.debug("Error while disconnecting client", exc_info=True)

    # Save assembled insight/reasoning into state.insights
    if final_text_parts:
        state.insights = "\n".join(final_text_parts)

    # --- derive a best-effort intent for quick reference ---
    try:
        if isinstance(state.results, dict):
            if "intent" in state.results:
                state.intent = state.results.get("intent")
            elif "total" in state.results:
                state.intent = "aggregation"
            elif "n" in state.results or ("rows" in state.results and isinstance(state.results.get("rows"), list)):
                state.intent = "top_n"
            elif "count" in state.results or ("value" in state.results):
                state.intent = "filter"
            else:
                state.intent = state.intent or "unknown"
    except Exception:
        logger.debug("Error deriving best-effort intent", exc_info=True)
        state.intent = state.intent or "unknown"

    # --- ensure state dir exists and build filename ---
    try:
        ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        outdir = pathlib.Path("state/one-shot")
        outdir.mkdir(parents=True, exist_ok=True)
        filename = outdir / f"agent_state_{ts}.json"

        save_state(state, path=str(filename))
        # print(f"[Saved state] to {filename}", flush=True)

        latest_path = outdir / "agent_state_latest.json"
        save_state(state, path=str(latest_path))
        print(f"[Saved state] to {latest_path}", flush=True)

    except Exception as e:
        logger.exception("Failed to save final agent state: %s", e)
        # fallback
        try:
            save_state(state)
            print(f"[Saved state] to {DEFAULT_STATE_PATH} (fallback)", flush=True)
        except Exception as e2:
            logger.exception("Fallback final save_state failed: %s / %s", e, e2)
            print("[Warning] Failed to save agent state.", flush=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.", flush=True)
    except Exception:
        logger.exception("Fatal error in one-shot runner")
        print("Fatal error; see logs.", flush=True)
