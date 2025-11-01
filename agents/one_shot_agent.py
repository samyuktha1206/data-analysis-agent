# # agents/one_shot.py

import tempfile
import shutil
import pathlib
import datetime
import os
import sys
import json
import asyncio
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List, Optional

from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    create_sdk_mcp_server,
    AssistantMessage,
    ToolUseBlock,
    ToolResultBlock,
    TextBlock,
)

# import your tools (tools.py must export these callables)
from tools import (
    validate_data_tool,
    calculate_total_tool,
    get_top_n_tool,
    filter_by_value_tool,
)

# ---------------------------
# Persistent state helpers
# ---------------------------

DEFAULT_STATE_PATH = DEFAULT_STATE_PATH = os.environ.get("AGENT_STATE_PATH", "state/one-shot/agent_state_latest.json")

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
def load_state(path: str = DEFAULT_STATE_PATH) -> Optional[AgentState]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        # allow older or partial payloads
        return AgentState(
            query=payload.get("query", ""),
            intent=payload.get("intent"),
            results=payload.get("results"),
            insights=payload.get("insights"),
            data_issues=payload.get("data_issues", []),
            timestamp=payload.get("timestamp"),
        )
    except Exception:
        return None
    
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

    # Write to a temp file in same dir then move (atomic on most OS)
    fd, tmp_path = tempfile.mkstemp(dir=str(p.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tf:
            json.dump(state.to_dict(), tf, indent=2, default=str)
        shutil.move(tmp_path, str(p))
    except Exception:
        # If anything goes wrong, try best-effort simple write (fallback)
        try:
            with open(str(p), "w", encoding="utf-8") as f:
                json.dump(state.to_dict(), f, indent=2, default=str)
        except Exception:
            # swallow — we don't want a state save failure to crash the run
            pass
    finally:
        # ensure no stray temp file
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
# def save_state(state: AgentState, path: str = DEFAULT_STATE_PATH) -> None:
#     try:
#         import datetime

#         state.timestamp = datetime.datetime.utcnow().isoformat() + "Z"
#     except Exception:
#         state.timestamp = None
#     with open(path, "w", encoding="utf-8") as f:
#         json.dump(state.to_dict(), f, indent=2, default=str)

        
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
# One-shot runner
# ---------------------------
async def main() -> None:
    """
    This main() is an async coroutine intended to be called from main.py's run_one_shot().
    It reads sys.argv to get the prompt (so you can run python main.py "What's the total revenue?").
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

    try:
        await client.connect()
    except Exception as e:
        print("Failed to connect ClaudeSDKClient:", e)
        return

    # Send the prompt (one-shot)
    try:
        await client.query(prompt)
    except Exception as e:
        print("Failed to send prompt:", e)
        await client.disconnect()
        return

    # Stream responses and populate AgentState
    final_text_parts: List[str] = []
    try:
        async for message in client.receive_response():
            # We only handle Assistant messages (tool use, tool results, text)
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    # Tool use block — the model decided to call a tool
                    if isinstance(block, ToolUseBlock):
                        tool_name = getattr(block, "name", getattr(block, "id", "<unknown>"))
                        print(f"[Tool use] {tool_name}", flush=True)
                        if getattr(block, "input", None):
                            try:
                                print("  input:", json.dumps(block.input, ensure_ascii=False), flush=True)
                            except Exception:
                                print("  input (raw):", repr(block.input)[:1000], flush=True)

                    # Tool result block — extract JSON/text from tool
                    elif isinstance(block, ToolResultBlock):
                        content = getattr(block, "content", None)
                        print("[Tool result] raw:", repr(content)[:1000], flush=True)

                        # Some tools return content as list of blocks; try find text blocks
                        if isinstance(content, list):
                            for part in content:
                                if isinstance(part, dict) and part.get("type") == "text":
                                    txt = (part.get("text") or "").strip()
                                    # try JSON parse (tools should return JSON text for structured payloads)
                                    try:
                                        parsed = json.loads(txt)
                                        # If validate tool returned errors, append to data_issues
                                        if parsed.get("ok") is False or parsed.get("status") in ("insufficient", "error"):
                                            state.data_issues.append(parsed)
                                        else:
                                            # heuristics: if tool returned a result/total store it
                                            if "result" in parsed:
                                                state.results = parsed["result"]
                                            elif "total" in parsed:
                                                state.results = {"column": parsed.get("column"), "total": parsed.get("total")}
                                            else:
                                                # keep raw parsed
                                                state.results = parsed
                                    except Exception:
                                        # not JSON — store raw text
                                        state.results = {"raw_text": txt}
                        else:
                            # fallback
                            state.results = {"tool_content_repr": repr(content)}

                    # Plain assistant text block (reasoning, recommendation)
                    elif isinstance(block, TextBlock):
                        txt = getattr(block, "text", "").strip()
                        if txt:
                            print("Claude:", txt, flush=True)
                            final_text_parts.append(txt)

    except Exception as e:
        print("Error while reading responses:", e)
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

        # Save assembled insight/reasoning into state.insights
    if final_text_parts:
        state.insights = "\n".join(final_text_parts)

    # --- derive a best-effort intent for quick reference ---
    try:
        if isinstance(state.results, dict):
            # many of your tools put an "intent" or "result" key; detect common shapes
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
        # don't fail the run for minor parsing errors
        state.intent = state.intent or "unknown"

    # --- ensure state/one-shot dir exists and build filename ---
    try:
        import datetime, pathlib
        ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        # outdir = pathlib.Path("state/one-shot")
        # outdir.mkdir(parents=True, exist_ok=True)
        # filename = outdir / f"agent_state_{ts}.json"
        # save_state(state, path=str(filename))   # uses your existing save_state helper
        # print(f"[Saved state] to {filename}", flush=True)

        outdir = pathlib.Path("state/one-shot")
        outdir.mkdir(parents=True, exist_ok=True)
        filename = outdir / f"agent_state_{ts}.json"

        # save timestamped file (atomic)
        save_state(state, path=str(filename))
        print(f"[Saved state] to {filename}", flush=True)

        # also save a 'latest' copy for convenience (atomic)
        latest_path = outdir / "agent_state_latest.json"
        save_state(state, path=str(latest_path))
        print(f"[Saved state] to {latest_path}", flush=True)

    except Exception as e:
        # fallback: save to DEFAULT_STATE_PATH if timestamped save fails
        try:
            save_state(state)
            print(f"[Saved state] to {DEFAULT_STATE_PATH} (fallback)", flush=True)
        except Exception as e2:
            print(f"[Warning] Failed to save agent state: {e} / {e2}", flush=True)


# Allow running agents/one_shot.py directly for testing:
if __name__ == "__main__":
    asyncio.run(main())

