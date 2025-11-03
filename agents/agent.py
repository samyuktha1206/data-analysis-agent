# agent.py
"""
Conversation wrapper around Claude SDK client.

"""
import os
import pathlib
import json
import asyncio
from typing import Optional
from datetime import datetime

from claude_agent_sdk import (
    ClaudeSDKClient,
    ClaudeAgentOptions,
    AssistantMessage,
    UserMessage,
    TextBlock,
    ToolUseBlock,
    ToolResultBlock,
)

# Try to import SDK error base class if available; fall back to Exception
try:
    from claude_agent_sdk import ClaudeSDKError  
except Exception:
    ClaudeSDKError = Exception  # fallback; narrower catching where SDK provides errors still works

# Tools imported for MCP server registration
from tools import validate_data_tool, calculate_total_tool, get_top_n_tool, filter_by_value_tool

# Allowed MCP tool names (MCP identifiers)
ALLOWED_TOOLS = [
    "mcp__dataAnalysis__validate_data_tool",
    "mcp__dataAnalysis__calculate_total_tool",
    "mcp__dataAnalysis__get_top_n_tool",
    "mcp__dataAnalysis__filter_by_value_tool",
]

SYSTEM_PROMPT = """
# You are a Data Analysis Agent. You analyze structured datasets (CSV/JSON) using the tools provided.
# You MUST use the provided tools to read and compute answers; do not invent numbers.

# ENVIRONMENT & DEFAULTS
# - The dataset path is provided to the Python runtime via the DATA_PATH environment variable.
# - The dataset schema (read-only) is:
#     - products : string  (product name)
#     - revenue  : numeric (sales amount)
# - Column names in the dataset are normalized to lower-case by the code (so use 'products' and 'revenue').
# - If the user asks for "total sales" and does not specify a column, assume column = "revenue".

# AVAILABLE TOOLS (calls to tools must use the MCP names)
# - mcp__dataAnalysis__validate_data_tool
#   Input: none
#   Behavior: validate dataset exists; return columns, row count, and any issues (missing cols, nulls, negatives).
#   Use when you need to discover columns or confirm data quality.

# - mcp__dataAnalysis__calculate_total_tool
#   Input: {"column": "<column_name>"}
#   Behavior: return {"column": "<column_name>", "total": <number>}.

# - mcp__dataAnalysis__get_top_n_tool
#   Input: {"column": "<column_name>", "n": <int>}
#   Behavior: return the top-n rows sorted by the numeric column (returns up to n rows).

# - mcp__dataAnalysis__filter_by_value_tool
#   Input: {"column": "<column_name>", "value": "<value>"}
#   Behavior: return rows matching value and the sum of the revenue for those rows.


# RULES (tool use, ambiguity, and failures)
# 1. DATA VALIDATION RULE (MANDATORY)
#     - BEFORE answering any user query that requires dataset content, you MUST call mcp__dataAnalysis__validate_data_tool.
#     - The validate_data_tool must return a JSON object (inside a single content text block) containing at minimum:
#         {"ok": <bool>, "status": "<valid|insufficient|error>", "columns": [...], "rows": <int>, "issues": [...]}
#     - Interpretation:
#         * If validate_data_tool returns {"ok": true, "status": "valid"} then proceed to call the appropriate analysis tool (calculate_total_tool, get_top_n_tool, or filter_by_value_tool).
#         * If validate_data_tool returns ok:false OR status in ["insufficient","error"] OR issues is non-empty (nulls, missing cols, negative values), STOP: do not call any further tools. Immediately return a structured response with intent="error", include the validate tool payload in data_issues, and one short recommendation for remediation.
#     - Always prefer calling validate_data_tool even if the user previously validated the dataset, because the dataset may have changed between requests.

# 2. If validate_data_tool returns {"ok": true, "status": "valid"}, for any query that requires numbers or data from the dataset you MUST call the appropriate tool.
#    - Aggregation => call calculate_total_tool.
#    - Top-N      => call get_top_n_tool.
#    - Filtering  => call filter_by_value_tool.
# 3. If you do not know which column to use, first call validate_data_tool to learn column names.
# 4. If a tool returns an error (dataset missing, column missing, file unreadable), include that error in the structured output's "data_issues" list.
# 5. If the user's query is ambiguous, ask exactly one clarifying question (and do not call tools).
# 6. Do not call any tool unless the query requires it.

# OUTPUT FORMAT (human + machine)
# After you run tools and compute results, produce **two parts** in this exact order:

# A) Short human-readable summary (1-2 sentences) and an optional small table (up to 5 rows) or numbers for quick reading.

# B) A JSON object **inside a single fenced code block** (only JSON in that block) matching the schema below. The JSON is the definitive machine-readable result.

# JSON SCHEMA (required keys)
# {
#   "intent": "<aggregation|top_n|filter|ambiguous|error>",
#   "supporting_data": { ... },     // numbers or up to 5 rows, small table representation
#   "recommendation": "<one actionable sentence>",
#   "reasoning": "<short explanation of how you computed/selected this>",
#   "data_issues": [ "<issue string>", ... ],
#   "clarifying_question": "<string>"  // only present when intent == "ambiguous"
# }

# OUTPUT RULES (strict)
# - "supporting_data" must contain either a numeric summary (e.g. {"column":"revenue","total":12345}) or {"rows":[{...},...]} with at most 5 rows.
# - "recommendation" must be exactly one actionable sentence (no lists).
# - "reasoning" must be short (1-2 sentences) explaining which tool was called and why.
# - If the tool returned an error, set intent="error" and include the tool error text in data_issues.
# - If ambiguous, set intent="ambiguous" and include clarifying_question; do not call tools.

# EXAMPLE QUERIES & EXPECTED TOOL USAGE
# - Aggregation: "What's the total revenue?"
#   -> call calculate_total_tool({"column":"revenue"}), then return total in supporting_data.

# - Top N: "Show top 5 products by revenue"
#   -> call get_top_n_tool({"column":"revenue","n":5}), then return rows (up to 5) in supporting_data.

# - Filtering: "What's revenue for Product A?" or "Filter products by onions"
#   -> call filter_by_value_tool({"column":"products","value":"product a"})

# EXAMPLES (final JSON-only block shown)
# Aggregation example:
# Human summary: Total revenue for the dataset is 45,234.75.
# ```json
# {
#   "intent": "aggregation",
#   "supporting_data": {"column": "revenue", "total": 45234.75},
#   "recommendation": "Increase promotion on the top-selling product to grow revenue by focusing on repeat buyers.",
#   "reasoning": "Used calculate_total_tool on 'revenue' to sum all sales.",
#   "data_issues": []
# }
# """

# --- Logging: write debug to logs/interactive.log, keep console quiet (WARNING+) ---
import logging
from logging.handlers import RotatingFileHandler

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOGFILE = os.path.join(LOG_DIR, "interactive.log")

logger = logging.getLogger("agents.agent")
logger.setLevel(logging.DEBUG)  # allow handlers to decide what to emit

# File handler (rotating) — debug + info go here for post-mortem
file_handler = RotatingFileHandler(LOGFILE, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s %(message)s"))

# Console handler — only warnings and errors printed to CLI so it stays clean
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

# Avoid adding handlers multiple times if module reloaded
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
# Optional: don't propagate to root to avoid duplicate messages if root configured elsewhere
logger.propagate = False

class ConversationSession:
    def __init__(
        self,
        system_prompt: str,
        mcp_servers: dict,
        allowed_tools: list,
        session_id_path: str = "state/interactive/session_id.txt",
        session_history_dir: str = "state/interactive/history",
    ):
        """
        Create a session wrapper that will build ClaudeAgentOptions at connect time.

        Parameters:
        - system_prompt, mcp_servers, allowed_tools: used to construct ClaudeAgentOptions on each connect().
        - session_id_path: path to latest session id (will be read to determine resume).
        - session_history_dir: directory to append historic session ids.
        """
        self.system_prompt = system_prompt
        self.mcp_servers = mcp_servers
        self.allowed_tools = allowed_tools

        self.options: Optional[ClaudeAgentOptions] = None
        self.client: Optional[ClaudeSDKClient] = None
        self.turn_out = 0
        self.session_id: Optional[str] = None
        self.session_id_path = session_id_path
        self.session_history_dir = session_history_dir
        self.session_history_path = os.path.join(self.session_history_dir, "session_ids.txt")

        # ensure directories exist
        try:
            pathlib.Path(self.session_id_path).parent.mkdir(parents=True, exist_ok=True)
            pathlib.Path(self.session_history_dir).mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.warning("Failed to create session directories: %s", e)

    def _build_options(self) -> ClaudeAgentOptions:
        """
        Build and return a fresh ClaudeAgentOptions object for connect().
        If the session_id file exists, include resume=<id>, otherwise omit it.
        """
        resume_session_id = None
        try:
            if os.path.exists(self.session_id_path):
                with open(self.session_id_path, "r", encoding="utf-8") as f:
                    resume_session_id = f.read().strip() or None
        except OSError as e:
            logger.warning("Failed to read session_id_path: %s", e)

        opts_kwargs = {
            "system_prompt": self.system_prompt,
            "mcp_servers": self.mcp_servers,
            "allowed_tools": self.allowed_tools,
        }
        if resume_session_id:
            opts_kwargs.update({"resume": resume_session_id})
            logger.info("connect(): will try to resume session %s", resume_session_id)
        else:
            logger.info("connect(): no resume id found; will start a fresh session")

        return ClaudeAgentOptions(**opts_kwargs)

    async def connect(self):
        """
        Build options freshly and connect the ClaudeSDKClient.
        """
        # Build options each time we connect — this enables 'new' to avoid resume.
        try:
            self.options = self._build_options()
        except Exception as e:
            logger.exception("Failed to build ClaudeAgentOptions: %s", e)
            raise

        self.client = ClaudeSDKClient(options=self.options)
        try:
            await self.client.connect()
            logger.info("Connected to Claude SDK client")
        except ClaudeSDKError as e:
            logger.exception("Claude SDK error during connect")
            raise
        except Exception as e:
            logger.exception("Unexpected error during client.connect()")
            raise

    async def disconnect(self):
        if self.client:
            try:
                await self.client.disconnect()
                logger.info("Client disconnected")
            except ClaudeSDKError as e:
                logger.warning("SDK error during disconnect (ignored): %s", e)
            except Exception as e:
                logger.exception("Unexpected error during disconnect (ignored): %s", e)
            finally:
                self.client = None


    def _write_atomic(self, path: str, text: str) -> None:
        """Atomically write 'text' to 'path' by writing to a temp file then replacing."""
        tmp = f"{path}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(text)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except OSError as e:
            logger.exception("Atomic write failed for %s: %s", path, e)
            raise


    def _maybe_handle_init(self, msg) -> None:
        """
        Capture SDK 'init' messages that contain session_id.
        - Always append the new session id to the history file (timestamp + sid) when it changes.
        - Atomically write the latest sid to self.session_id_path.
        - Avoid immediate duplicate entries (if the latest history line already equals the sid).
        """
        try:
            subtype = getattr(msg, "subtype", None)
            data = getattr(msg, "data", None) or {}
            if subtype != "init" or not isinstance(data, dict):
                return

            sid = data.get("session_id") or data.get("sessionId") or data.get("id")
            if not sid:
                return
            sid = str(sid)

            # If the sid is the same as currently stored in memory, do nothing.
            if sid == self.session_id:
                logger.debug("Received same session id as current; no change.")
                return

            # Ensure directories exist
            try:
                pathlib.Path(self.session_history_dir).mkdir(parents=True, exist_ok=True)
                pathlib.Path(self.session_id_path).parent.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                logger.warning("Could not create session directories: %s", e, exc_info=True)

            history_path = self.session_history_path

            # Append new sid to history unless the last recorded line already equals this sid
            try:
                append_sid = True
                if os.path.exists(history_path):
                    try:
                        # Read last non-empty line to avoid duplicates
                        with open(history_path, "rb") as h:
                            # Seek from end and find last line efficiently
                            h.seek(0, os.SEEK_END)
                            end = h.tell()
                            if end == 0:
                                last_line = None
                            else:
                                # Walk backwards to find last newline
                                pos = end - 1
                                while pos >= 0:
                                    h.seek(pos)
                                    if h.read(1) == b"\n":
                                        break
                                    pos -= 1
                                if pos < 0:
                                    h.seek(0)
                                else:
                                    h.seek(pos + 1)
                                last_line = h.readline().decode("utf-8", errors="ignore").strip()
                        if last_line and last_line.endswith("\t" + sid):
                            # last line already contains this sid
                            append_sid = False
                    except Exception:
                        # If reading last line fails for any reason, fallback to simple append.
                        logger.debug("Could not read last history line to dedupe; will append.", exc_info=True)
                        append_sid = True

                if append_sid:
                    ts = datetime.utcnow().isoformat() + "Z"
                    line = f"{ts}\t{sid}\n"
                    # Append in text mode
                    try:
                        with open(history_path, "a", encoding="utf-8") as hf:
                            hf.write(line)
                            hf.flush()
                            os.fsync(hf.fileno())
                        logger.info("Appended new session id to history: %s", history_path)
                    except OSError as e:
                        logger.warning("Failed to append session id to history: %s", e, exc_info=True)
            except Exception:
                logger.exception("Unexpected error when updating session history; continuing.")

            # Atomically write latest session id
            try:
                self._write_atomic(self.session_id_path, sid)
            except OSError:
                # Fallback: try simple write (not atomic). Log the error.
                try:
                    with open(self.session_id_path, "w", encoding="utf-8") as f:
                        f.write(sid)
                    logger.info("Saved latest session id (non-atomic) to %s", self.session_id_path)
                except Exception as e:
                    logger.exception("Failed to save latest session id: %s", e)

            # Update in-memory
            self.session_id = sid

        except Exception:
            # Log unexpected errors but don't crash the agent
            logger.exception("Unhandled error in _maybe_handle_init")

    def display_message(self, msg) -> None:
        """
        Display message content in a compact human-readable form.

        Defensive behavior: tolerates partial/unexpected SDK objects.
        Uses logging for internal messages; uses print minimally for interactive prompts/results.
        """
        def _get_status(obj):
            if isinstance(obj, dict):
                return obj.get("status")
            return getattr(obj, "status", None)

        # ---------------------------
        # Handle User messages
        # ---------------------------
        try:
            if isinstance(msg, UserMessage):
                for block in getattr(msg, "content", []):
                    try:
                        if isinstance(block, TextBlock):
                            # Print user's text to the console (user-facing)
                            print(f"User: {block.text}")
                        elif isinstance(block, ToolResultBlock):
                            content = getattr(block, "content", None)
                            preview = None
                            if isinstance(content, (dict, list)):
                                try:
                                    preview = json.dumps(content, indent=2, ensure_ascii=False)[:200]
                                except Exception:
                                    preview = str(content)[:200]
                            else:
                                preview = str(content)[:200]
                            logger.info("User (tool result preview): %s", preview)
                    except (AttributeError, KeyError, TypeError) as e:
                        logger.debug("Error rendering user block: %s", e, exc_info=True)
                return
        except Exception as e:
            logger.exception("Error handling UserMessage: %s", e)

        # ---------------------------
        # Handle Assistant messages
        # ---------------------------
        try:
            if isinstance(msg, AssistantMessage):
                for block in getattr(msg, "content", []):
                    try:
                        if isinstance(block, TextBlock):
                            # Assistant text: show to user and also log
                            print(f"Claude: {block.text}")
                            logger.info("Assistant text block: %s", block.text)
                        elif isinstance(block, ToolUseBlock):
                            name = getattr(block, "name", getattr(block, "id", "<unknown>"))
                            print(f"[Tool use] {name}", flush=True)
                            logger.info("[Tool use] %s", name)
                            if getattr(block, "input", None) is not None:
                                logger.info("  Input: %s", getattr(block, "input"))
                        elif isinstance(block, ToolResultBlock):
                            content = getattr(block, "content", None)
                            status = _get_status(content)
                            if status is not None:
                                logger.info("Tool status: %s", status)
                            if isinstance(content, (dict, list)):
                                # Log full JSON tool result (debug level)
                                print("[Tool result]:")
                                print(content.status)
                                print(json.dumps(content, indent=2, ensure_ascii=False))
                                try:
                                    logger.info("[Tool result]:\n%s", json.dumps(content, indent=2, ensure_ascii=False))
                                except Exception:
                                    logger.info("[Tool result]: %s", str(content)[:2000])
                            else:
                                print(content.status)
                                print(f"[Tool result]: {str(content)[:1000]}")
                                logger.info("Tool result object: %s", str(content)[:1000])
                        else:
                            print(f"[Unknown block type: {type(block).__name__}]")
                            logger.info("[Unknown block type: %s]", type(block).__name__)
                    except (AttributeError, KeyError, TypeError) as e:
                        logger.debug("Error rendering assistant block: %s", e, exc_info=True)
                return
        except Exception as e:
            logger.exception("Error handling AssistantMessage: %s", e)

    async def start(self) -> None:
        """
        Run the interactive conversation loop.

        Commands:
        - 'exit'      → quit session
        - 'interrupt' → stop current task
        - 'new'       → start a new session

        Ensures the client is connected before sending queries and always attempts to disconnect in finally.
        """
        try:
            await self.connect()
        except ClaudeSDKError as e:
            logger.error("Failed to connect; aborting interactive session: %s", e)
            return
        except Exception as e:
            logger.exception("Unexpected error during connect; aborting.")
            return

        # User-facing welcome
        print("You're starting a conversation with Claude Data Analysis Agent")
        print("Commands: type 'exit' to quit, 'interrupt' to stop current task, 'new' for new session.")

        try:
            while True:
                try:
                    query = input("You: ").strip()
                except (KeyboardInterrupt, EOFError):
                    print("\nExiting session.")
                    break

                if not query:
                    continue

                cmd = query.lower().strip()
                if cmd == "exit":
                    break
                elif cmd == "interrupt":
                    if self.client:
                        try:
                            await self.client.interrupt()
                            print("Task interrupted!")
                        except ClaudeSDKError as e:
                            logger.warning("Failed to interrupt task (SDK error): %s", e)
                            print("Failed to interrupt task.")
                        except Exception as e:
                            logger.exception("Failed to interrupt task (unexpected): %s", e)
                            print("Failed to interrupt task.")
                    else:
                        print("No active client to interrupt.")
                    continue
                if query.lower() == 'new':
                    # --- NEW SESSION FLOW ---
                    # 1. Disconnect current client (if any)
                    if self.client:
                        await self.disconnect()

                    # 2. Archive or remove the existing session_id file so next connect doesn't resume
                    try:
                        # Move current session_id file to history with timestamp if exists
                        if os.path.exists(self.session_id_path):
                            with open(self.session_id_path, "r", encoding="utf-8") as f:
                                sid = f.read().strip()
                            if sid:
                                ts = datetime.utcnow().isoformat() + "Z"
                                hist_line = f"{ts}\t{sid}\n"
                                # append history
                                pathlib.Path(self.session_history_dir).mkdir(parents=True, exist_ok=True)
                                with open(self.session_history_path, "a", encoding="utf-8") as h:
                                    h.write(hist_line)
                            # remove latest so next connect won't resume
                            try:
                                os.remove(self.session_id_path)
                                logger.info("Removed latest session_id file to force a fresh session on next connect")
                            except OSError:
                                # if removal fails, try to overwrite with empty content
                                try:
                                    with open(self.session_id_path, "w", encoding="utf-8") as f:
                                        f.write("")
                                except Exception:
                                    logger.warning("Could not remove or clear session_id_path")
                    except Exception as e:
                        logger.exception("Error archiving/removing session_id file: %s", e)

                    # 3. Clear in-memory session id so logic won't treat as same
                    self.session_id = None

                    # 4. Connect — since session_id file is gone, connect() will NOT set resume in options
                    try:
                        await self.connect()
                        print("Started a new session.")
                    except Exception as e:
                        logger.exception("Failed to start new session: %s", e)
                        print("Failed to start new session; see logs.")
                    continue
                
                if not self.client:
                    try:
                        await self.connect()
                    except ClaudeSDKError as e:
                        logger.error("Unable to connect client: %s", e)
                        print("Unable to connect client.")
                        continue
                    except Exception as e:
                        logger.exception("Unexpected error connecting client: %s", e)
                        print("Unable to connect client.")
                        continue

                # send query and stream responses
                try:
                    await self.client.query(query)
                    async for message in self.client.receive_response():
                        # capture session start if present
                        try:
                            self._maybe_handle_init(message)
                        except Exception as e:
                            logger.debug("Error in _maybe_handle_init: %s", e, exc_info=True)

                        # display the message
                        self.display_message(message)
                    # newline after full response
                    print()
                except ClaudeSDKError as e:
                    logger.exception("SDK error during query/response: %s", e)
                    print("Error during query or response (SDK error). See logs for details.")
                    await self.disconnect()
                    continue
                except Exception as e:
                    logger.exception("Unexpected error during query/response: %s", e)
                    print("Error during query or response (unexpected). See logs for details.")
                    await self.disconnect()
                    continue

            print("Conversation ended.")
        finally:
            await self.disconnect()


async def main():
    """
    Entry point: build the MCP server and start the conversation session.

    On startup, resumes the last conversation if a session ID exists in
    `state/interactive/session_id.txt`. 
    The user can type `new` during an active session to archive the old ID
    into `state/interactive/history/session_ids.txt` and start a fresh
    Claude session without resuming previous context.

    This ensures persistent continuity across runs but allows clean restarts
    when needed.
    """
    from claude_agent_sdk import create_sdk_mcp_server 

    dataAnalysis = create_sdk_mcp_server(
        name="dataAnalysis",
        version="1.0.0", 
        tools=[
            validate_data_tool,
            calculate_total_tool,
            get_top_n_tool,
            filter_by_value_tool,
        ],
    )

    session_id_file = "state/interactive/session_id.txt"
    session_history_dir = "state/interactive/history"
    resume_session_id = None

    try:
        if os.path.exists(session_id_file):
            try:
                with open(session_id_file, "r", encoding="utf-8") as f:
                    resume_session_id = f.read().strip() or None
                    if resume_session_id:
                        logger.info("Resuming session id loaded from %s", session_id_file)
            except OSError as e:
                logger.warning("Failed to read session id file: %s", e)
    except Exception as e:
        logger.exception("Unexpected error checking session id file: %s", e)

    
    session = ConversationSession(
    system_prompt=SYSTEM_PROMPT,
    mcp_servers={"dataAnalysis": dataAnalysis},
    allowed_tools=ALLOWED_TOOLS,
    session_id_path="state/interactive/session_id.txt",
    session_history_dir="state/interactive/history",
    )
    await session.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted. Goodbye.")
    except Exception as e:
        logger.exception("Fatal error in main: %s", e)
        # explicit non-zero exit for CI if needed
        raise
