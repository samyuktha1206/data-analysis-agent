import os
import json
import asyncio
from typing import Optional
from claude_agent_sdk import (
    ClaudeSDKClient,
    ClaudeAgentOptions,
    AssistantMessage,
    UserMessage,
    TextBlock,
    ToolUseBlock,
    ToolResultBlock,
)
from tools import validate_data_tool, calculate_total_tool, get_top_n_tool, filter_by_value_tool

ALLOWED_TOOLS = [
    "mcp__dataAnalysis__validate_data_tool",
    "mcp__dataAnalysis__calculate_total_tool",
    "mcp__dataAnalysis__get_top_n_tool",
    "mcp__dataAnalysis__filter_by_value_tool"
]

SYSTEM_PROMPT = """
You are a Data Analysis Agent. You analyze structured datasets (CSV/JSON) using the tools provided.
You MUST use the provided tools to read and compute answers; do not invent numbers.

ENVIRONMENT & DEFAULTS
- The dataset path is provided to the Python runtime via the DATA_PATH environment variable.
- The dataset schema (read-only) is:
    - products : string  (product name)
    - revenue  : numeric (sales amount)
- Column names in the dataset are normalized to lower-case by the code (so use 'products' and 'revenue').
- If the user asks for "total sales" and does not specify a column, assume column = "revenue".

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
1. Always call the validate_data_tool first to make sure the data has no missing values or negatives. Once the 
validate_data_tool results in "ok":"True" alone move on to call the appropriate tool to process the 
user query. If the data has negative values or missing values or null values, abort and return the error message to the user.
2. For any query that requires numbers or data from the dataset you MUST call the appropriate tool.
   - Aggregation => call calculate_total_tool.
   - Top-N      => call get_top_n_tool.
   - Filtering  => call filter_by_value_tool.
3. If you do not know which column to use, first call validate_data_tool to learn column names.
4. If a tool returns an error (dataset missing, column missing, file unreadable), include that error in the structured output's "data_issues" list.
5. If the user's query is ambiguous, ask exactly one clarifying question (and do not call tools).
6. Do not call any tool unless the query requires it.

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
- If ambiguous, set intent="ambiguous" and include clarifying_question; do not call tools.

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

class ConversationSession:
    """
    Simple conversation session wrapper around ClaudeSDKClient.

    Responsibilities:
      - create/connect/disconnect the client
      - accept a small set of CLI commands (exit, interrupt, new)
      - display incoming messages safely (handles missing attributes)
    """
    def __init__(self, options: Optional[ClaudeAgentOptions] = None):
        """
        Initialize the ConversationSession.

        Parameters
        ----------
        options : Optional[ClaudeAgentOptions]
            Options used to construct the ClaudeSDKClient. Must be provided
            before calling connect().
        """
        self.options = options
        self.client: Optional[ClaudeSDKClient] = None
        self.turn_out = 0

    async def connect(self):
        """
        Create the ClaudeSDKClient and connect.

        Raises
        ------
        RuntimeError
            If self.options is None.
        Exception
            Any exception raised by the underlying client.connect() is propagated.
        """
        if self.options is None:
            raise RuntimeError(
                "ConversationSession.options is None — provide ClaudeAgentOptions before connect()"
            )
        self.client = ClaudeSDKClient(options=self.options)
        await self.client.connect()

    async def disconnect(self):
        """
        Disconnect the client and clear the reference. Safe to call multiple times.
        Any exception during disconnect is caught and ignored to ensure graceful shutdown.
        """
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                # swallow exceptions during disconnect to avoid crashing cleanup
                # (the user-facing CLI will already be shutting down)
                pass
            finally:
                self.client = None

    def display_message(self, msg):
        """
        Display message content in a compact human-readable form.

        This function is defensive: it tolerates partial or unexpected SDK objects.
        """
        # ---------------------------
        # Handle User messages
        # ---------------------------
        
        try:
            if isinstance(msg, UserMessage):
                for block in msg.content:
                    try:
                        if isinstance(block, TextBlock):
                            print(f"User: {block.text}")
                        elif isinstance(block, ToolResultBlock):
                            content = block.content
                            if isinstance(content, (dict, list)):
                                preview = json.dumps(content)[:200]
                            else:
                                preview = str(content)[:200]
                            print(f"Tool Result: {preview}...")
                    except Exception as e:
                        print(f"[Error rendering user block: {e}]")
                return
        except Exception as e:
            print(f"[Error handling UserMessage: {e}]")

        # ---------------------------
        # Handle Assistant messages
        # ---------------------------
        try:
            if isinstance(msg, AssistantMessage):
                for block in msg.content:
                    try:
                        if isinstance(block, TextBlock):
                            print(f"Claude: {block.text}")
                        elif isinstance(block, ToolUseBlock):
                            name = getattr(block, "name", getattr(block, "id", "<unknown>"))
                            print(f"[Tool use] {name}")
                            if getattr(block, "input", None):
                                print("  Input:", block.input)
                        elif isinstance(block, ToolResultBlock):
                            content = block.content
                            if isinstance(content, (dict, list)):
                                print("[Tool result]:")
                                print(json.dumps(content, indent=2, ensure_ascii=False))
                            else:
                                print(f"[Tool result]: {str(content)[:1000]}")
                        else:
                            print(f"[Unknown block type: {type(block).__name__}]")
                    except Exception as e:
                        print(f"[Error rendering assistant block: {e}]")
                return
        except Exception as e:
            print(f"[Error handling AssistantMessage: {e}]")


    async def start(self):
        """
        Run the interactive conversation loop.

        Commands:
        - 'exit'      → quit session
        - 'interrupt' → stop current task
        - 'new'       → start a new session
        The method ensures the client is connected before sending queries and
        always attempts to disconnect in the finally block.
        """
        await self.connect()
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

                if query.lower() == 'exit':
                    break
                elif query.lower() == 'interrupt':
                    if self.client:
                        try:
                            await self.client.interrupt()
                            print("Task interrupted!")
                        except Exception as e:
                            print(f"Failed to interrupt task: {e}")
                    else:
                        print("No active client to interrupt.")
                    continue
                elif query.lower() == 'new':
                    if self.client:
                        await self.disconnect()
                    try:
                        await self.connect()
                        print("Started a new session.")
                    except Exception as e:
                        print(f"Failed to start new session: {e}")
                    continue

                # --- normal message flow ---
                if not self.client:
                    try:
                        await self.connect()
                    except Exception as e:
                        print(f"Unable to connect client: {e}")
                        continue

                try:
                    await self.client.query(query)
                    async for message in self.client.receive_response():
                        self.display_message(message)
                    print()  # newline after response
                except Exception as e:
                    print(f"Error during query or response: {e}")
                    # disconnect to reset state
                    await self.disconnect()
                    continue
                
            print("Conversation ended.")
        finally:
            await self.disconnect()

async def main():
    """
    Entry point: build the MCP server and start the conversation session.
    Kept minimal so main.py doesn't need to know tool internals.
    """

    from claude_agent_sdk import create_sdk_mcp_server

    dataAnalysis = create_sdk_mcp_server(
        name="dataAnalysis",
        version="2.0.0",
        tools=[
            validate_data_tool,
            calculate_total_tool,
            get_top_n_tool,
            filter_by_value_tool,
        ],
    )
    options = ClaudeAgentOptions(
        system_prompt=SYSTEM_PROMPT,
            mcp_servers={"dataAnalysis": dataAnalysis},
            allowed_tools=ALLOWED_TOOLS,
    )
    session = ConversationSession(options)
    await session.start()
    
if __name__ == "__main__":
    # safe: only runs when directly executing agent.py
    asyncio.run(main())

