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
1. For any query that requires numbers or data from the dataset you MUST call the appropriate tool.
   - Aggregation => call calculate_total_tool.
   - Top-N      => call get_top_n_tool.
   - Filtering  => call filter_by_value_tool.
2. If you do not know which column to use, first call validate_data_tool to learn column names.
3. If a tool returns an error (dataset missing, column missing, file unreadable), include that error in the structured output's "data_issues" list.
4. If the user's query is ambiguous, ask exactly one clarifying question (and do not call tools).
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
    def __init__(self, options: Optional[ClaudeAgentOptions] = None):
        # store options for later use; do NOT create the client at import time
        self.options = options
        # correct type annotation with assignment (do NOT try to assign Optional[...] as a value)
        self.client: Optional[ClaudeSDKClient] = None
        self.turn_out = 0

    async def connect(self):
        # create client using stored options (must be set before calling connect)
        if self.options is None:
            raise RuntimeError("ConversationSession.options is None — provide ClaudeAgentOptions before connect()")
        self.client = ClaudeSDKClient(options=self.options)
        await self.client.connect()

    async def disconnect(self):
        if self.client:
            await self.client.disconnect()
            self.client = None

    def display_message(self, msg):
        """Display message content in a clean format."""

        if isinstance(msg, UserMessage):
            for block in msg.content:
                if isinstance(block, TextBlock):
                    print(f"User: {block.text}")
                elif isinstance(block, ToolResultBlock):
                    print(
                        f"Tool Result: {block.content[:100] if block.content else 'None'}..."
                    )
        elif isinstance(msg, AssistantMessage):
            # for block in msg.content:
            #     if isinstance(block, TextBlock):
            #         print(f"Claude: {block.text}")
            #     elif isinstance(block, ToolUseBlock):
            #         print(f"Using tool: {block.name}")
            #         # Show tool inputs for calculator
            #         if block.input:
            #             print(f"  Input: {block.input}")
            for block in getattr(msg, "content", []) or []:
                if isinstance(block, TextBlock):
                    # streaming behavior in start() prints without newline; here we print a line
                    print(f"Claude: {block.text}")
                elif isinstance(block, ToolUseBlock):
                    name = getattr(block, "name", getattr(block, "id", "<unknown>"))
                    print(f"[Tool use] {name}")
                    if getattr(block, "input", None):
                        print("  Input:", block.input)
                elif isinstance(block, ToolResultBlock):
                    print("DEBUG TOOL BLOCK RECEIVED:", type(block), repr(block)[:500])
                    
                    content = block.content
                    
                    print("DEBUG TOOL CONTENT TYPE:", type(content))
                    print("DEBUG TOOL CONTENT REPR (first 1000 chars):")

                    if isinstance(content, (dict, list)):
                        print("[Tool result]:")
                        print(json.dumps(content, indent=2, ensure_ascii=False))
                    else:
                        print(f"[Tool result]: {str(content)[:500]}")


    async def start(self):
        await self.connect()
        print("You're starting a conversation with Claude Data Analysis Agent")
        print("Commands: type 'exit' to quit, 'interrupt' to stop current task, 'new' for new session.")

        try:
            while True:
                query = input("You: ").strip()
                if not query:
                    continue
                if query.lower() == 'exit':
                    break
                elif query.lower() == 'interrupt':
                    await self.client.interrupt()
                    print("Task interrupted!")
                    continue
                elif query.lower() == 'new':
                    await self.client.disconnect()
                    await self.client.connect()
                    print("Started a new session.")
                    continue

                await self.client.query(query)

                async for message in self.client.receive_response():
                    self.display_message(message)
                print()  # newline after response

            await self.client.disconnect()
            print("Conversation ended.")
        finally:
            await self.disconnect()

async def main():
    # Build the MCP server here so main.py does not have to know about tools internals
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

