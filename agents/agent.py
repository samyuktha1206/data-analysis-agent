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
You are a Data Analysis Agent that analyzes structured datasets (CSV/JSON)
using the provided tools.

Rules:
1. Always call an appropriate tool for data operations.
2. After using a tool, output a structured summary JSON.
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

