# main.py
import os
import sys
import json
import asyncio
from dotenv import load_dotenv

# import query under an alias to avoid name collisions with local variables
from claude_agent_sdk import query as sdk_query, ClaudeAgentOptions

# Agent constants and MCP server (from your modules)
from agents.agent import ALLOWED_TOOLS, SYSTEM_PROMPT
# from tools import dataAnalysis  # MCP server created in tools.py
# print("DEBUG: dataAnalysis object:", dataAnalysis)

# try:
#     inst = dataAnalysis.get_instance() if hasattr(dataAnalysis, "get_instance") else dataAnalysis.get("instance", None)
#     print("DEBUG: dataAnalysis instance attr:", inst)
#     # list any attribute that might have tools
#     if inst is not None:
#         print("DEBUG: instance dir:", [n for n in dir(inst) if 'tool' in n.lower() or 'reg' in n.lower()])
# except Exception as e:
#     print("DEBUG: inspecting dataAnalysis failed:", e)

# load .env if present
load_dotenv()

USAGE = """
Usage:
  Interactive mode: python main.py
  One-shot query:   python main.py "What's our total revenue?"

Notes:
  - set ANTHROPIC_API_KEY in env or .env file.
  - Data path defaults to data/sample_data.csv but can be changed via DATA_PATH env var.
"""

# async def run_one_shot(prompt: str):
#     """Run a single one-off query using the lightweight sdk_query() API."""

#     options = ClaudeAgentOptions(
#         system_prompt=SYSTEM_PROMPT,
#         mcp_servers={"dataAnalysis": dataAnalysis},
#         allowed_tools=ALLOWED_TOOLS,
#         max_turns=1,
#     )

#     print(f"\n🧠 Running one-shot query:\n> {prompt}\n")

#     print("DEBUG: MCP servers loaded:", options.mcp_servers)
#     print("DEBUG: Allowed tools:", options.allowed_tools) 

#     text_blocks: list[str] = []

    # sdk_query is an async iterator that yields messages
    # async for message in sdk_query(prompt=prompt, options=options):
    #     content = getattr(message, "content", None)
    #     if content:
    #         for block in content:
    #             txt = getattr(block, "text", None) or (block.get("text") if isinstance(block, dict) else None)
    #             if txt:
    #                 # stream to stdout and collect
    #                 print(txt, end="", flush=True)
    #                 text_blocks.append(txt)

    # print()  # newline after streaming completes
    # combined = "\n".join(text_blocks).strip()
    # print("\n--- RAW COMBINED OUTPUT ---")
    # print(combined)
    # print("--- END RAW OUTPUT ---\n")

async def run_interactive():
    from agents import agent
    await agent.main()

    # session = ConversationSession()
    # try:
    #     await session.start()
    # finally:
    #     await session.disconnect()

async def main():
    # quick env checks
    api_key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY")
    if not api_key:
        print("Warning: ANTHROPIC_API_KEY (or CLAUDE_API_KEY) not set in environment. The SDK may fail without it.")

    data_path = os.environ.get("DATA_PATH", "data/sample_data.csv")
    print(data_path)
    if not os.path.exists(os.path.abspath(data_path)):
        print(f"Warning: data file not found at {data_path}. Create it or set DATA_PATH env var.")

    if len(sys.argv) > 1:
        prompt = " ".join(sys.argv[1:])
        # asyncio.run(run_one_shot(prompt))
    else:
        asyncio.run(run_interactive())

if __name__ == "__main__":
    asyncio.run(main())
