# main.py
import os
import sys
import asyncio
from dotenv import load_dotenv

# import query under an alias to avoid name collisions with local variables
from claude_agent_sdk import query as sdk_query, ClaudeAgentOptions

# Agent constants (from agents/agent.py)
from agents.agent import ALLOWED_TOOLS, SYSTEM_PROMPT

load_dotenv()

USAGE = """
Usage:
  Interactive mode: python main.py
  One-shot query:   python main.py "What's our total revenue?"
"""

async def run_interactive():
    # import lazily to avoid circular imports at module import time
    from agents import agent
    # agent.main() must be an async coroutine and must NOT call asyncio.run() internally.
    await agent.main()

async def main():
    # quick env checks
    api_key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY")
    if not api_key:
        print("Warning: ANTHROPIC_API_KEY (or CLAUDE_API_KEY) not set in environment. The SDK may fail without it.")

    data_path = os.environ.get("DATA_PATH", "data/sample_data.csv")
    print("DATA_PATH:", data_path)
    if not os.path.exists(os.path.abspath(data_path)):
        print(f"Warning: data file not found at {data_path}. Create it or set DATA_PATH env var.")

    if len(sys.argv) > 1:
        prompt = " ".join(sys.argv[1:])
        # one-shot feature currently disabled in your snippet; if you re-enable it,
        # ensure dataAnalysis is available and sdk_query is used safely.
        print("One-shot mode not enabled in this script version.")
    else:
        # AVOID nested asyncio.run: simply await the coroutine
        await run_interactive()

if __name__ == "__main__":
    # Single top-level asyncio.run call
    asyncio.run(main())
