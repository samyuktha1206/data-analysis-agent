"""
main.py

Entry point for the Claude Data Analysis Agent.

Supports:
- Interactive mode (default): `python main.py`
- One-shot mode placeholder (prints a notice): `python main.py "your question"`

This script intentionally avoids nested asyncio.run() calls and performs
simple environment and file checks before starting the interactive session.
"""
import os
import sys
import asyncio
from typing import Optional
from dotenv import load_dotenv

# Import query under an alias to avoid name collisions with local variables
from claude_agent_sdk import query as sdk_query, ClaudeAgentOptions

# Agent constants (from agents/agent.py)
from agents.agent import ALLOWED_TOOLS, SYSTEM_PROMPT

# Load environment variables from .env if present (non-fatal if missing)
try:
    success = load_dotenv()
    print(f".env loaded successfully? {success}")
except Exception:
    # Swallow errors: absence or parse issues in .env should not crash the program
    pass

USAGE = """
Usage:
  Interactive mode: python main.py
  One-shot query:   python main.py "What's our total revenue?"
"""

async def run_one_shot() -> None:
    try:
        from agents import one_shot_agent
    except Exception as e:
        print("Failed to import agent module:", e)
        return
    try:
        await one_shot_agent.main()
    except KeyboardInterrupt:
        print("\nInterrupted. Exiting one-shot session.")
    except Exception as e:
        print("An unexpected error occurred in the one-shot session:", e)

async def run_interactive() -> None:
    """
    Launch the interactive conversation loop.

    Notes
    -----
    - Imports `agents.agent` lazily to avoid circular imports at module import time.
    - Expects `agents.agent.main()` to be an async coroutine that does NOT call asyncio.run().
    """
    try:
        from agents import agent  # lazy import to avoid circular deps
    except Exception as e:
        print("Failed to import agent module:", e)
        return
    
    try:
        await agent.main()
    except KeyboardInterrupt:
        # Graceful_exit for Ctrl+C during the interactive loop
        print("\nInterrupted. Exiting interactive session.")
    except Exception as e:
        # Keep the program alive and show a concise error
        print("An unexpected error occurred in the interactive session:", e)

def _check_environment() -> None:
    """
    Perform non-fatal checks for required environment variables and dataset path.
    Prints helpful hints; does not raise exceptions.
    """
    api_key: Optional[str] = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY")
    if not api_key:
        print(
            "Warning: ANTHROPIC_API_KEY (or CLAUDE_API_KEY) not set. "
            "The SDK may fail to authenticate without it."
        )

    data_path = os.environ.get("DATA_PATH", "data/sample_data.csv")
    print("DATA_PATH:", data_path)
    try:
        abs_path = os.path.abspath(data_path)
        if not os.path.exists(abs_path):
            print(f"Warning: data file not found at {data_path}. Create it or set DATA_PATH env var.")
    except Exception:
        # Even if path resolution fails, continue; the agent/tools will surface precise errors later.
        pass

async def main() -> None:
    """
    Program entry coroutine.

    Behavior
    --------
    - If a prompt is provided on the command line, prints a notice that one-shot mode
      is not enabled in this script version (kept to match current behavior).
    - Otherwise, enters interactive mode.
    - All exceptions are handled to provide a clean user experience.
    """
    _check_environment()

    # One-shot mode placeholder (kept as-is per your current script)
    if len(sys.argv) > 1:
        await run_one_shot()
        return

    # Interactive mode
    await run_interactive()


if __name__ == "__main__":
    # Single top-level asyncio.run call with graceful shutdown.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted. Goodbye.")
    except Exception as e:
        # Final safety net: keep the traceback minimal and user-friendly.
        print("Fatal error:", e)
        # Optionally exit with non-zero code to signal failure in CI/CD contexts.   
        sys.exit(1)

