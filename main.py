# """
# main.py

# Entry point for the Claude Data Analysis Agent.

# Supports:
# - Interactive mode (default): `python main.py`
# - One-shot mode placeholder (prints a notice): `python main.py "your question"`

# This script intentionally avoids nested asyncio.run() calls and performs
# simple environment and file checks before starting the interactive session.
# """
# import os
# import sys
# import asyncio
# from typing import Optional
# from dotenv import load_dotenv

# # Load environment variables from .env if present (non-fatal if missing)
# try:
#     load_dotenv()
# except Exception:
#     pass

# USAGE = """
# Usage:
#   Interactive mode: python main.py
#   One-shot query:   python main.py "What's our total revenue?"
# """

# async def run_one_shot() -> None:
#     try:
#         from agents import one_shot_agent
#     except Exception as e:
#         print("Failed to import agent module:", e)
#         return
#     try:
#         await one_shot_agent.main()
#     except KeyboardInterrupt:
#         print("\nInterrupted. Exiting one-shot session.")
#     except Exception as e:
#         print("An unexpected error occurred in the one-shot session:", e)

# async def run_interactive() -> None:
#     """
#     Launch the interactive conversation loop.

#     Notes
#     -----
#     - Imports `agents.agent` lazily to avoid circular imports at module import time.
#     - Expects `agents.agent.main()` to be an async coroutine that does NOT call asyncio.run().
#     """
#     try:
#         from agents import agent  # lazy import to avoid circular deps
#     except Exception as e:
#         print("Failed to import agent module:", e)
#         return
    
#     try:
#         await agent.main()
#     except KeyboardInterrupt:
#         # Graceful_exit for Ctrl+C during the interactive loop
#         print("\nInterrupted. Exiting interactive session.")
#     except Exception as e:
#         # Keep the program alive and show a concise error
#         print("An unexpected error occurred in the interactive session:", e)

# def _check_environment() -> None:
#     """
#     Perform non-fatal checks for required environment variables and dataset path.
#     Prints helpful hints; does not raise exceptions.
#     """
#     api_key: Optional[str] = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY")
#     if not api_key:
#         print(
#             "Warning: ANTHROPIC_API_KEY (or CLAUDE_API_KEY) not set. "
#             "The SDK may fail to authenticate without it."
#         )

#     data_path = os.environ.get("DATA_PATH", "data/sample_data.csv")
#     print("DATA_PATH:", data_path)
#     try:
#         abs_path = os.path.abspath(data_path)
#         if not os.path.exists(abs_path):
#             print(f"Warning: data file not found at {data_path}. Create it or set DATA_PATH env var.")
#     except Exception:
#         # Even if path resolution fails, continue; the agent/tools will surface precise errors later.
#         pass

# async def main() -> None:
#     """
#     Program entry coroutine.

#     Behavior
#     --------
#     - If a prompt is provided on the command line, enters one-shot mode.
#     - Otherwise, enters interactive mode.
#     - All exceptions are handled to provide a clean user experience.
#     """
#     _check_environment()

#     # One-shot mode 
#     if len(sys.argv) > 1:
#         await run_one_shot()
#         return

#     # Interactive mode
#     await run_interactive()


# if __name__ == "__main__":
#     # Single top-level asyncio.run call with graceful shutdown.
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         print("\nInterrupted. Goodbye.")
#     except Exception as e:
#         # Final safety net: keep the traceback minimal and user-friendly.
#         print("Fatal error:", e)
#         # Optionally exit with non-zero code to signal failure in CI/CD contexts.   
#         sys.exit(1)

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
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional
from dotenv import load_dotenv

# --- Logging setup: write debug to logs/main.log, keep console clean (WARNING+) ---
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
MAIN_LOGFILE = os.path.join(LOG_DIR, "main.log")

logger = logging.getLogger("agents.main")
logger.setLevel(logging.DEBUG)  # allow handlers to decide what to emit

# Rotating file handler: keep detailed logs for post-mortem
file_handler = RotatingFileHandler(MAIN_LOGFILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s %(message)s"))

# Console handler: only warnings/errors to avoid cluttering interactive CLI
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
# avoid propagation to root logger to prevent duplicate messages
logger.propagate = False

# Load environment variables from .env if present (non-fatal if missing)
try:
    load_dotenv()
    logger.debug(".env loaded (if present).")
except Exception:
    # Swallow errors: absence or parse issues in .env should not crash the program
    logger.exception("Error while loading .env (ignored)")

USAGE = """
Usage:
  Interactive mode: python main.py
  One-shot query:   python main.py "What's our total revenue?"
"""

async def run_one_shot() -> None:
    try:
        from agents import one_shot_agent
    except Exception as e:
        # Keep user-facing message simple but log full trace
        print("Failed to import one-shot agent module. See logs for details.")
        logger.exception("Failed to import one_shot_agent: %s", e)
        return
    try:
        await one_shot_agent.main()
    except KeyboardInterrupt:
        print("\nInterrupted. Exiting one-shot session.")
        logger.info("One-shot session interrupted by user.")
    except Exception as e:
        print("An unexpected error occurred in the one-shot session. See logs for details.")
        logger.exception("Unexpected error in one-shot session: %s", e)

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
        print("Failed to import agent module. See logs for details.")
        logger.exception("Failed to import agents.agent: %s", e)
        return
    
    try:
        await agent.main()
    except KeyboardInterrupt:
        # Graceful_exit for Ctrl+C during the interactive loop
        print("\nInterrupted. Exiting interactive session.")
        logger.info("Interactive session interrupted by user.")
    except Exception as e:
        # Keep the program alive and show a concise error
        print("An unexpected error occurred in the interactive session. See logs for details.")
        logger.exception("Unexpected error in interactive session: %s", e)

def _check_environment() -> None:
    """
    Perform non-fatal checks for required environment variables and dataset path.
    Prints helpful hints; does not raise exceptions.
    """
    api_key: Optional[str] = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY")
    if not api_key:
        # user-facing notification
        print(
            "Warning: ANTHROPIC_API_KEY (or CLAUDE_API_KEY) not set. "
            "The SDK may fail to authenticate without it."
        )
        logger.warning("ANTHROPIC_API_KEY or CLAUDE_API_KEY not set in environment.")

    data_path = os.environ.get("DATA_PATH", "data/sample_data.csv")
    # still print to user for clarity, but also log
    print("DATA_PATH:", data_path)
    logger.info("DATA_PATH: %s", data_path)
    try:
        abs_path = os.path.abspath(data_path)
        if not os.path.exists(abs_path):
            print(f"Warning: data file not found at {data_path}. Create it or set DATA_PATH env var.")
            logger.warning("Data file not found at %s (abs: %s)", data_path, abs_path)
    except Exception as e:
        # Even if path resolution fails, continue; the agent/tools will surface precise errors later.
        logger.exception("Error while checking DATA_PATH: %s", e)

async def main() -> None:
    """
    Program entry coroutine.

    Behavior
    --------
    - If a prompt is provided on the command line, enters one-shot mode.
    - Otherwise, enters interactive mode.
    - All exceptions are handled to provide a clean user experience.
    """
    logger.debug("main() start")
    _check_environment()

    # One-shot mode 
    if len(sys.argv) > 1:
        logger.info("Starting one-shot mode with args: %s", sys.argv[1:])
        await run_one_shot()
        logger.debug("one-shot finished")
        return

    # Interactive mode
    logger.info("Starting interactive mode")
    await run_interactive()
    logger.debug("interactive finished")

if __name__ == "__main__":
    # Single top-level asyncio.run call with graceful shutdown.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted. Goodbye.")
        logger.info("main: interrupted by user")
    except Exception as e:
        # Final safety net: keep the traceback minimal and user-friendly.
        print("Fatal error:", e)
        logger.exception("Fatal error in main: %s", e)
        # Optionally exit with non-zero code to signal failure in CI/CD contexts.   
        sys.exit(1)
