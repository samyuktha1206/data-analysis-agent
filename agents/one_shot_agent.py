# agents/one_shot.py
import json
import asyncio
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from claude_agent_sdk import query, ClaudeAgentOptions, ClaudeSDKClient, AssistantMessage, ToolUseBlock, TextBlock, ToolResultBlock

from tools import get_df, dataAnalysis
from agents.agent import ALLOWED_TOOLS
# small AgentState dataclass (reuse/extend if you already have one)
@dataclass
class AgentState:
    query: str
    validated: bool = False
    validation_payload: Optional[Dict[str, Any]] = None
    tool_results: List[Dict[str, Any]] = None
    assistant_text: Optional[str] = None
    insight: Optional[str] = None

    def to_dict(self):
        return asdict(self)

SYSTEM_PROMPT = """"""

class OneShotSession:
    """Run a single request with prevalidation, in-process Claude client, and structured state."""
    def __init__(self, allowed_tools = ALLOWED_TOOLS, mcp_server = dataAnalysis):
        self.mcp_server = mcp_server

async def main():
    options = ClaudeAgentOptions(
        system_prompt = SYSTEM_PROMPT,
        allowed_tools = ALLOWED_TOOLS,
        mcp_servers = {"dataAnalysis": dataAnalysis}
    )

    async for message in query(options):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, ToolUseBlock):
                    tool_name = block.name
                    tool_input = block.input or {}
                    print(f"{bold('🛠️  Tool used:')} {tool_name}")
                    print(f"{faint('   input:')} {json.dumps(tool_input, ensure_ascii=False)}")
                elif isinstance(block, ToolResultBlock):
                    # Show summarized tool result text if present
                    shown = False
                    if isinstance(block.content, list):
                        for part in block.content:
                            if isinstance(part, dict) and part.get("type") == "text":
                                text = (part.get("text") or "").strip()
                                if text:
                                    preview = text if len(text) <= 200 else (text[:197] + "...")
                                    print(f"{faint('   result:')} {preview}")
                                    shown = True
                                    break
                    if not shown:
                        print(f"{faint('   result:')} (no textual content)")
                elif isinstance(block, TextBlock):
                            print(f"Claude: {block.text}")

if __name__ == "__main__":
    asyncio.run(main())
    
