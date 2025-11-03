# memory_tool.py
import shutil
from pathlib import Path
from typing import List, Optional, Union, Any
from typing_extensions import override

# NOTE: these imports mirror the example you provided.
# They come from the "anthropic" package (beta memory tool types).
# If your package layout is different, adjust imports accordingly.
from anthropic.lib.tools import BetaAbstractMemoryTool
from anthropic.types.beta import (
    BetaMemoryTool20250818ViewCommand,
    BetaMemoryTool20250818CreateCommand,
    BetaMemoryTool20250818DeleteCommand,
    BetaMemoryTool20250818InsertCommand,
    BetaMemoryTool20250818RenameCommand,
    BetaMemoryTool20250818StrReplaceCommand,
)

class LocalFilesystemMemoryTool(BetaAbstractMemoryTool):
    """
    File-system backed implementation of the beta 'memory' tool.

    - All memory paths must start with '/memories'
    - Files are stored under base_path / 'memories'
    - Protects against path traversal
    """

    def __init__(self, base_path: Union[str, Path] = "./memory"):
        super().__init__()
        self.base_path = Path(base_path)
        self.memory_root = self.base_path / "memories"
        self.memory_root.mkdir(parents=True, exist_ok=True)

    def _validate_path(self, path: str) -> Path:
        """
        Validate a memory tool path and return the resolved Path inside memory_root.
        Accepts '/memories' or '/memories/...'
        Raises ValueError on invalid / traversal attempts.
        """
        if not path.startswith("/memories"):
            raise ValueError(f"Path must start with /memories, got: {path}")

        relative_path = path[len("/memories") :].lstrip("/")
        full_path = self.memory_root / relative_path if relative_path else self.memory_root

        try:
            # ensure full_path is inside memory_root
            full_path.resolve().relative_to(self.memory_root.resolve())
        except Exception as e:
            raise ValueError(f"Path {path} would escape /memories directory") from e

        return full_path

    # -----------------------
    # Required command handlers
    # -----------------------
    @override
    def view(self, command: BetaMemoryTool20250818ViewCommand) -> str:
        """
        If path is a directory -> returns a short listing.
        If path is a file -> returns file contents (with optional numbered lines if view_range used).
        """
        full_path = self._validate_path(command.path)

        if full_path.is_dir():
            items: List[str] = []
            for item in sorted(full_path.iterdir()):
                # hide dotfiles by default
                if item.name.startswith("."):
                    continue
                items.append(f"{item.name}/" if item.is_dir() else item.name)
            header = f"Directory: {command.path}"
            if items:
                listing = "\n".join([f"- {it}" for it in items])
                return f"{header}\n{listing}"
            return f"{header}\n(empty)"
        elif full_path.is_file():
            try:
                content = full_path.read_text(encoding="utf-8")
            except Exception as e:
                raise RuntimeError(f"Cannot read file {command.path}: {e}") from e

            lines = content.splitlines()
            view_range = getattr(command, "view_range", None)
            if view_range:
                # view_range might be [start, end]; support -1 as "to end"
                start_line = max(1, view_range[0]) - 1
                end_line = len(lines) if view_range[1] == -1 else min(len(lines), view_range[1])
                sliced = lines[start_line:end_line]
                start_num = start_line + 1
            else:
                sliced = lines
                start_num = 1

            numbered = [f"{i + start_num:4d}: {line}" for i, line in enumerate(sliced)]
            return "\n".join(numbered)
        else:
            raise RuntimeError(f"Path not found: {command.path}")

    @override
    def create(self, command: BetaMemoryTool20250818CreateCommand) -> str:
        """
        Create or overwrite a file with the provided file_text.
        Returns a short success string.
        """
        full_path = self._validate_path(command.path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            full_path.write_text(command.file_text, encoding="utf-8")
        except Exception as e:
            raise RuntimeError(f"Failed to write file {command.path}: {e}") from e
        return f"File created successfully at {command.path}"

    @override
    def str_replace(self, command: BetaMemoryTool20250818StrReplaceCommand) -> str:
        """
        Replace a unique substring in the file. Raises if the old_str is not found or occurs multiple times.
        """
        full_path = self._validate_path(command.path)
        if not full_path.is_file():
            raise FileNotFoundError(f"File not found: {command.path}")

        content = full_path.read_text(encoding="utf-8")
        count = content.count(command.old_str)
        if count == 0:
            raise ValueError(f"Text not found in {command.path}")
        if count > 1:
            raise ValueError(f"Text appears {count} times in {command.path}. Must be unique to replace.")

        new_content = content.replace(command.old_str, command.new_str)
        full_path.write_text(new_content, encoding="utf-8")
        return f"File {command.path} has been edited (str_replace)"

    @override
    def insert(self, command: BetaMemoryTool20250818InsertCommand) -> str:
        """
        Insert text at a specific 0-based line index. If insert_line == len(lines), append.
        """
        full_path = self._validate_path(command.path)
        if not full_path.is_file():
            raise FileNotFoundError(f"File not found: {command.path}")

        lines = full_path.read_text(encoding="utf-8").splitlines()
        insert_line = command.insert_line
        if insert_line < 0 or insert_line > len(lines):
            raise ValueError(f"Invalid insert_line {insert_line}. Must be 0..{len(lines)}")

        lines.insert(insert_line, command.insert_text.rstrip("\n"))
        full_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return f"Text inserted at line {insert_line} in {command.path}"

    @override
    def delete(self, command: BetaMemoryTool20250818DeleteCommand) -> str:
        """
        Delete a file or directory (recursive). Cannot delete the root '/memories'.
        """
        full_path = self._validate_path(command.path)
        if command.path == "/memories":
            raise ValueError("Cannot delete the /memories directory itself")

        if full_path.is_file():
            full_path.unlink()
            return f"File deleted: {command.path}"
        elif full_path.is_dir():
            shutil.rmtree(full_path)
            return f"Directory deleted: {command.path}"
        else:
            raise FileNotFoundError(f"Path not found: {command.path}")

    @override
    def rename(self, command: BetaMemoryTool20250818RenameCommand) -> str:
        """
        Rename/move a path inside the memory tree.
        """
        old_full = self._validate_path(command.old_path)
        new_full = self._validate_path(command.new_path)

        if not old_full.exists():
            raise FileNotFoundError(f"Source path not found: {command.old_path}")
        if new_full.exists():
            raise ValueError(f"Destination already exists: {command.new_path}")

        new_full.parent.mkdir(parents=True, exist_ok=True)
        old_full.rename(new_full)
        return f"Renamed {command.old_path} to {command.new_path}"

    @override
    def clear_all_memory(self) -> str:
        """
        Helper to clear all memories (delete and recreate the memory root).
        """
        if self.memory_root.exists():
            shutil.rmtree(self.memory_root)
        self.memory_root.mkdir(parents=True, exist_ok=True)
        return "All memory cleared"
