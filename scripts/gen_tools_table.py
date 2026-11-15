"""Regenerate the tool tables in README.md from the live server definitions.

Usage: python scripts/gen_tools_table.py
"""

import asyncio
import re
from pathlib import Path

from databricks_mcp.config import Settings
from databricks_mcp.server import TOOLSETS, build_server

TITLES = {
    "clusters": "Clusters",
    "jobs": "Jobs",
    "workspace": "Workspace / notebooks",
    "volumes": "Unity Catalog volumes (Files API)",
    "dbfs": "DBFS (legacy, read-only)",
    "sql": "SQL warehouses & queries",
    "unity_catalog": "Unity Catalog",
    "commands": "Run code on a cluster",
}
START, END = "<!-- tools:start -->", "<!-- tools:end -->"


def render() -> str:
    out = [START, ""]
    for toolset in TOOLSETS:
        tools = asyncio.run(build_server(settings=Settings(), toolsets=[toolset]).list_tools())
        out += [f"**{TITLES[toolset]}** (`{toolset}`)", "", "| Tool | Read-only | What it does |", "|---|:---:|---|"]
        for tool in tools:
            desc = " ".join((tool.description or "").split())
            out.append(f"| `{tool.name}` | {'✓' if tool.annotations.read_only_hint else ''} | {desc} |")
        out.append("")
    out.append(END)
    return "\n".join(out)


def main() -> None:
    readme = Path(__file__).resolve().parent.parent / "README.md"
    text = readme.read_text()
    table = render()
    if START in text:
        text = re.sub(re.escape(START) + ".*?" + re.escape(END), lambda _: table, text, flags=re.S)
    else:
        text = text.replace("TOOLS_TABLE_PLACEHOLDER", table)
    readme.write_text(text)


if __name__ == "__main__":
    main()
