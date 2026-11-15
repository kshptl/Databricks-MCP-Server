# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""`databricks-mcp` command line entry point."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from dataclasses import replace

from databricks_mcp import __version__
from databricks_mcp.config import Settings
from databricks_mcp.server import TOOLSETS, build_server


def _parse_toolsets(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [t.strip() for t in raw.split(",") if t.strip()]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="databricks-mcp", description="MCP server for Databricks.")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command")

    run = sub.add_parser("run", help="Run the MCP server (the default command).")
    run.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default=os.environ.get("DATABRICKS_MCP_TRANSPORT", "stdio"),
        help="stdio for desktop clients (default); streamable-http to serve over HTTP.",
    )
    run.add_argument("--host", default="127.0.0.1", help="Bind address for streamable-http.")
    run.add_argument("--port", type=int, default=8000, help="Port for streamable-http.")
    for p in (run, sub.add_parser("list-tools", help="Print the tools the server would expose.")):
        p.add_argument("--read-only", action="store_true", help="Only expose tools that don't change anything.")
        p.add_argument(
            "--toolsets",
            default=os.environ.get("DATABRICKS_MCP_TOOLSETS"),
            help=f"Comma-separated subset of: {','.join(TOOLSETS)}. Default: all.",
        )
    run.add_argument(
        "--log-level",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=os.environ.get("DATABRICKS_MCP_LOG_LEVEL", "WARNING").upper(),
    )
    return parser


def _settings(args: argparse.Namespace) -> Settings:
    settings = Settings.from_env()
    if args.read_only:
        settings = replace(settings, read_only=True)
    return settings


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    # `run` is the default, so `databricks-mcp --read-only` works in client configs.
    if not argv or argv[0] not in {"run", "list-tools", "-h", "--help", "--version"}:
        argv = ["run", *argv]
    args = _parser().parse_args(argv)

    try:
        server = build_server(
            settings=_settings(args),
            toolsets=_parse_toolsets(args.toolsets),
            log_level=getattr(args, "log_level", "WARNING"),
        )
    except ValueError as exc:
        print(f"databricks-mcp: {exc}", file=sys.stderr)
        return 2

    if args.command == "list-tools":
        for tool in asyncio.run(server.list_tools()):
            ro = " [read-only]" if tool.annotations and tool.annotations.read_only_hint else ""
            print(f"{tool.name}{ro}\n    {(tool.description or '').splitlines()[0]}")
        return 0

    # stdout is the MCP channel under stdio, so logs must go to stderr.
    logging.basicConfig(stream=sys.stderr, level=args.log_level, format="%(levelname)s %(name)s: %(message)s")
    # httpx logs every request URL at INFO; keep that out unless asked for.
    logging.getLogger("httpx").setLevel(max(logging.WARNING, logging.getLevelName(args.log_level)))
    if args.transport == "stdio":
        server.run("stdio")
    else:
        server.run("streamable-http", host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    sys.exit(main())
