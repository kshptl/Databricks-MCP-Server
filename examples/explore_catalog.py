"""Call the MCP server the way an MCP client would, from Python.

Needs DATABRICKS_HOST and DATABRICKS_TOKEN (or any other Databricks auth) in your env.

    pip install databricks-mcp-toolkit
    python examples/explore_catalog.py
"""

import asyncio
import json

from mcp import Client

from databricks_mcp.config import Settings
from databricks_mcp.server import build_server


async def main() -> None:
    server = build_server(settings=Settings(read_only=True), toolsets=["unity_catalog"])
    async with Client(server) as client:
        catalogs = json.loads((await client.call_tool("list_catalogs", {"max_results": 5})).content[0].text)
        for catalog in catalogs.get("catalogs", []):
            print(catalog["name"])
            schemas = await client.call_tool("list_schemas", {"catalog_name": catalog["name"], "max_results": 5})
            for schema in json.loads(schemas.content[0].text).get("schemas", []):
                print("  ", schema["name"])


if __name__ == "__main__":
    asyncio.run(main())
