"""MCP server entry point for wrds-mcp."""

import logging

from dotenv import load_dotenv
from fastmcp import FastMCP

from wrds_mcp.db.connection import wrds_lifespan
from wrds_mcp.tools.bonds import bonds_mcp
from wrds_mcp.tools.ratings import ratings_mcp
from wrds_mcp.tools.financials import financials_mcp

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = FastMCP(
    "wrds-mcp",
    lifespan=wrds_lifespan,
    on_duplicate_tools="error",
)

mcp.mount(bonds_mcp)
mcp.mount(ratings_mcp)
mcp.mount(financials_mcp)


def main():
    """CLI entry point."""
    mcp.run()


if __name__ == "__main__":
    main()
