#!/usr/bin/env python3
"""
Graph API module entry point.

This module enables running the Graph API server as a module:
    python -m robosystems.graph_api --help                    # Server help
    python -m robosystems.graph_api --base-path /data ...     # Start server
"""

if __name__ == "__main__":
  from robosystems.graph_api.main import main

  exit(main())
