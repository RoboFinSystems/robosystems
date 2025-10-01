#!/usr/bin/env python3
"""
Kuzu API module entry point.

This module enables running the Kuzu API server or client as a module:
    python -m robosystems.kuzu_api --help                    # Server help
    python -m robosystems.kuzu_api --base-path /data ...     # Start server
    python -m robosystems.kuzu_api cli health                # Client commands
"""

import sys

if __name__ == "__main__":
  # Check if first argument is 'cli' to route to client
  if len(sys.argv) > 1 and sys.argv[1] == "cli":
    # Remove 'cli' from args and run client
    sys.argv.pop(1)
    from robosystems.kuzu_api.cli import main

    exit(main())
  else:
    # Default to server
    from robosystems.kuzu_api.main import main

    exit(main())
