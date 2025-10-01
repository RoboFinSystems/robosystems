#!/usr/bin/env python
# type: ignore
"""
Clear Valkey/Redis queues including priority-encoded variants.

Celery/Kombu adds priority suffixes to queue names that make them hard to find.
This script handles finding and clearing all variants of a queue name.

Usage:
    # Clear a specific queue
    uv run python robosystems/scripts/clear_valkey_queues.py shared-processing

    # Clear multiple queues
    uv run python robosystems/scripts/clear_valkey_queues.py shared-processing shared-ingestion

    # Clear queues and unacked messages
    uv run python robosystems/scripts/clear_valkey_queues.py --clear-unacked shared-processing

    # List queues without clearing
    uv run python robosystems/scripts/clear_valkey_queues.py --list-only shared-processing
"""

import argparse
import sys
from typing import List

import redis
from robosystems.config.valkey_registry import ValkeyDatabase


def get_redis_client() -> redis.Redis:
  """Get Redis client for Celery broker database with authentication in prod/staging."""
  from robosystems.config.valkey_registry import create_redis_client

  # Use the factory method which handles SSL params correctly
  return create_redis_client(ValkeyDatabase.CELERY_BROKER, decode_responses=False)


def find_queue_variants(client: redis.Redis, base_queue_name: str) -> List[bytes]:
  """
  Find all variants of a queue name including priority-encoded versions.

  Common priority suffixes:
  - \x06\x16\x31 - Priority level 1
  - \x06\x16\x32 - Priority level 2
  - \x06\x16\x33 - Priority level 3
  - \x06\x16\x34 - Priority level 4
  - \x06\x16\x35 - Priority level 5
  - \x06\x16\x36 - Priority level 6
  - \x06\x16\x37 - Priority level 7
  - \x06\x16\x38 - Priority level 8
  - \x06\x16\x39 - Priority level 9
  """
  found_queues = []
  base_bytes = base_queue_name.encode()

  # Get all keys that might be our queue
  # Use a pattern that captures both exact and priority-encoded variants
  pattern = f"{base_queue_name}*"
  all_keys = client.keys(pattern.encode())

  for key in all_keys:
    # Filter out kombu binding keys
    if b"_kombu.binding" in key:
      continue

    # Check if this key starts with our base queue name
    if key.startswith(base_bytes):
      # Could be exact match or priority-encoded variant
      # Check it's actually a list (queue) not some other type
      if client.type(key) == b"list":
        found_queues.append(key)

  return found_queues


def find_unacked_variants(client: redis.Redis, base_queue_name: str) -> List[bytes]:
  """Find unacknowledged message keys for a queue."""
  found_unacked = []
  pattern = f"unacked_*{base_queue_name}*".encode()

  # Get all keys matching unacked pattern
  all_keys = client.keys(pattern)
  found_unacked.extend(all_keys)

  # Also check for unacked_index keys
  index_pattern = f"unacked_index_*{base_queue_name}*".encode()
  index_keys = client.keys(index_pattern)
  found_unacked.extend(index_keys)

  return found_unacked


def clear_queue(client: redis.Redis, queue_key: bytes) -> int:
  """Clear a single queue and return the number of messages cleared."""
  # Get queue length before clearing
  queue_len = client.llen(queue_key)

  # Delete the queue
  if queue_len > 0:
    client.delete(queue_key)

  return queue_len


def clear_unacked(client: redis.Redis, unacked_key: bytes) -> int:
  """Clear unacknowledged messages and return the count."""
  key_type = client.type(unacked_key)

  if key_type == b"zset":
    # Sorted set (typical for unacked messages)
    count = client.zcard(unacked_key)
    if count > 0:
      client.delete(unacked_key)
    return count
  elif key_type == b"hash":
    # Hash (for unacked_index)
    count = client.hlen(unacked_key)
    if count > 0:
      client.delete(unacked_key)
    return count
  elif key_type == b"list":
    # List
    count = client.llen(unacked_key)
    if count > 0:
      client.delete(unacked_key)
    return count
  else:
    # Unknown type, just delete it
    client.delete(unacked_key)
    return 1


def format_queue_name(queue_key: bytes) -> str:
  """Format queue name for display, showing priority suffix if present."""
  # Check for Kombu priority encoding pattern \x06\x16{priority}
  if b"\x06\x16" in queue_key:
    # Split at the priority encoding
    base = queue_key[: queue_key.find(b"\x06\x16")]
    suffix = queue_key[queue_key.find(b"\x06\x16") :]

    # Extract priority number (last byte)
    if len(suffix) >= 3:
      priority_byte = suffix[2]  # The byte after \x06\x16
      try:
        # Convert ASCII digit to integer
        priority = int(chr(priority_byte))
        return f"{base.decode('utf-8', errors='ignore')} [priority={priority}]"
      except Exception:
        # Not a valid priority digit, show hex
        return f"{base.decode('utf-8', errors='ignore')} [suffix=\\x{suffix.hex()}]"
    else:
      return f"{base.decode('utf-8', errors='ignore')} [suffix=\\x{suffix.hex()}]"

  # No priority encoding, try simple UTF-8 decode
  try:
    decoded = queue_key.decode("utf-8")
    # Check if it ends with a digit (simplified priority notation)
    if decoded and decoded[-1].isdigit() and not decoded[-2:].isdigit():
      # Might be simplified priority notation like "shared-ingestion9"
      base = decoded[:-1]
      priority = decoded[-1]
      # Only show as priority if base name matches expected pattern
      if base.endswith(("-ingestion", "-processing", "-extraction")):
        return f"{base} [priority={priority}]"
    return decoded
  except UnicodeDecodeError:
    # Fallback to showing raw bytes as hex
    return f"<binary: \\x{queue_key.hex()}>"


def main():
  parser = argparse.ArgumentParser(
    description="Clear Valkey/Redis queues including priority-encoded variants"
  )
  parser.add_argument(
    "queues",
    nargs="+",
    help="Queue names to clear (e.g., shared-processing, shared-ingestion)",
  )
  parser.add_argument(
    "--clear-unacked",
    action="store_true",
    help="Also clear unacknowledged messages",
  )
  parser.add_argument(
    "--list-only",
    action="store_true",
    help="List queues without clearing them",
  )
  parser.add_argument(
    "--verbose",
    "-v",
    action="store_true",
    help="Show verbose output",
  )

  args = parser.parse_args()

  client = get_redis_client()

  total_cleared = 0
  total_unacked_cleared = 0

  for queue_name in args.queues:
    print(f"\n{'=' * 60}")
    print(f"Processing queue: {queue_name}")
    print(f"{'=' * 60}")

    # Find all variants of this queue
    queue_variants = find_queue_variants(client, queue_name)

    if not queue_variants:
      print(f"❌ No queues found matching '{queue_name}'")
      continue

    print(f"✅ Found {len(queue_variants)} queue variant(s):")

    for queue_key in queue_variants:
      formatted_name = format_queue_name(queue_key)
      queue_len = client.llen(queue_key)

      if args.list_only:
        print(f"  • {formatted_name}: {queue_len} messages")
      else:
        if queue_len > 0:
          cleared = clear_queue(client, queue_key)
          total_cleared += cleared
          print(f"  • {formatted_name}: cleared {cleared} messages")
        else:
          print(f"  • {formatted_name}: empty (skipped)")

    # Handle unacknowledged messages if requested
    if args.clear_unacked:
      print("\nSearching for unacknowledged messages...")
      unacked_variants = find_unacked_variants(client, queue_name)

      if unacked_variants:
        print(f"✅ Found {len(unacked_variants)} unacked key(s):")

        for unacked_key in unacked_variants:
          formatted_name = format_queue_name(unacked_key)

          if args.list_only:
            key_type = client.type(unacked_key)
            if key_type == b"zset":
              count = client.zcard(unacked_key)
            elif key_type == b"hash":
              count = client.hlen(unacked_key)
            elif key_type == b"list":
              count = client.llen(unacked_key)
            else:
              count = 1
            print(f"  • {formatted_name}: {count} entries")
          else:
            cleared = clear_unacked(client, unacked_key)
            if cleared > 0:
              total_unacked_cleared += cleared
              print(f"  • {formatted_name}: cleared {cleared} entries")
            else:
              print(f"  • {formatted_name}: empty (skipped)")
      else:
        print("❌ No unacknowledged message keys found")

  # Summary
  if not args.list_only:
    print(f"\n{'=' * 60}")
    print("Summary:")
    print(f"{'=' * 60}")
    print(f"✅ Total messages cleared: {total_cleared}")
    if args.clear_unacked:
      print(f"✅ Total unacked entries cleared: {total_unacked_cleared}")
    print("✅ Queues are now clear!")

  return 0


if __name__ == "__main__":
  sys.exit(main())
