#!/usr/bin/env python
# type: ignore
"""
SEC Orchestrator - Phase-Based Processing

This script orchestrates complete SEC filing processing with:
- Phase-based execution (download, process, ingest)
- Rate-limited downloads via shared-extraction queue
- Unlimited processing parallelism via shared-processing queue
- Optional company limits for testing
- All state managed in Redis/Valkey
"""

import argparse
import time


def main():
  """Main entry point for SEC orchestration."""

  parser = argparse.ArgumentParser(
    description="SEC Orchestration - Phase-Based Processing",
    formatter_class=argparse.RawDescriptionHelpFormatter,
  )

  subparsers = parser.add_subparsers(dest="command", help="Commands")

  # Plan command
  plan_parser = subparsers.add_parser("plan", help="Plan phased SEC processing")
  plan_parser.add_argument("--start-year", type=int, default=2024, help="Start year")
  plan_parser.add_argument("--end-year", type=int, default=2025, help="End year")
  plan_parser.add_argument(
    "--max-companies", type=int, help="Max companies to process (for testing)"
  )

  # Start phase command
  phase_parser = subparsers.add_parser("start-phase", help="Start a processing phase")
  phase_parser.add_argument(
    "--phase",
    choices=["download", "process", "consolidate", "ingest"],
    required=True,
    help="Phase to start",
  )
  phase_parser.add_argument(
    "--resume", action="store_true", help="Resume from last checkpoint if available"
  )
  phase_parser.add_argument(
    "--retry-failed", action="store_true", help="Include previously failed companies"
  )

  # Status command
  subparsers.add_parser("status", help="Show processing status")

  # Reset command (database reset)
  reset_parser = subparsers.add_parser(
    "reset", help="Reset SEC database (delete all data and recreate)"
  )
  reset_parser.add_argument(
    "--confirm", action="store_true", help="Confirm database reset"
  )

  args = parser.parse_args()

  if not args.command:
    parser.print_help()
    return

  if args.command == "plan":
    # Trigger remote Celery task for planning
    from robosystems.tasks.sec_xbrl.orchestration import plan_phased_processing

    print("🚀 Planning phased SEC processing...")
    print(f"📅 Years: {args.start_year} - {args.end_year}")
    if args.max_companies:
      print(f"🧪 TEST MODE: Limited to {args.max_companies} companies")

    task = plan_phased_processing.apply_async(
      kwargs={
        "start_year": args.start_year,
        "end_year": args.end_year,
        "max_companies": args.max_companies,
      }
    )

    print(f"📋 Task ID: {task.id}")
    print("⏳ Waiting for planning to complete...")

    try:
      result = task.get(timeout=120)

      if result["status"] == "success":
        print("\n✅ Planning completed!")
        print(f"📦 Companies: {result['companies']}")
        print(f"📅 Years: {', '.join(map(str, result['years']))}")
        print("\n🎯 Phases ready:")
        for phase in result["phases"]:
          print(f"  - {phase}")
        print(f"\n{result['message']}")
        print("\n🎆 Next steps:")
        print("  1. Start download: just sec-phase download")
        print("  2. Start process: just sec-phase process")
        print("  3. Consolidate: just sec-phase consolidate")
        print("  4. Ingest: just sec-phase ingest")
        print("\n📊 Check status: just sec-status")
      else:
        print(f"❌ Planning failed: {result.get('error', 'Unknown error')}")

    except Exception as e:
      print(f"❌ Failed to complete planning: {e}")

  elif args.command == "start-phase":
    # Start a specific phase
    from robosystems.tasks.sec_xbrl.orchestration import start_phase

    mode = "Starting"
    if args.resume:
      mode = "Resuming"
    elif args.retry_failed:
      mode = "Retrying failed companies in"

    print(f"🚀 {mode} phase: {args.phase}")

    task = start_phase.apply_async(
      kwargs={
        "phase": args.phase,
        "resume": args.resume,
        "retry_failed": args.retry_failed,
      }
    )

    print(f"📋 Task ID: {task.id}")
    print("⏳ Waiting for phase to start...")

    try:
      result = task.get(timeout=60)

      if result["status"] == "started":
        print(f"\n✅ Phase '{args.phase}' started successfully!")
        if "task_id" in result:
          print(f"📋 Phase task ID: {result['task_id']}")
        if "companies" in result:
          print(f"📦 Processing {result['companies']} companies")
        print("\n📊 Monitor progress: just sec-status")
      elif result["status"] == "failed":
        print(f"❌ Failed to start phase: {result.get('error', 'Unknown error')}")
      else:
        print(f"ℹ️ Phase status: {result}")

    except Exception as e:
      print(f"❌ Failed to start phase: {e}")

  elif args.command == "status":
    # Get status of phased processing
    from robosystems.tasks.sec_xbrl.orchestration import get_phase_status

    print("🔍 Fetching processing status...")

    print("📡 Using direct mode (bastion tunnel detected)...")
    result = get_phase_status(include_failed=True)

    try:
      if result["status"] == "success":
        print("\n📊 SEC Processing Status")
        print("=" * 40)

        # Configuration
        config = result.get("config", {})
        if config:
          print("Configuration:")
          print(f"  Max Companies: {config.get('max_companies') or 'All'}")
          print(f"  Batch Size: {config.get('companies_per_batch', 50)}")

        # Stats
        stats = result.get("stats", {})
        if stats:
          print("\nStatistics:")
          print(f"  Total Companies: {stats.get('total_companies', 0)}")
          print(f"  Total Years: {stats.get('total_years', 0)}")

        # Phases
        phases = result.get("phases", {})
        if phases:
          print("\nPhases:")
          for phase_name, phase_info in phases.items():
            status = phase_info.get("status", "unknown")
            symbol = (
              "✅" if status == "completed" else "🔄" if status == "running" else "⏳"
            )
            print(f"  {symbol} {phase_name}: {status}")
            if phase_info.get("started_at"):
              print(f"      Started: {phase_info['started_at']}")
            if phase_info.get("checkpoint"):
              cp = phase_info["checkpoint"]
              print(f"      Checkpoint: {cp['completed_count']} completed")
            if phase_info.get("failed_count", 0) > 0:
              print(f"      ⚠️ Failed: {phase_info['failed_count']} companies")

        print(f"\nLast Updated: {result.get('last_updated', 'Never')}")

      elif result["status"] == "no_plan":
        print("\n⚠️ No processing plan found")
        print("Run 'just sec-plan' to create a processing plan")
      else:
        print(f"❌ Failed to get status: {result.get('error', 'Unknown error')}")

    except Exception as e:
      print(f"❌ Failed to fetch status: {e}")

  elif args.command == "reset":
    # Reset SEC database
    from robosystems.tasks.sec_xbrl.maintenance import reset_sec_database

    if not args.confirm:
      print("🚨 WARNING: This will completely DELETE and RECREATE the SEC database!")
      print("   ALL data will be lost. This action cannot be undone.")
      response = input("\nType 'DELETE EVERYTHING' to confirm: ")
      if response != "DELETE EVERYTHING":
        print("Cancelled.")
        return
      args.confirm = True

    print("🔄 Starting SEC database reset...")

    task = reset_sec_database.apply_async(kwargs={"confirm": args.confirm})

    print(f"📋 Task ID: {task.id}")
    print("⏳ Waiting for reset to complete (this may take a few minutes)...")

    # Poll for status
    for _ in range(60):  # Max 5 minutes
      try:
        if task.ready():
          result = task.result
          if isinstance(result, dict):
            if result.get("status") == "success":
              print("\n✅ SEC database reset completed successfully!")
              print("📊 Schema verification:")
              print(f"  - Node types: {result.get('node_types', 0)}")
              print(f"  - Relationship types: {result.get('relationship_types', 0)}")
              print("🚀 Database is now empty and ready for fresh data ingestion")
            else:
              print(f"❌ Reset failed: {result.get('error', 'Unknown error')}")
          else:
            print(f"❌ Unexpected result: {result}")
          break
      except Exception as e:
        print(f"❌ Error checking task status: {e}")
        break

      print(".", end="", flush=True)
      from robosystems.config import env

      wait_time = 1 if env.is_development() else 5
      time.sleep(wait_time)
    else:
      print("\n⏰ Timeout waiting for reset to complete")
      print(f"Check task {task.id} status manually")


if __name__ == "__main__":
  main()
