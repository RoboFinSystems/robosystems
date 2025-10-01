"""Tests for Schedule operations with Kuzu graph database integration.

Since the CreateSchedule engine is not yet implemented, these tests focus on
testing schedule-related graph database operations and data structures that
would be used by a future schedule engine implementation.
"""

import pytest


@pytest.fixture
def schedule_test_data(kuzu_repository_with_schema):
  """Create test data for schedule-related operations."""
  # Create schedule-specific schema for financial scheduling operations
  kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE ScheduleEntity(
        identifier STRING,
        name STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

  kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE ScheduleProcess(
        identifier STRING,
        name STRING,
        type STRING,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

  kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE ScheduleTransaction(
        identifier STRING,
        name STRING,
        account STRING,
        amount DOUBLE,
        start_date STRING,
        number_of_months INT64,
        monthly_amount DOUBLE,
        type STRING,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

  kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE ScheduleEntry(
        identifier STRING,
        period_date STRING,
        amount DOUBLE,
        description STRING,
        account STRING,
        status STRING,
        created_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

  # Create relationship tables
  kuzu_repository_with_schema.execute_query("""
      CREATE REL TABLE ENTITY_HAS_SCHEDULE_PROCESS(FROM ScheduleEntity TO ScheduleProcess)
    """)

  kuzu_repository_with_schema.execute_query("""
      CREATE REL TABLE PROCESS_HAS_SCHEDULE_TRANSACTION(FROM ScheduleProcess TO ScheduleTransaction)
    """)

  kuzu_repository_with_schema.execute_query("""
      CREATE REL TABLE TRANSACTION_HAS_SCHEDULE_ENTRY(FROM ScheduleTransaction TO ScheduleEntry)
    """)

  # Insert test data
  entity_data = {
    "identifier": "test-entity-123",
    "name": "Test Schedule Entity",
    "created_at": "2023-01-01 00:00:00",
    "updated_at": "2023-01-01 00:00:00",
  }

  kuzu_repository_with_schema.execute_single(
    """
      CREATE (c:ScheduleEntity {
        identifier: $identifier,
        name: $name,
        created_at: timestamp($created_at),
        updated_at: timestamp($updated_at)
      }) RETURN c
    """,
    entity_data,
  )

  process_data = {
    "identifier": "proc-123",
    "name": "Prepaid Expense Process",
    "type": "Prepaid Expense",
    "status": "active",
    "created_at": "2023-01-01 00:00:00",
    "updated_at": "2023-01-01 00:00:00",
  }

  kuzu_repository_with_schema.execute_single(
    """
      CREATE (p:ScheduleProcess {
        identifier: $identifier,
        name: $name,
        type: $type,
        status: $status,
        created_at: timestamp($created_at),
        updated_at: timestamp($updated_at)
      }) RETURN p
    """,
    process_data,
  )

  transaction_data = {
    "identifier": "trans-456",
    "name": "Annual Insurance Premium",
    "account": "Prepaid Insurance",
    "amount": 1200.00,
    "start_date": "2023-01-01",
    "number_of_months": 12,
    "monthly_amount": 100.00,
    "type": "prepaid_expense",
    "status": "pending",
    "created_at": "2023-01-01 00:00:00",
    "updated_at": "2023-01-01 00:00:00",
  }

  kuzu_repository_with_schema.execute_single(
    """
      CREATE (t:ScheduleTransaction {
        identifier: $identifier,
        name: $name,
        account: $account,
        amount: $amount,
        start_date: $start_date,
        number_of_months: $number_of_months,
        monthly_amount: $monthly_amount,
        type: $type,
        status: $status,
        created_at: timestamp($created_at),
        updated_at: timestamp($updated_at)
      }) RETURN t
    """,
    transaction_data,
  )

  # Create relationships
  kuzu_repository_with_schema.execute_single(
    """
      MATCH (c:ScheduleEntity {identifier: $entity_id})
      MATCH (p:ScheduleProcess {identifier: $process_id})
      CREATE (c)-[:ENTITY_HAS_SCHEDULE_PROCESS]->(p)
      RETURN c, p
    """,
    {"entity_id": "test-entity-123", "process_id": "proc-123"},
  )

  kuzu_repository_with_schema.execute_single(
    """
      MATCH (p:ScheduleProcess {identifier: $process_id})
      MATCH (t:ScheduleTransaction {identifier: $transaction_id})
      CREATE (p)-[:PROCESS_HAS_SCHEDULE_TRANSACTION]->(t)
      RETURN p, t
    """,
    {"process_id": "proc-123", "transaction_id": "trans-456"},
  )

  return {
    "entity_id": "test-entity-123",
    "process_id": "proc-123",
    "transaction_id": "trans-456",
  }


class TestScheduleDataStructures:
  """Test schedule-related data structures and operations in graph database."""

  def test_schedule_entity_process_relationship(
    self, schedule_test_data, kuzu_repository_with_schema
  ):
    """Test entity-process relationship for scheduling."""
    # Verify entity-process relationship
    result = kuzu_repository_with_schema.execute_single(
      """
          MATCH (c:ScheduleEntity)-[:ENTITY_HAS_SCHEDULE_PROCESS]->(p:ScheduleProcess)
          WHERE c.identifier = $entity_id AND p.identifier = $process_id
          RETURN c.name as entity_name, p.type as process_type, p.status as process_status
        """,
      {
        "entity_id": schedule_test_data["entity_id"],
        "process_id": schedule_test_data["process_id"],
      },
    )

    assert result is not None
    assert result["entity_name"] == "Test Schedule Entity"
    assert result["process_type"] == "Prepaid Expense"
    assert result["process_status"] == "active"

  def test_schedule_transaction_details(
    self, schedule_test_data, kuzu_repository_with_schema
  ):
    """Test schedule transaction data validation."""
    # Verify transaction data structure
    result = kuzu_repository_with_schema.execute_single(
      """
          MATCH (t:ScheduleTransaction {identifier: $transaction_id})
          RETURN t.name as name,
                 t.account as account,
                 t.amount as total_amount,
                 t.number_of_months as months,
                 t.monthly_amount as monthly_amount,
                 t.start_date as start_date
        """,
      {"transaction_id": schedule_test_data["transaction_id"]},
    )

    assert result is not None
    assert result["name"] == "Annual Insurance Premium"
    assert result["account"] == "Prepaid Insurance"
    assert result["total_amount"] == 1200.00
    assert result["months"] == 12
    assert result["monthly_amount"] == 100.00
    assert result["start_date"] == "2023-01-01"

  def test_schedule_calculation_validation(
    self, schedule_test_data, kuzu_repository_with_schema
  ):
    """Test schedule calculation logic validation."""
    # Test that monthly amount correctly divides total amount
    result = kuzu_repository_with_schema.execute_single(
      """
          MATCH (t:ScheduleTransaction {identifier: $transaction_id})
          RETURN t.amount as total_amount,
                 t.monthly_amount as monthly_amount,
                 t.number_of_months as months,
                 (t.amount / t.number_of_months) as calculated_monthly,
                 (t.monthly_amount * t.number_of_months) as calculated_total
        """,
      {"transaction_id": schedule_test_data["transaction_id"]},
    )

    assert result is not None
    assert result["total_amount"] == 1200.00
    assert result["monthly_amount"] == 100.00
    assert result["months"] == 12
    assert result["calculated_monthly"] == 100.00
    assert result["calculated_total"] == 1200.00

  def test_schedule_entry_generation(
    self, schedule_test_data, kuzu_repository_with_schema
  ):
    """Test generation of individual schedule entries."""
    # Generate monthly schedule entries for the transaction
    months = [
      "2023-01-01",
      "2023-02-01",
      "2023-03-01",
      "2023-04-01",
      "2023-05-01",
      "2023-06-01",
      "2023-07-01",
      "2023-08-01",
      "2023-09-01",
      "2023-10-01",
      "2023-11-01",
      "2023-12-01",
    ]

    for i, month in enumerate(months):
      entry_data = {
        "identifier": f"entry-{i + 1}",
        "period_date": month,
        "amount": 100.00,
        "description": f"Insurance expense - {month}",
        "account": "Insurance Expense",
        "status": "scheduled",
        "created_at": "2023-01-01 00:00:00",
      }

      kuzu_repository_with_schema.execute_single(
        """
              CREATE (e:ScheduleEntry {
                identifier: $identifier,
                period_date: $period_date,
                amount: $amount,
                description: $description,
                account: $account,
                status: $status,
                created_at: timestamp($created_at)
              }) RETURN e
            """,
        entry_data,
      )

      # Link entry to transaction
      kuzu_repository_with_schema.execute_single(
        """
              MATCH (t:ScheduleTransaction {identifier: $transaction_id})
              MATCH (e:ScheduleEntry {identifier: $entry_id})
              CREATE (t)-[:TRANSACTION_HAS_SCHEDULE_ENTRY]->(e)
              RETURN t, e
            """,
        {
          "transaction_id": schedule_test_data["transaction_id"],
          "entry_id": f"entry-{i + 1}",
        },
      )

    # Verify all entries were created and linked
    result = kuzu_repository_with_schema.execute_single(
      """
          MATCH (t:ScheduleTransaction {identifier: $transaction_id})
                -[:TRANSACTION_HAS_SCHEDULE_ENTRY]->(e:ScheduleEntry)
          RETURN count(e) as entry_count, sum(e.amount) as total_amount
        """,
      {"transaction_id": schedule_test_data["transaction_id"]},
    )

    assert result is not None
    assert result["entry_count"] == 12
    assert result["total_amount"] == 1200.00

  def test_schedule_process_types(
    self, schedule_test_data, kuzu_repository_with_schema
  ):
    """Test different types of schedule processes."""
    process_types = [
      ("Accrued Expense", "accrued"),
      ("Deferred Revenue", "deferred"),
      ("Depreciation", "depreciation"),
      ("Amortization", "amortization"),
    ]

    for i, (process_type, type_code) in enumerate(process_types, 1):
      process_data = {
        "identifier": f"proc-type-{i}",
        "name": f"{process_type} Process",
        "type": process_type,
        "status": "active",
        "created_at": "2023-01-01 00:00:00",
        "updated_at": "2023-01-01 00:00:00",
      }

      kuzu_repository_with_schema.execute_single(
        """
              CREATE (p:ScheduleProcess {
                identifier: $identifier,
                name: $name,
                type: $type,
                status: $status,
                created_at: timestamp($created_at),
                updated_at: timestamp($updated_at)
              }) RETURN p
            """,
        process_data,
      )

    # Test process type query (even more simplified)
    result = kuzu_repository_with_schema.execute_query("""
          MATCH (p:ScheduleProcess)
          RETURN p.type as process_type
          ORDER BY p.type
        """)

    # Should have 5 processes now (1 original + 4 new)
    assert len(result) == 5
    expected_types = [
      "Accrued Expense",
      "Amortization",
      "Deferred Revenue",
      "Depreciation",
      "Prepaid Expense",
    ]
    actual_types = [r["process_type"] for r in result]
    assert actual_types == expected_types

  def test_schedule_status_workflow(
    self, schedule_test_data, kuzu_repository_with_schema
  ):
    """Test schedule status workflow and state transitions."""
    # Create additional entries with different statuses
    statuses = ["scheduled", "processing", "completed", "cancelled"]

    for i, status in enumerate(statuses, 1):
      entry_data = {
        "identifier": f"status-entry-{i}",
        "period_date": f"2023-{i:02d}-15",
        "amount": 50.00,
        "description": f"Entry with {status} status",
        "account": "Test Account",
        "status": status,
        "created_at": "2023-01-01 00:00:00",
      }

      kuzu_repository_with_schema.execute_single(
        """
              CREATE (e:ScheduleEntry {
                identifier: $identifier,
                period_date: $period_date,
                amount: $amount,
                description: $description,
                account: $account,
                status: $status,
                created_at: timestamp($created_at)
              }) RETURN e
            """,
        entry_data,
      )

    # Test status-based queries
    for status in statuses:
      result = kuzu_repository_with_schema.execute_single(
        """
              MATCH (e:ScheduleEntry)
              WHERE e.status = $status
              RETURN count(e) as count
            """,
        {"status": status},
      )

      assert result is not None
      assert result["count"] >= 1

  def test_schedule_account_aggregation(
    self, schedule_test_data, kuzu_repository_with_schema
  ):
    """Test account-based aggregation for schedule reporting."""
    # Create entries for different accounts
    accounts = [
      ("Insurance Expense", 100.00),
      ("Rent Expense", 2000.00),
      ("Software Licenses", 500.00),
      ("Professional Services", 750.00),
    ]

    for i, (account, amount) in enumerate(accounts, 1):
      entry_data = {
        "identifier": f"account-entry-{i}",
        "period_date": "2023-01-01",
        "amount": amount,
        "description": f"Monthly {account}",
        "account": account,
        "status": "scheduled",
        "created_at": "2023-01-01 00:00:00",
      }

      kuzu_repository_with_schema.execute_single(
        """
              CREATE (e:ScheduleEntry {
                identifier: $identifier,
                period_date: $period_date,
                amount: $amount,
                description: $description,
                account: $account,
                status: $status,
                created_at: timestamp($created_at)
              }) RETURN e
            """,
        entry_data,
      )

    # Test account aggregation
    result = kuzu_repository_with_schema.execute_query("""
          MATCH (e:ScheduleEntry)
          WHERE e.period_date = '2023-01-01' AND e.identifier STARTS WITH 'account-entry-'
          RETURN e.account as account, e.amount as total_amount
          ORDER BY e.account
        """)

    assert len(result) == 4
    expected_accounts = [
      "Insurance Expense",
      "Professional Services",
      "Rent Expense",
      "Software Licenses",
    ]
    actual_accounts = [r["account"] for r in result]
    assert actual_accounts == expected_accounts

    # Verify amounts (since we're not aggregating, each account appears once)
    account_totals = {r["account"]: r["total_amount"] for r in result}
    assert account_totals["Insurance Expense"] == 100.00
    assert account_totals["Rent Expense"] == 2000.00
    assert account_totals["Software Licenses"] == 500.00
    assert account_totals["Professional Services"] == 750.00

  def test_schedule_period_analysis(
    self, schedule_test_data, kuzu_repository_with_schema
  ):
    """Test period-based schedule analysis."""
    # Create entries across multiple periods
    periods = ["2023-01-01", "2023-02-01", "2023-03-01", "2023-04-01"]
    base_amount = 200.00

    for i, period in enumerate(periods):
      entry_data = {
        "identifier": f"period-entry-{i + 1}",
        "period_date": period,
        "amount": base_amount * (i + 1),  # Increasing amounts
        "description": f"Period analysis entry for {period}",
        "account": "Analysis Account",
        "status": "scheduled",
        "created_at": "2023-01-01 00:00:00",
      }

      kuzu_repository_with_schema.execute_single(
        """
              CREATE (e:ScheduleEntry {
                identifier: $identifier,
                period_date: $period_date,
                amount: $amount,
                description: $description,
                account: $account,
                status: $status,
                created_at: timestamp($created_at)
              }) RETURN e
            """,
        entry_data,
      )

    # Test period-based retrieval (simplified without aggregation)
    result = kuzu_repository_with_schema.execute_query("""
          MATCH (e:ScheduleEntry)
          WHERE e.identifier STARTS WITH 'period-entry-'
          RETURN e.period_date as period, e.amount as period_total
          ORDER BY e.period_date
        """)

    assert len(result) == 4
    expected_totals = [200.00, 400.00, 600.00, 800.00]
    actual_totals = [r["period_total"] for r in result]
    assert actual_totals == expected_totals

    # Test count of entries (simplified)
    count_result = kuzu_repository_with_schema.execute_query("""
          MATCH (e:ScheduleEntry)
          WHERE e.identifier STARTS WITH 'period-entry-'
          RETURN e.identifier as entry_id
        """)

    assert len(count_result) == 4  # 4 entries created

    # Verify all expected amounts are present
    all_amounts = [r["period_total"] for r in result]
    assert sum(all_amounts) == 2000.00  # 200 + 400 + 600 + 800


if __name__ == "__main__":
  pytest.main([__file__, "-v"])
