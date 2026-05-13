"""Counterparty agents for Cascade Advisory Group LLC.

Each agent dict is the request body for `create-agent`. The
`NAME_TO_EXTERNAL_ID` map lets `data.py` transaction tuples reference
agents by display name (the same way QB transactions reference
customers/vendors by name); `main.py` resolves to the actual
`Agent.id` after creation.

`external_id` is the demo-side stable key used for re-create idempotency:
the `(source='native', external_id)` tuple is unique at the DB level, so
re-running the demo upserts rather than duplicating.

Inventory:
- 6 customers (3 recurring billing relationships + 3 occasional)
- 9 vendors (8 named + 1 government tax authority)
- 2 employees
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------

CUSTOMERS: list[dict] = [
  {
    "agent_type": "customer",
    "name": "TechCorp Inc",
    "legal_name": "TechCorp Incorporated",
    "email": "ap@techcorp.example.com",
    "phone": "(415) 555-2100",
    "address": {
      "line1": "1 Market Street",
      "line2": "Suite 200",
      "city": "San Francisco",
      "state": "CA",
      "postal_code": "94105",
      "country": "USA",
    },
    "source": "native",
    "external_id": "cust-techcorp",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "customer",
    "name": "Global Dynamics LLC",
    "legal_name": "Global Dynamics LLC",
    "email": "billing@globaldynamics.example.com",
    "phone": "(312) 555-8842",
    "address": {
      "line1": "200 W Madison St",
      "city": "Chicago",
      "state": "IL",
      "postal_code": "60606",
      "country": "USA",
    },
    "source": "native",
    "external_id": "cust-globaldynamics",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "customer",
    "name": "Meridian Capital",
    "legal_name": "Meridian Capital Partners, LP",
    "email": "ops@meridiancapital.example.com",
    "phone": "(212) 555-4419",
    "address": {
      "line1": "350 Park Avenue",
      "line2": "12th Floor",
      "city": "New York",
      "state": "NY",
      "postal_code": "10022",
      "country": "USA",
    },
    "source": "native",
    "external_id": "cust-meridian",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "customer",
    "name": "Bayshore Logistics",
    "legal_name": "Bayshore Logistics Co.",
    "email": "finance@bayshore.example.com",
    "phone": "(206) 555-7733",
    "address": {
      "line1": "1500 Alaskan Way",
      "city": "Seattle",
      "state": "WA",
      "postal_code": "98101",
      "country": "USA",
    },
    "source": "native",
    "external_id": "cust-bayshore",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "customer",
    "name": "Apex Manufacturing",
    "legal_name": "Apex Manufacturing, Inc.",
    "email": "ap@apexmfg.example.com",
    "phone": "(513) 555-3300",
    "address": {
      "line1": "4400 Industrial Pkwy",
      "city": "Cincinnati",
      "state": "OH",
      "postal_code": "45241",
      "country": "USA",
    },
    "source": "native",
    "external_id": "cust-apex",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "customer",
    "name": "Sterling Group",
    "legal_name": "Sterling Group, LLC",
    "email": "accounts@sterlinggroup.example.com",
    "phone": "(617) 555-9120",
    "address": {
      "line1": "75 State Street",
      "city": "Boston",
      "state": "MA",
      "postal_code": "02109",
      "country": "USA",
    },
    "source": "native",
    "external_id": "cust-sterling",
    "is_active": True,
    "is_1099_recipient": False,
  },
]

# ---------------------------------------------------------------------------
# Vendors
# ---------------------------------------------------------------------------

VENDORS: list[dict] = [
  {
    "agent_type": "vendor",
    "name": "Cascade Business Center",
    "legal_name": "Cascade Business Center LLC",
    "email": "leasing@cascadebc.example.com",
    "phone": "(503) 555-1100",
    "address": {
      "line1": "100 SW Main St",
      "city": "Portland",
      "state": "OR",
      "postal_code": "97204",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-cascade-bc",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "Herman Miller",
    "legal_name": "MillerKnoll, Inc.",
    "email": "ar@hermanmiller.example.com",
    "phone": "(616) 555-4100",
    "address": {
      "line1": "855 East Main Avenue",
      "city": "Zeeland",
      "state": "MI",
      "postal_code": "49464",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-herman-miller",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "Nationwide Insurance",
    "legal_name": "Nationwide Mutual Insurance Company",
    "email": "billing@nationwide.example.com",
    "phone": "(614) 555-2200",
    "address": {
      "line1": "One Nationwide Plaza",
      "city": "Columbus",
      "state": "OH",
      "postal_code": "43215",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-nationwide",
    "is_active": True,
    "is_1099_recipient": True,
  },
  {
    "agent_type": "vendor",
    "name": "Basecamp",
    "legal_name": "37signals, LLC",
    "email": "billing@basecamp.example.com",
    "phone": "(312) 555-6700",
    "address": {
      "line1": "30 N Racine Ave",
      "line2": "Suite 200",
      "city": "Chicago",
      "state": "IL",
      "postal_code": "60607",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-basecamp",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "Amazon Web Services",
    "legal_name": "Amazon Web Services, Inc.",
    "email": "aws-billing@amazon.example.com",
    "phone": "(206) 555-1000",
    "address": {
      "line1": "410 Terry Avenue North",
      "city": "Seattle",
      "state": "WA",
      "postal_code": "98109",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-aws",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "Amazon",
    "legal_name": "Amazon.com, Inc.",
    "email": "business-billing@amazon.example.com",
    "phone": "(206) 555-1010",
    "address": {
      "line1": "410 Terry Avenue North",
      "city": "Seattle",
      "state": "WA",
      "postal_code": "98109",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-amazon",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "Slack Technologies",
    "legal_name": "Slack Technologies, LLC",
    "email": "billing@slack.example.com",
    "phone": "(415) 555-3400",
    "address": {
      "line1": "500 Howard St",
      "city": "San Francisco",
      "state": "CA",
      "postal_code": "94105",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-slack",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "United Airlines",
    "legal_name": "United Airlines, Inc.",
    "email": "corporate@united.example.com",
    "phone": "(872) 555-8800",
    "address": {
      "line1": "233 S Wacker Dr",
      "city": "Chicago",
      "state": "IL",
      "postal_code": "60606",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-united",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "government",
    "name": "IRS/State",
    "legal_name": "U.S. Internal Revenue Service",
    "phone": "(800) 555-1040",
    "source": "native",
    "external_id": "vendor-irs",
    "is_active": True,
    "is_1099_recipient": False,
  },
]

# ---------------------------------------------------------------------------
# Employees
# ---------------------------------------------------------------------------

EMPLOYEES: list[dict] = [
  {
    "agent_type": "employee",
    "name": "Sarah Chen",
    "legal_name": "Sarah W. Chen",
    "email": "sarah@cascadeadvisory.example.com",
    "phone": "(415) 555-7001",
    "source": "native",
    "external_id": "emp-schen",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "employee",
    "name": "Marcus Reyes",
    "legal_name": "Marcus J. Reyes",
    "email": "marcus@cascadeadvisory.example.com",
    "phone": "(415) 555-7002",
    "source": "native",
    "external_id": "emp-mreyes",
    "is_active": True,
    "is_1099_recipient": False,
  },
]


# ---------------------------------------------------------------------------
# Aggregated lookups
# ---------------------------------------------------------------------------

AGENTS: list[dict] = CUSTOMERS + VENDORS + EMPLOYEES

# Map the human-readable name `data.py` uses on transaction tuples to the
# agent's external_id. Agents not in this map (e.g. "Various") become
# agent_id=NULL on events — a legitimate "miscellaneous" pattern that
# QuickBooks also produces.
NAME_TO_EXTERNAL_ID: dict[str, str] = {
  agent["name"]: agent["external_id"] for agent in AGENTS
}
