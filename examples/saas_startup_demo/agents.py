"""Counterparty agents for Cadence Labs, Inc.

The eight B2B customer logos (annual subscription + onboarding services), the
operating vendors (hosting, ads, workspace, tooling, insurance), and the
founder/employees. Customer names match ``data.LOGOS`` so the scenario can
reference them by name; ``main.py`` resolves them to Agent ids.
"""

from __future__ import annotations


def _customer(name: str, ext: str, city: str, state: str) -> dict:
  return {
    "agent_type": "customer",
    "name": name,
    "legal_name": name,
    "email": f"ap@{ext}.example.com",
    "address": {"city": city, "state": state, "country": "USA"},
    "source": "native",
    "external_id": f"cust-{ext}",
    "is_active": True,
    "is_1099_recipient": False,
  }


CUSTOMERS: list[dict] = [
  _customer("Northwind Trading", "northwind", "Chicago", "IL"),
  _customer("Atlas Manufacturing", "atlas", "Cleveland", "OH"),
  _customer("Brightline Health", "brightline", "Boston", "MA"),
  _customer("Cobalt Logistics", "cobalt", "Memphis", "TN"),
  _customer("Summit Retail Group", "summitretail", "Minneapolis", "MN"),
  _customer("Vantage Financial", "vantage", "Charlotte", "NC"),
  _customer("Kestrel Media", "kestrel", "Austin", "TX"),
  _customer("Onyx Robotics", "onyx", "Pittsburgh", "PA"),
]

VENDORS: list[dict] = [
  {
    "agent_type": "vendor",
    "name": "Amazon Web Services",
    "legal_name": "Amazon Web Services, Inc.",
    "email": "aws-billing@amazon.example.com",
    "address": {"city": "Seattle", "state": "WA", "country": "USA"},
    "source": "native",
    "external_id": "vendor-aws",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "LinkedIn",
    "legal_name": "LinkedIn Corporation",
    "email": "ads-billing@linkedin.example.com",
    "address": {"city": "Sunnyvale", "state": "CA", "country": "USA"},
    "source": "native",
    "external_id": "vendor-linkedin",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "Flatiron Workspaces",
    "legal_name": "Flatiron Workspaces LLC",
    "email": "billing@flatironworkspaces.example.com",
    "address": {"city": "New York", "state": "NY", "country": "USA"},
    "source": "native",
    "external_id": "vendor-flatiron",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "Datadog",
    "legal_name": "Datadog, Inc.",
    "email": "billing@datadog.example.com",
    "address": {"city": "New York", "state": "NY", "country": "USA"},
    "source": "native",
    "external_id": "vendor-datadog",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "Vouch Insurance",
    "legal_name": "Vouch Insurance Services, LLC",
    "email": "billing@vouch.example.com",
    "address": {"city": "San Francisco", "state": "CA", "country": "USA"},
    "source": "native",
    "external_id": "vendor-vouch",
    "is_active": True,
    "is_1099_recipient": True,
  },
]

EMPLOYEES: list[dict] = [
  {
    "agent_type": "employee",
    "name": "Priya Nair",
    "legal_name": "Priya S. Nair",
    "email": "priya@cadencelabs.example.com",
    "source": "native",
    "external_id": "emp-pnair",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "employee",
    "name": "Sam Whitfield",
    "legal_name": "Samuel R. Whitfield",
    "email": "sam@cadencelabs.example.com",
    "source": "native",
    "external_id": "emp-swhitfield",
    "is_active": True,
    "is_1099_recipient": False,
  },
]


AGENTS: list[dict] = CUSTOMERS + VENDORS + EMPLOYEES

NAME_TO_EXTERNAL_ID: dict[str, str] = {a["name"]: a["external_id"] for a in AGENTS}
