"""Counterparty agents for Driftline Coffee Roasters.

Each dict is a ``create-agent`` request body, referenced from the scenario by
display name and resolved to an ``Agent.id`` during the run. The cast is chosen
so the working-capital reveal traces to specific counterparties:

- **Summit Markets** — the regional grocery chain that signs mid-window and
  then slips net-30 → net-90. The concentrated, aged wholesale AR traces here.
- **Andean Green Coffee Importers** — the green-coffee supplier; the inventory
  pre-buy traces to its ``bill_received`` events.
- DTC subscriber/order aggregates, a few regional café accounts, a 3PL, and
  the usual operating vendors round out the books.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------

CUSTOMERS: list[dict] = [
  {
    "agent_type": "customer",
    "name": "Driftline Subscribers",
    "legal_name": "Driftline DTC Subscription Members",
    "email": "subscriptions@driftlinecoffee.example.com",
    "source": "native",
    "external_id": "cust-dtc-subs",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "customer",
    "name": "Driftline Web Orders",
    "legal_name": "Driftline DTC One-Time Web Orders",
    "email": "orders@driftlinecoffee.example.com",
    "source": "native",
    "external_id": "cust-dtc-orders",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "customer",
    "name": "Summit Markets",
    "legal_name": "Summit Markets Holdings, Inc.",
    "email": "ap@summitmarkets.example.com",
    "phone": "(206) 555-9000",
    "address": {
      "line1": "4000 Airport Way S",
      "city": "Seattle",
      "state": "WA",
      "postal_code": "98108",
      "country": "USA",
    },
    "source": "native",
    "external_id": "cust-summit",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "customer",
    "name": "Pioneer Square Cafés",
    "legal_name": "Pioneer Square Cafés LLC",
    "email": "orders@pioneersquarecafes.example.com",
    "phone": "(206) 555-3120",
    "address": {
      "line1": "100 Yesler Way",
      "city": "Seattle",
      "state": "WA",
      "postal_code": "98104",
      "country": "USA",
    },
    "source": "native",
    "external_id": "cust-pioneer",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "customer",
    "name": "Emerald City Grocers",
    "legal_name": "Emerald City Grocers Co-op",
    "email": "purchasing@emeraldcitygrocers.example.com",
    "phone": "(425) 555-6611",
    "address": {
      "line1": "8200 164th Ave NE",
      "city": "Redmond",
      "state": "WA",
      "postal_code": "98052",
      "country": "USA",
    },
    "source": "native",
    "external_id": "cust-emerald",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "customer",
    "name": "Cascadia Coffee Bars",
    "legal_name": "Cascadia Coffee Bars, Inc.",
    "email": "ap@cascadiacoffeebars.example.com",
    "phone": "(503) 555-7740",
    "address": {
      "line1": "1200 NW Lovejoy St",
      "city": "Portland",
      "state": "OR",
      "postal_code": "97209",
      "country": "USA",
    },
    "source": "native",
    "external_id": "cust-cascadia",
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
    "name": "Andean Green Coffee Importers",
    "legal_name": "Andean Green Coffee Importers, LLC",
    "email": "sales@andeangreen.example.com",
    "phone": "(305) 555-2200",
    "address": {
      "line1": "1900 NW 22nd St",
      "city": "Miami",
      "state": "FL",
      "postal_code": "33142",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-andean",
    "is_active": True,
    "is_1099_recipient": True,
  },
  {
    "agent_type": "vendor",
    "name": "Shipwise Fulfillment",
    "legal_name": "Shipwise Fulfillment Services, Inc.",
    "email": "billing@shipwise.example.com",
    "phone": "(253) 555-4400",
    "address": {
      "line1": "2600 Port of Tacoma Rd",
      "city": "Tacoma",
      "state": "WA",
      "postal_code": "98421",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-shipwise",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "Ballard Industrial Properties",
    "legal_name": "Ballard Industrial Properties LLC",
    "email": "leasing@ballardindustrial.example.com",
    "phone": "(206) 555-1180",
    "address": {
      "line1": "5300 Shilshole Ave NW",
      "city": "Seattle",
      "state": "WA",
      "postal_code": "98107",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-ballard",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "Probat Roasters",
    "legal_name": "Probat-Werke North America, Inc.",
    "email": "ar@probat.example.com",
    "phone": "(704) 555-9090",
    "address": {
      "line1": "1390 Salem Industrial Dr",
      "city": "Winston-Salem",
      "state": "NC",
      "postal_code": "27127",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-probat",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "Meta Platforms",
    "legal_name": "Meta Platforms, Inc.",
    "email": "ads-billing@meta.example.com",
    "phone": "(650) 555-1500",
    "address": {
      "line1": "1 Hacker Way",
      "city": "Menlo Park",
      "state": "CA",
      "postal_code": "94025",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-meta",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "vendor",
    "name": "Shopify",
    "legal_name": "Shopify (USA) Inc.",
    "email": "billing@shopify.example.com",
    "phone": "(888) 555-7676",
    "address": {
      "line1": "33 New Montgomery St",
      "city": "San Francisco",
      "state": "CA",
      "postal_code": "94105",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-shopify",
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
    "name": "Pacific Power & Light",
    "legal_name": "Pacific Power & Light Company",
    "email": "billing@pacificpower.example.com",
    "phone": "(503) 555-3030",
    "address": {
      "line1": "825 NE Multnomah St",
      "city": "Portland",
      "state": "OR",
      "postal_code": "97232",
      "country": "USA",
    },
    "source": "native",
    "external_id": "vendor-pacificpower",
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
    "name": "Dana Okafor",
    "legal_name": "Dana A. Okafor",
    "email": "dana@driftlinecoffee.example.com",
    "phone": "(206) 555-7011",
    "source": "native",
    "external_id": "emp-dokafor",
    "is_active": True,
    "is_1099_recipient": False,
  },
  {
    "agent_type": "employee",
    "name": "Theo Marsh",
    "legal_name": "Theodore R. Marsh",
    "email": "theo@driftlinecoffee.example.com",
    "phone": "(206) 555-7012",
    "source": "native",
    "external_id": "emp-tmarsh",
    "is_active": True,
    "is_1099_recipient": False,
  },
]


# ---------------------------------------------------------------------------
# Aggregated lookups
# ---------------------------------------------------------------------------

AGENTS: list[dict] = CUSTOMERS + VENDORS + EMPLOYEES

NAME_TO_EXTERNAL_ID: dict[str, str] = {
  agent["name"]: agent["external_id"] for agent in AGENTS
}
