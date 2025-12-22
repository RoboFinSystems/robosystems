"""URI utilities for QuickBooks graph entities.

These functions generate consistent URIs for QuickBooks entities
when mapping to the graph database schema.
"""

from robosystems.config import URIConstants


def rl_entity_uri(entity_id: str) -> str:
  """Generate RoboLedger entity URI."""
  return f"{URIConstants.ROBOSYSTEMS_BASE_URI}/api/entity/{entity_id}"


def qb_entity_uri(realm_id: str) -> str:
  """Generate QuickBooks entity URI."""
  return f"{URIConstants.QUICKBOOKS_BASE_URI}/entity/{realm_id}"


def qb_chart_of_accounts_uri(realm_id: str) -> str:
  """Generate QuickBooks chart of accounts URI."""
  return f"{URIConstants.QUICKBOOKS_BASE_URI}/entity/{realm_id}#ChartOfAccounts"


def qb_coa_network_uri(realm_id: str) -> str:
  """Generate QuickBooks COA network URI."""
  return f"{URIConstants.QUICKBOOKS_BASE_URI}/entity/{realm_id}/role/ChartOfAccounts"


def qb_transaction_uri(realm_id: str, tx_type: str, tx_id: str) -> str:
  """Generate QuickBooks transaction URI."""
  tx_type = tx_type.replace(" ", "")
  tx_type = tx_type.replace("(Check)", "Check")
  return f"{URIConstants.QUICKBOOKS_BASE_URI}/entity/{realm_id}/transaction/{tx_type}/{tx_id}"


def qb_line_item_uri(realm_id: str, tx_type: str, tx_id: str, li_num: int) -> str:
  """Generate QuickBooks line item URI."""
  return f"{qb_transaction_uri(realm_id, tx_type, tx_id)}/line-item/{li_num}"


def qb_element_uri(realm_id: str, element_name: str) -> str:
  """Generate QuickBooks element URI."""
  acct_name = qb_stripped_account_name(element_name)
  return f"{URIConstants.QUICKBOOKS_BASE_URI}/entity/{realm_id}/element#{acct_name}"


def qb_stripped_account_name(acct_name: str) -> str:
  """Strip and normalize account name for URI generation."""
  acct_name = acct_name.title()
  acct_name = acct_name.replace(" ", "")
  acct_name = acct_name.split("(")[0].strip()
  symbol_list = [
    "&",
    "(",
    ")",
    ".",
    ",",
    ":",
    ";",
    "!",
    "?",
    "/",
    "\\",
    "|",
    "+",
    "=",
    "*",
    "@",
    "#",
    "$",
    "%",
    "^",
    "<",
    ">",
    "~",
    "`",
  ]
  for symbol in symbol_list:
    acct_name = acct_name.replace(symbol, "")
  return acct_name


def rl_coa_root_element_uri() -> tuple[str, str]:
  """Generate RoboLedger COA root element URI and qname."""
  qname = "ChartOfAccountsAbstract"
  uri = f"{URIConstants.ROBOSYSTEMS_BASE_URI}/taxonomy#{qname}"
  return uri, qname
