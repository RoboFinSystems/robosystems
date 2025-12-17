"""URI generation utilities for Plaid graph entities.

These utilities generate consistent URIs and QNames for Plaid data
when stored in the knowledge graph.
"""

import re


def plaid_account_element_uri(account_id: str) -> str:
  """Generate URI for Plaid account element.

  Args:
      account_id: The Plaid account ID

  Returns:
      URI string for the account element
  """
  return f"https://plaid.com/account/{account_id}#element"


def plaid_account_qname(name: str, account_type: str, subtype: str) -> str:
  """Generate QName for Plaid account element.

  Args:
      name: Account name
      account_type: Plaid account type (depository, credit, etc.)
      subtype: Plaid account subtype (checking, savings, etc.)

  Returns:
      Clean QName string
  """
  # Remove special characters from name
  clean_name = re.sub(r"[^\w]", "", name)

  # Include account type and subtype for uniqueness
  if subtype:
    return f"{clean_name}_{account_type}_{subtype}".replace(" ", "")
  else:
    return f"{clean_name}_{account_type}".replace(" ", "")


def plaid_transaction_uri(transaction_id: str) -> str:
  """Generate URI for Plaid transaction.

  Args:
      transaction_id: The Plaid transaction ID

  Returns:
      URI string for the transaction
  """
  return f"https://plaid.com/transaction/{transaction_id}"


def plaid_line_item_uri(transaction_id: str, line_number: int) -> str:
  """Generate URI for Plaid transaction line item.

  Args:
      transaction_id: The Plaid transaction ID
      line_number: Line item number within the transaction

  Returns:
      URI string for the line item
  """
  return f"{plaid_transaction_uri(transaction_id)}/line-item/{line_number}"
