import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
from robosystems.utils import generate_deterministic_uuid7, generate_uuid7
from robosystems.config import PrefixConstants, URIConstants
from robosystems.logger import logger
from robosystems.middleware.graph import get_graph_repository
from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils


class TrialBalanceProcessor:
  def __init__(self, entity_id, date=None, database_name=None):
    self.entity_id = entity_id
    self.database_name = database_name or MultiTenantUtils.get_database_name(entity_id)
    self.repository = get_graph_repository(self.database_name)
    MultiTenantUtils.log_database_operation(
      "Trial balance initialization", self.database_name, entity_id
    )

    # Get entity information from Kuzu (formerly entity)
    entity_query = "MATCH (e:Entity) WHERE e.identifier = $entity_id RETURN e"
    entity_result = self.repository.execute_query(
      entity_query, {"entity_id": entity_id}
    )
    if not entity_result:
      raise Exception(f"Entity not found: {entity_id}")

    self.entity_node = entity_result[0]["e"]
    self.co_node = self.entity_node  # Backward compatibility

    if date is None:
      self.date = dt.datetime.now().strftime("%Y-%m-%d")
    else:
      self.date = date

    logger.debug(
      f"Trial balance processor initialized with database: {self.database_name}"
    )

  def generate(self):
    """Generate trial balance using Kuzu operations."""
    logger.info(
      f"Generating trial balance for entity: {self.entity_id} as of {self.date}"
    )

    self.reset_tb_report()
    self.get_coa_df()
    self.get_all_transactions_df()
    self.get_full_trial_balance()
    self.get_retained_earnings()
    self.get_current_year_trial_balance()
    self.make_tb_report()

    logger.info("Trial balance generation completed")

  async def generate_async(self):
    """Async version of generate method for use in async contexts."""
    logger.info(
      f"Generating trial balance async for entity: {self.entity_id} as of {self.date}"
    )

    # Note: Kuzu operations are synchronous, so we just call the sync method
    self.reset_tb_report()
    self.get_coa_df()
    self.get_all_transactions_df()
    self.get_full_trial_balance()
    self.get_retained_earnings()
    self.get_current_year_trial_balance()
    self.make_tb_report()

    logger.info("Async trial balance generation completed")

  def get_coa_df(self):
    query = """
    MATCH (e:Element)-[:HAS_TO_ELEMENT]-(a:Association)--(s:Structure)-[:HAS_TAXONOMY]-(t:Taxonomy)--(entity:Entity)
    WHERE entity.identifier = $entity_id AND s.type = $structure_type
    RETURN e.qname as qname,e.balance as balance,e.period_type as period_type,a.sequence as sequence
    ORDER BY a.sequence
    """
    params = dict(entity_id=self.entity_id, structure_type="ChartOfAccounts")
    results = self.repository.execute_query(query, params)

    # Convert to DataFrame
    coa_df = pd.DataFrame(results)
    self.coa_df = coa_df
    return coa_df

  def get_all_transactions_df(self):
    query = """
    MATCH (entity:Entity)-[:ENTITY_HAS_TRANSACTION]-(t:Transaction)-[:HAS_LINE_ITEM]-(l:LineItem)-[:LINE_ITEM_HAS_ELEMENT]-(e:Element)
    WHERE entity.identifier = $entity_id
    RETURN t.date as date, t.identifier as identifier,l.debit_amount as debit,l.credit_amount as credit,e.qname as qname
    ORDER BY t.date, t.identifier
    """
    params = dict(entity_id=self.entity_id)
    results = self.repository.execute_query(query, params)

    # Convert to DataFrame
    trans_df = pd.DataFrame(results)

    # Only process date column if DataFrame is not empty
    if not trans_df.empty and "date" in trans_df.columns:
      trans_df["date"] = pd.to_datetime(trans_df["date"])
      trans_df = trans_df[trans_df["date"] <= self.date]

    self.trans_df = trans_df
    return trans_df

  def get_full_trial_balance(self):
    bal_df = self.trans_df.groupby(["qname"]).sum(["debit", "credit"])
    bal_df.reset_index(inplace=True)

    trial_balance_df = pd.merge(self.coa_df, bal_df, on="qname", how="left")
    trial_balance_df[["debit", "credit"]] = trial_balance_df[
      ["debit", "credit"]
    ].fillna(0)

    tb_summary = trial_balance_df.copy()
    for i, r in tb_summary.iterrows():
      if r["balance"] == "debit":
        tb_summary.loc[i, "debit"] = r["debit"] - r["credit"]
        tb_summary.loc[i, "credit"] = 0
      elif r["balance"] == "credit":
        tb_summary.loc[i, "debit"] = 0
        tb_summary.loc[i, "credit"] = r["credit"] - r["debit"]

      if r["balance"] == "debit" and tb_summary.loc[i, "debit"] < 0:
        tb_summary.loc[i, "credit"] = -tb_summary.loc[i, "debit"]
        tb_summary.loc[i, "debit"] = 0
      elif r["balance"] == "credit" and tb_summary.loc[i, "credit"] < 0:
        tb_summary.loc[i, "debit"] = -tb_summary.loc[i, "credit"]
        tb_summary.loc[i, "credit"] = 0

    tb_summary = tb_summary[tb_summary["period_type"] == "instant"]
    self.tb_summary = tb_summary

  def get_retained_earnings(self):
    tb_date = dt.datetime.strptime(self.date, "%Y-%m-%d")
    self.last_day_prior_year = tb_date - relativedelta(years=1, day=31, month=12)

    py_trans_df = self.trans_df[self.trans_df["date"] <= self.last_day_prior_year]
    py_bal = py_trans_df.groupby(["qname"]).sum(["debit", "credit"])
    py_bal.reset_index(inplace=True)

    pytrial_bal_df = pd.merge(self.coa_df, py_bal, on="qname", how="left")
    pytrial_bal_df[["debit", "credit"]] = pytrial_bal_df[["debit", "credit"]].fillna(0)
    pytrial_bal_df = pytrial_bal_df[pytrial_bal_df["period_type"] == "duration"]

    pytrial_bal_df = pytrial_bal_df.groupby("period_type").sum(["account_balance"])
    re_amount = pytrial_bal_df["debit"] - pytrial_bal_df["credit"]
    re_amount = re_amount.values[0]

    re_row = self.tb_summary[self.tb_summary["qname"] == "qbo:RetainedEarnings"]
    if re_amount < 0:
      self.tb_summary.loc[re_row.index, "credit"] = -re_amount
    elif re_amount > 0:
      self.tb_summary.loc[re_row.index, "debit"] = re_amount

  def get_current_year_trial_balance(self):
    cy_trans_df = self.trans_df[self.trans_df["date"] > self.last_day_prior_year]
    cy_bal = cy_trans_df.groupby(["qname"]).sum(["debit", "credit"])
    cy_bal.reset_index(inplace=True)

    cytrial_bal_df = pd.merge(self.coa_df, cy_bal, on="qname", how="left")
    cytrial_bal_df[["debit", "credit"]] = cytrial_bal_df[["debit", "credit"]].fillna(0)
    cytrial_bal_df = cytrial_bal_df[cytrial_bal_df["period_type"] == "duration"]
    cytb_summary = cytrial_bal_df.copy()
    for i, r in cytb_summary.iterrows():
      if r["balance"] == "debit":
        cytb_summary.loc[i, "debit"] = r["debit"] - r["credit"]
        cytb_summary.loc[i, "credit"] = 0
      elif r["balance"] == "credit":
        cytb_summary.loc[i, "debit"] = 0
        cytb_summary.loc[i, "credit"] = r["credit"] - r["debit"]

      if r["balance"] == "debit" and cytb_summary.loc[i, "debit"] < 0:
        cytb_summary.loc[i, "credit"] = -cytb_summary.loc[i, "debit"]
        cytb_summary.loc[i, "debit"] = 0
      elif r["balance"] == "credit" and cytb_summary.loc[i, "credit"] < 0:
        cytb_summary.loc[i, "debit"] = -cytb_summary.loc[i, "credit"]
        cytb_summary.loc[i, "credit"] = 0

    full_tb_summary = pd.concat([self.tb_summary, cytb_summary])
    # Drop rows where both debit and credit are 0
    zero_rows_mask = (full_tb_summary["debit"] == 0) & (full_tb_summary["credit"] == 0)
    full_tb_summary = full_tb_summary[~zero_rows_mask]
    full_tb_summary = full_tb_summary.drop(
      columns=["balance", "period_type", "sequence"]
    )
    self.trial_balance_df = full_tb_summary

  def make_tb_report(self):
    uri = self.tb_report_uri()

    # Create or update report
    report_date = dt.datetime.strptime(self.date, "%Y-%m-%d").date()
    merge_report_query = """
    MERGE (r:Report {uri: $uri})
    SET r.identifier = $identifier,
        r.name = $name,
        r.form = $form,
        r.report_date = $report_date
    RETURN r
    """

    report_params = {
      "uri": uri,
      "identifier": generate_uuid7(),
      "name": "Trial Balance",
      "form": "Trial Balance",
      "report_date": report_date.isoformat(),
    }

    self.repository.execute_query(merge_report_query, report_params)

    # Connect report to entity
    connect_report_query = """
    MATCH (e:Entity), (r:Report)
    WHERE e.identifier = $entity_id AND r.uri = $report_uri
    MERGE (e)-[:ENTITY_HAS_REPORT]->(r)
    """

    self.repository.execute_query(
      connect_report_query, {"entity_id": self.entity_id, "report_uri": uri}
    )

    for i, r in self.trial_balance_df.iterrows():
      # Get element by qname
      element_query = "MATCH (e:Element) WHERE e.qname = $qname RETURN e"
      element_result = self.repository.execute_query(
        element_query, {"qname": r["qname"]}
      )

      if not element_result:
        logger.warning(f"Element not found for qname: {r['qname']}")
        continue

      element = element_result[0]["e"]

      if element["balance"] == "debit" and r["debit"] != 0:
        amount = r["debit"]
      elif element["balance"] == "debit" and r["credit"] != 0:
        amount = r["credit"] * -1
      elif element["balance"] == "credit" and r["credit"] != 0:
        amount = r["credit"]
      elif element["balance"] == "credit" and r["debit"] != 0:
        amount = r["debit"] * -1
      else:
        continue  # Skip if no amount

      # Create fact
      fact_identifier = generate_deterministic_uuid7(f"{r.to_json()}", namespace="fact")
      create_fact_query = """
      CREATE (f:Fact {
        identifier: $identifier,
        value: $value,
        type: $type,
        decimals: $decimals
      })
      RETURN f
      """

      fact_params = {
        "identifier": fact_identifier,
        "value": float(amount),
        "type": "monetary",
        "decimals": "2",
      }

      self.repository.execute_query(create_fact_query, fact_params)

      # Connect fact to report
      connect_fact_report_query = """
      MATCH (f:Fact), (r:Report)
      WHERE f.identifier = $fact_id AND r.uri = $report_uri
      CREATE (r)-[:HAS_FACT]->(f)
      """

      self.repository.execute_query(
        connect_fact_report_query, {"fact_id": fact_identifier, "report_uri": uri}
      )

      # Connect fact to element
      connect_fact_element_query = """
      MATCH (f:Fact), (e:Element)
      WHERE f.identifier = $fact_id AND e.qname = $qname
      CREATE (f)-[:HAS_ELEMENT]->(e)
      """

      self.repository.execute_query(
        connect_fact_element_query, {"fact_id": fact_identifier, "qname": r["qname"]}
      )

      start_date = None
      if element["period_type"] == "instant":
        start_date = self.last_day_prior_year + dt.timedelta(days=1)
        start_date = start_date.strftime("%Y-%m-%d")

      self.make_period(fact_identifier, self.date, start_date)
      self.make_entity(fact_identifier)

  def make_period(self, fact_identifier, end_date, start_date=None):
    if start_date is None:
      period_uri = f"{URIConstants.ISO_8601_URI}#{end_date}"
    else:
      period_uri = f"{URIConstants.ISO_8601_URI}#{start_date}/{end_date}"

    # Create or get period
    if start_date is None:
      merge_period_query = """
      MERGE (p:Period {uri: $uri})
      SET p.instant_date = $instant_date
      RETURN p
      """
      period_params = {"uri": period_uri, "instant_date": end_date}
    else:
      merge_period_query = """
      MERGE (p:Period {uri: $uri})
      SET p.start_date = $start_date,
          p.end_date = $end_date
      RETURN p
      """
      period_params = {
        "uri": period_uri,
        "start_date": start_date,
        "end_date": end_date,
      }

    self.repository.execute_query(merge_period_query, period_params)

    # Connect fact to period
    connect_query = """
    MATCH (f:Fact), (p:Period)
    WHERE f.identifier = $fact_id AND p.uri = $period_uri
    CREATE (f)-[:HAS_PERIOD]->(p)
    """

    self.repository.execute_query(
      connect_query, {"fact_id": fact_identifier, "period_uri": period_uri}
    )

  def make_entity(self, fact_identifier):
    entity_uri = self.co_node["uri"]

    # Create or get entity
    merge_entity_query = """
    MERGE (e:Entity {uri: $uri})
    SET e.scheme = $scheme,
        e.identifier = $identifier
    RETURN e
    """

    entity_params = {
      "uri": entity_uri,
      "scheme": URIConstants.ROBOSYSTEMS_BASE_URI,
      "identifier": self.co_node["identifier"],
    }

    self.repository.execute_query(merge_entity_query, entity_params)

    # Connect fact to entity
    connect_query = """
    MATCH (f:Fact), (e:Entity)
    WHERE f.identifier = $fact_id AND e.uri = $entity_uri
    CREATE (f)-[:HAS_ENTITY]->(e)
    """

    self.repository.execute_query(
      connect_query, {"fact_id": fact_identifier, "entity_uri": entity_uri}
    )

  def make_units(self, fact_identifier):
    unit_uri = f"{URIConstants.ISO_4217_URI}#USD"

    # Create or get unit
    value = "USD"
    measure = PrefixConstants.ISO_4217_PREFIX + ":" + value
    merge_unit_query = """
    MERGE (u:Unit {uri: $uri})
    SET u.measure = $measure,
        u.value = $value
    RETURN u
    """

    unit_params = {"uri": unit_uri, "measure": measure, "value": value}

    self.repository.execute_query(merge_unit_query, unit_params)

    # Connect fact to unit
    connect_query = """
    MATCH (f:Fact), (u:Unit)
    WHERE f.identifier = $fact_id AND u.uri = $unit_uri
    CREATE (f)-[:HAS_UNIT]->(u)
    """

    self.repository.execute_query(
      connect_query, {"fact_id": fact_identifier, "unit_uri": unit_uri}
    )

  def tb_report_uri(self):
    return f"{self.co_node['uri']}/reports#TrialBalance"

  def reset_tb_report(self):
    tb_report_uri = self.tb_report_uri()

    # Delete facts connected to the trial balance report
    delete_facts_query = """
    MATCH (r:Report)-[:HAS_FACT]-(f:Fact)
    WHERE r.uri = $report_uri
    DETACH DELETE f
    """

    self.repository.execute_query(delete_facts_query, {"report_uri": tb_report_uri})

    # Delete the trial balance report
    delete_report_query = """
    MATCH (r:Report)
    WHERE r.uri = $report_uri
    DETACH DELETE r
    """

    self.repository.execute_query(delete_report_query, {"report_uri": tb_report_uri})
