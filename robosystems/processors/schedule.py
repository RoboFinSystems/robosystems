import os
from arelle import XbrlConst
from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils
from robosystems.config import PrefixConstants, URIConstants
from robosystems.utils import generate_uuid7, generate_deterministic_uuid7
from dateutil.relativedelta import relativedelta
from datetime import datetime
from robosystems.logger import logger
from robosystems.middleware.graph import get_graph_repository


class ScheduleProcessor:
  def __init__(
    self,
    process_identifier,
    transaction_identifier,
    lineitem_identifier,
    transaction_name,
    transaction_account,
    start_date,
    number_of_months,
    database_name=None,
  ):
    self.process_identifier = process_identifier
    self.transaction_identifier = transaction_identifier
    self.lineitem_identifier = lineitem_identifier
    self.transaction_name = transaction_name
    self.transaction_account = transaction_account
    self.start_date = datetime.strptime(start_date, "%B %d, %Y").date()
    self.number_of_months = number_of_months

    # Initialize repository first
    self.database_name = database_name or MultiTenantUtils.get_database_name(None)
    self.repository = get_graph_repository(self.database_name)

    # Get process, transaction, and line item from Kuzu
    process_query = "MATCH (p:Process) WHERE p.identifier = $process_id RETURN p"
    process_result = self.repository.execute_query(
      process_query, {"process_id": self.process_identifier}
    )
    if not process_result:
      raise ValueError(f"Process not found: {self.process_identifier}")
    self.process = process_result[0]["p"]

    transaction_query = "MATCH (t:Transaction) WHERE t.identifier = $tx_id RETURN t"
    transaction_result = self.repository.execute_query(
      transaction_query, {"tx_id": transaction_identifier}
    )
    if not transaction_result:
      raise ValueError(f"Transaction not found: {transaction_identifier}")
    self.transaction = transaction_result[0]["t"]

    lineitem_query = "MATCH (li:LineItem) WHERE li.identifier = $li_id RETURN li"
    lineitem_result = self.repository.execute_query(
      lineitem_query, {"li_id": lineitem_identifier}
    )
    if not lineitem_result:
      raise ValueError(f"LineItem not found: {lineitem_identifier}")
    self.lineitem = lineitem_result[0]["li"]

    # Get entity from process
    entity_query = "MATCH (p:Process)-[:BELONGS_TO_ENTITY]->(c:Entity) WHERE p.identifier = $process_id RETURN c"
    entity_result = self.repository.execute_query(
      entity_query, {"process_id": self.process_identifier}
    )
    if not entity_result:
      raise ValueError(f"Entity not found for process: {self.process_identifier}")
    self.entity = entity_result[0]["c"]

    entity_id = self.entity["identifier"] if self.entity else None
    self.database_name = database_name or MultiTenantUtils.get_database_name(entity_id)
    MultiTenantUtils.log_database_operation(
      "Schedule processor initialization", self.database_name, entity_id
    )

    logger.debug(f"Schedule processor initialized with database: {self.database_name}")
    self.process_type_config()

  def process_type_config(self):
    self.process_type = self.process["type"]
    if self.process_type == "Prepaid Expense":
      self.process_type_prefix = "PrepaidExpense"
    elif self.process_type == "Fixed Asset Depreciation":
      self.process_type_prefix = "DepreciationExpense"
    elif self.process_type == "Intangible Asset Amortization":
      self.process_type_prefix = "AmortizationExpense"
    elif self.process_type == "Accrued Expense":
      self.process_type_prefix = "AccruedExpense"
    elif self.process_type == "Unearned Revenue":
      self.process_type_prefix = "UnearnedRevenue"
    else:
      self.process_type_prefix = "Other"

    self.transaction_name_stripped = self.transaction_name.replace(" ", "")

  def make_schedule(self):
    """Create schedule using Kuzu operations."""
    logger.info(f"Creating schedule for process: {self.process_identifier}")

    self.make_report()
    self.make_taxonomy()
    self.make_structure()
    self.make_facts()

    logger.info("Schedule creation completed")

  async def make_schedule_async(self):
    """Async version of make_schedule method for use in async contexts."""
    logger.info(f"Creating schedule async for process: {self.process_identifier}")

    # Note: Kuzu operations are synchronous, so we just call the sync method
    self.make_report()
    self.make_taxonomy()
    self.make_structure()
    self.make_facts()

    logger.info("Async schedule creation completed")

  def make_report(self):
    report_uri = os.path.join(URIConstants.ROBOSYSTEMS_BASE_URI, "report", "schedule")

    # Check if report exists
    check_query = "MATCH (r:Report) WHERE r.uri = $uri RETURN r"
    existing_report = self.repository.execute_query(check_query, {"uri": report_uri})

    if not existing_report:
      # Create new report
      report_id = generate_uuid7()
      create_query = """
      CREATE (r:Report {
        uri: $uri,
        identifier: $identifier,
        name: $name,
        form: $form
      })
      RETURN r
      """

      params = {
        "uri": report_uri,
        "identifier": report_id,
        "name": "Schedules",
        "form": "Schedules",
      }

      self.repository.execute_query(create_query, params)

      # Connect to entity if exists
      if self.entity:
        connect_query = """
        MATCH (c:Entity), (r:Report)
        WHERE c.identifier = $entity_id AND r.uri = $report_uri
        CREATE (c)-[:HAS_REPORT]->(r)
        """

        self.repository.execute_query(
          connect_query,
          {"entity_id": self.entity["identifier"], "report_uri": report_uri},
        )

    self.report_uri = report_uri

  def make_taxonomy(self):
    """Create or find taxonomy using Kuzu operations."""
    taxonomy_uri = os.path.join(
      URIConstants.ROBOSYSTEMS_BASE_URI, "taxonomy", "schedule"
    )

    # Check if taxonomy exists
    check_query = "MATCH (t:Taxonomy) WHERE t.uri = $uri RETURN t"
    existing_taxonomy = self.repository.execute_query(
      check_query, {"uri": taxonomy_uri}
    )

    if not existing_taxonomy:
      # Create new taxonomy
      taxonomy_id = generate_uuid7()
      create_query = """
      CREATE (t:Taxonomy {
        uri: $uri,
        identifier: $identifier,
        name: $name,
        namespace: $namespace
      })
      RETURN t
      """

      params = {
        "uri": taxonomy_uri,
        "identifier": taxonomy_id,
        "name": "Schedule Taxonomy",
        "namespace": URIConstants.ROBOSYSTEMS_BASE_URI,
      }

      result = self.repository.execute_query(create_query, params)
      self.taxonomy = result[0]["t"]
    else:
      self.taxonomy = existing_taxonomy[0]["t"]

    # Connect taxonomy to entity, report, and process
    if self.entity:
      connect_entity_query = """
      MATCH (c:Entity), (t:Taxonomy)
      WHERE c.identifier = $entity_id AND t.uri = $taxonomy_uri
      MERGE (c)-[:HAS_TAXONOMY]->(t)
      """
      self.repository.execute_query(
        connect_entity_query,
        {"entity_id": self.entity["identifier"], "taxonomy_uri": taxonomy_uri},
      )

    # Connect to report
    connect_report_query = """
    MATCH (r:Report), (t:Taxonomy)
    WHERE r.uri = $report_uri AND t.uri = $taxonomy_uri
    MERGE (r)-[:HAS_TAXONOMY]->(t)
    """
    self.repository.execute_query(
      connect_report_query,
      {"report_uri": self.report_uri, "taxonomy_uri": taxonomy_uri},
    )

    # Connect to process
    connect_process_query = """
    MATCH (p:Process), (t:Taxonomy)
    WHERE p.identifier = $process_id AND t.uri = $taxonomy_uri
    MERGE (p)-[:HAS_TAXONOMY]->(t)
    """
    self.repository.execute_query(
      connect_process_query,
      {"process_id": self.process_identifier, "taxonomy_uri": taxonomy_uri},
    )

  def make_structure(self):
    """Create or find structure using Kuzu operations."""
    structure_uri = os.path.join(
      URIConstants.ROBOSYSTEMS_BASE_URI,
      "structure",
      "schedule",
      self.process_identifier,
    )

    # Check if structure exists
    check_query = "MATCH (s:Structure) WHERE s.uri = $uri RETURN s"
    existing_structure = self.repository.execute_query(
      check_query, {"uri": structure_uri}
    )

    if not existing_structure:
      # Create new structure
      structure_id = generate_deterministic_uuid7(structure_uri, namespace="structure")
      network_uri = os.path.join(
        URIConstants.ROBOSYSTEMS_BASE_URI,
        "network",
        "schedule",
        self.process_identifier,
      )

      create_query = """
      CREATE (s:Structure {
        uri: $uri,
        identifier: $identifier,
        network_uri: $network_uri,
        type: $type,
        name: $name
      })
      RETURN s
      """

      params = {
        "uri": structure_uri,
        "identifier": structure_id,
        "network_uri": network_uri,
        "type": "Schedule",
        "name": f"{self.process['type']} Schedule",
      }

      result = self.repository.execute_query(create_query, params)
      self.structure = result[0]["s"]
    else:
      self.structure = existing_structure[0]["s"]

    # Connect structure to taxonomy
    taxonomy_uri = os.path.join(
      URIConstants.ROBOSYSTEMS_BASE_URI, "taxonomy", "schedule"
    )
    connect_taxonomy_query = """
    MATCH (s:Structure), (t:Taxonomy)
    WHERE s.uri = $structure_uri AND t.uri = $taxonomy_uri
    MERGE (s)-[:HAS_TAXONOMY]->(t)
    """
    self.repository.execute_query(
      connect_taxonomy_query,
      {"structure_uri": structure_uri, "taxonomy_uri": taxonomy_uri},
    )

    # Check if structure has associations
    associations_query = """
    MATCH (s:Structure)-[:HAS_ASSOCIATION]->(a:Association)
    WHERE s.uri = $structure_uri
    RETURN count(a) as association_count
    """
    association_result = self.repository.execute_single(
      associations_query, {"structure_uri": structure_uri}
    )

    if not association_result or association_result["association_count"] == 0:
      self.make_base_associations()

    self.add_transaction_member()
    self.add_transaction_lineitem()

  def _create_association(
    self,
    from_element,
    to_element,
    arcrole,
    order_value,
    association_type,
    root=False,
    preferred_label=None,
  ):
    """Helper method to create associations using Kuzu operations."""
    association_id = generate_uuid7()

    create_query = """
    CREATE (a:Association {
      identifier: $identifier,
      arcrole: $arcrole,
      order_value: $order_value,
      association_type: $association_type,
      root: $root,
      preferred_label: $preferred_label
    })
    RETURN a
    """

    params = {
      "identifier": association_id,
      "arcrole": arcrole,
      "order_value": order_value,
      "association_type": association_type,
      "root": root,
      "preferred_label": preferred_label,
    }

    result = self.repository.execute_query(create_query, params)
    association = result[0]["a"]

    # Connect association to elements and structure
    structure_uri = os.path.join(
      URIConstants.ROBOSYSTEMS_BASE_URI,
      "structure",
      "schedule",
      self.process_identifier,
    )

    connect_query = """
    MATCH (s:Structure), (from_e:Element), (to_e:Element), (a:Association)
    WHERE s.uri = $structure_uri
      AND from_e.uri = $from_uri
      AND to_e.uri = $to_uri
      AND a.identifier = $association_id
    CREATE (s)-[:HAS_ASSOCIATION]->(a)
    CREATE (a)-[:FROM_ELEMENT]->(from_e)
    CREATE (a)-[:TO_ELEMENT]->(to_e)
    """

    self.repository.execute_query(
      connect_query,
      {
        "structure_uri": structure_uri,
        "from_uri": from_element["uri"],
        "to_uri": to_element["uri"],
        "association_id": association_id,
      },
    )

    return association

  def make_base_associations(self):
    """Create base associations using Kuzu operations."""
    # Create association 1: Root -> Table
    self._create_association(
      from_element=self.make_root_element(),
      to_element=self.make_schedule_table_element(),
      arcrole=XbrlConst.parentChild,
      order_value=1,
      association_type="Presentation",
      root=True,
      preferred_label="http://www.xbrl.org/2003/role/terseLabel",
    )

    # Create association 2: Table -> Axis
    self._create_association(
      from_element=self.make_schedule_table_element(),
      to_element=self.make_schedule_axis_element(),
      arcrole=XbrlConst.parentChild,
      order_value=1,
      association_type="Presentation",
      root=False,
      preferred_label="http://www.xbrl.org/2003/role/terseLabel",
    )

    # Create association 3: Table -> LineItems
    self._create_association(
      from_element=self.make_schedule_table_element(),
      to_element=self.make_schedule_lineitems_element(),
      arcrole=XbrlConst.parentChild,
      order_value=2,
      association_type="Presentation",
      root=False,
      preferred_label="http://www.xbrl.org/2003/role/terseLabel",
    )

    # Create association 4: Axis -> Domain
    self._create_association(
      from_element=self.make_schedule_axis_element(),
      to_element=self.make_schedule_domain_element(),
      arcrole=XbrlConst.parentChild,
      order_value=1,
      association_type="Presentation",
      root=False,
      preferred_label="http://www.xbrl.org/2003/role/terseLabel",
    )

  def add_transaction_member(self):
    """Add transaction member association using Kuzu operations."""
    self._create_association(
      from_element=self.make_schedule_domain_element(),
      to_element=self.make_schedule_member_element(),
      arcrole=XbrlConst.parentChild,
      order_value=1,
      association_type="Presentation",
      root=False,
      preferred_label="http://www.xbrl.org/2003/role/terseLabel",
    )

  def add_transaction_lineitem(self):
    """Add transaction lineitem association using Kuzu operations."""
    self._create_association(
      from_element=self.make_schedule_lineitems_element(),
      to_element=self.make_element(self.transaction_account),
      arcrole=XbrlConst.parentChild,
      order_value=1,
      association_type="Presentation",
      root=False,
      preferred_label="http://www.xbrl.org/2003/role/terseLabel",
    )

  def _create_element(self, element_name, element_type, classification=None, **kwargs):
    """Helper method to create elements using Kuzu operations."""
    uri = os.path.join(URIConstants.ROBOSYSTEMS_BASE_URI, "element", "schedule")
    uri = f"{uri}#{element_name}"
    qname = f"{PrefixConstants.ROBOSYSTEMS_PREFIX}:{element_name}"

    # Check if element exists
    check_query = "MATCH (e:Element) WHERE e.uri = $uri RETURN e"
    existing_element = self.repository.execute_query(check_query, {"uri": uri})

    if existing_element:
      return existing_element[0]["e"]

    # Default properties
    properties = {
      "uri": uri,
      "identifier": generate_deterministic_uuid7(uri, namespace="association"),
      "qname": qname,
      "period_type": "duration",
      "type": element_type,
      "is_abstract": kwargs.get("is_abstract", False),
      "is_dimension_item": kwargs.get("is_dimension_item", False),
      "is_domain_member": kwargs.get("is_domain_member", False),
      "is_hypercube_item": kwargs.get("is_hypercube_item", False),
      "is_integer": kwargs.get("is_integer", False),
      "is_numeric": kwargs.get("is_numeric", False),
      "is_shares": kwargs.get("is_shares", False),
      "is_fraction": kwargs.get("is_fraction", False),
      "is_textblock": kwargs.get("is_textblock", False),
      "substitution_group": kwargs.get(
        "substitution_group", "http://www.xbrl.org/2003/instance#item"
      ),
      "item_type": kwargs.get(
        "item_type", "http://www.xbrl.org/2003/instance#stringItemType"
      ),
    }

    if classification:
      properties["classification"] = classification

    # Build the CREATE query dynamically
    property_assignments = []
    for key, value in properties.items():
      property_assignments.append(f"{key}: ${key}")

    create_query = f"""
    CREATE (e:Element {{
      {", ".join(property_assignments)}
    }})
    RETURN e
    """

    result = self.repository.execute_query(create_query, properties)
    return result[0]["e"]

  def make_root_element(self):
    """Create root schedule element using Kuzu operations."""
    return self._create_element(
      element_name="ScheduleAbstract",
      element_type="String",
      is_abstract=True,
      is_domain_member=True,
      substitution_group="http://www.xbrl.org/2003/instance#item",
      item_type="http://www.xbrl.org/2003/instance#stringItemType",
    )

  def make_schedule_table_element(self):
    """Create schedule table element using Kuzu operations."""
    return self._create_element(
      element_name="ScheduleTable",
      element_type="Table",
      classification="hypercubeElement",
      is_abstract=True,
      is_hypercube_item=True,
      substitution_group="http://xbrl.org/2005/xbrldt#hypercubeItem",
      item_type="http://www.xbrl.org/2003/instance#stringItemType",
    )

  def make_schedule_axis_element(self):
    """Create schedule axis element using Kuzu operations."""
    element_name = self.process_type_prefix + "Axis"
    return self._create_element(
      element_name=element_name,
      element_type="Axis",
      is_abstract=True,
      is_dimension_item=True,
      substitution_group="http://xbrl.org/2005/xbrldt#dimensionItem",
      item_type="http://www.xbrl.org/2003/instance#stringItemType",
    )

  def make_schedule_domain_element(self):
    """Create schedule domain element using Kuzu operations."""
    element_name = self.process_type_prefix + "Domain"
    return self._create_element(
      element_name=element_name,
      element_type="Domain",
      classification="domainElement",
      is_abstract=True,
      is_domain_member=True,
      substitution_group="http://xbrl.org/2005/xbrldt#dimensionItem",
      item_type="http://www.xbrl.org/dtr/type/2022-03-31#domainItemType",
    )

  def make_schedule_member_element(self):
    """Create schedule member element using Kuzu operations."""
    element_name = self.transaction_name_stripped + "Member"
    return self._create_element(
      element_name=element_name,
      element_type="Domain",
      classification="memberElement",
      is_abstract=False,
      is_domain_member=True,
      substitution_group="http://www.xbrl.org/2003/instance#item",
      item_type="http://www.xbrl.org/dtr/type/2022-03-31#domainItemType",
    )

  def make_schedule_lineitems_element(self):
    """Create schedule line items element using Kuzu operations."""
    return self._create_element(
      element_name="ScheduleLineItems",
      element_type="String",
      classification="lineItemsElement",
      is_abstract=True,
      is_domain_member=True,
      substitution_group="http://www.xbrl.org/2003/instance#item",
      item_type="http://www.xbrl.org/2003/instance#stringItemType",
    )

  def make_element(self, acct):
    """Get element by account using Kuzu operations."""
    # Get the lineitem's element first
    lineitem_element_query = """
    MATCH (li:LineItem)-[:HAS_ELEMENT]->(e:Element)
    WHERE li.identifier = $lineitem_id
    RETURN e.uri as uri
    """

    lineitem_element_result = self.repository.execute_single(
      lineitem_element_query, {"lineitem_id": self.lineitem_identifier}
    )

    if not lineitem_element_result:
      raise ValueError(f"No element found for lineitem: {self.lineitem_identifier}")

    # Build the element URI based on the account
    base_uri = lineitem_element_result["uri"].split("#")[0]
    element_uri = f"{base_uri}#{acct.split(':')[1]}"

    # Find the element
    element_query = "MATCH (e:Element) WHERE e.uri = $uri RETURN e"
    element_result = self.repository.execute_single(element_query, {"uri": element_uri})

    if not element_result:
      raise ValueError(f"Element not found: {element_uri}")

    return element_result

  def make_facts(self):
    """Create facts using Kuzu operations."""
    # Delete existing factsets for this lineitem
    delete_factsets_query = """
    MATCH (li:LineItem)-[:HAS_FACTSET]->(fs:FactSet)
    WHERE li.identifier = $lineitem_id
    DETACH DELETE fs
    """
    self.repository.execute_query(
      delete_factsets_query, {"lineitem_id": self.lineitem_identifier}
    )

    # Create new factset
    factset_id = generate_uuid7()
    create_factset_query = """
    CREATE (fs:FactSet {
      identifier: $identifier
    })
    RETURN fs
    """
    self.repository.execute_query(create_factset_query, {"identifier": factset_id})

    # Connect factset to report, structure, and lineitem
    connect_factset_query = """
    MATCH (r:Report), (s:Structure), (li:LineItem), (fs:FactSet)
    WHERE r.uri = $report_uri
      AND s.uri = $structure_uri
      AND li.identifier = $lineitem_id
      AND fs.identifier = $factset_id
    CREATE (r)-[:HAS_FACTSET]->(fs)
    CREATE (s)-[:HAS_FACTSET]->(fs)
    CREATE (li)-[:HAS_FACTSET]->(fs)
    """

    structure_uri = os.path.join(
      URIConstants.ROBOSYSTEMS_BASE_URI,
      "structure",
      "schedule",
      self.process_identifier,
    )
    self.repository.execute_query(
      connect_factset_query,
      {
        "report_uri": self.report_uri,
        "structure_uri": structure_uri,
        "lineitem_id": self.lineitem_identifier,
        "factset_id": factset_id,
      },
    )

    # Create facts for each month
    cumsum_value = 0
    for i in range(self.number_of_months):
      start_date = self.start_date + relativedelta(months=i)
      end_date = self.start_date + relativedelta(months=(i + 1))
      end_date = end_date - relativedelta(days=1)
      value = round(self.lineitem["debit_amount"] / self.number_of_months, 2)
      if i == self.number_of_months - 1:
        value = round(self.lineitem["debit_amount"] - cumsum_value, 2)
      cumsum_value += value
      fact = self.make_fact(value, start_date, end_date)

      # Connect fact to factset
      connect_fact_query = """
      MATCH (fs:FactSet), (f:Fact)
      WHERE fs.identifier = $factset_id AND f.identifier = $fact_id
      CREATE (fs)-[:HAS_FACT]->(f)
      """
      self.repository.execute_query(
        connect_fact_query,
        {
          "factset_id": factset_id,
          "fact_id": fact["identifier"],
        },
      )

  def make_fact(self, value, start_date, end_date):
    """Create fact using Kuzu operations."""
    fact_id = generate_uuid7()

    # Create fact
    create_fact_query = """
    CREATE (f:Fact {
      identifier: $identifier,
      value: $value,
      type: $type,
      decimals: $decimals
    })
    RETURN f
    """

    fact_result = self.repository.execute_query(
      create_fact_query,
      {
        "identifier": fact_id,
        "value": value,
        "type": "Numeric",
        "decimals": 2,
      },
    )
    fact = fact_result[0]["f"]

    # Connect fact to report and element
    element = self.make_element(self.transaction_account)
    connect_fact_query = """
    MATCH (r:Report), (e:Element), (f:Fact)
    WHERE r.uri = $report_uri
      AND e.uri = $element_uri
      AND f.identifier = $fact_id
    CREATE (f)-[:BELONGS_TO_REPORT]->(r)
    CREATE (f)-[:HAS_ELEMENT]->(e)
    """

    self.repository.execute_query(
      connect_fact_query,
      {
        "report_uri": self.report_uri,
        "element_uri": element["uri"],
        "fact_id": fact_id,
      },
    )

    # Create and connect supporting entities
    self.make_units(fact)
    self.make_period(fact, start_date, end_date)
    self.make_entity(fact)
    self.make_fact_dimensions(fact)

    return fact

  def make_period(self, fact, start_date, end_date):
    """Create or find period using Kuzu operations."""
    period_uri = f"{URIConstants.ISO_8601_URI}#{start_date}/{end_date}"

    # Check if period exists
    check_period_query = "MATCH (p:Period) WHERE p.uri = $uri RETURN p"
    existing_period = self.repository.execute_query(
      check_period_query, {"uri": period_uri}
    )

    if not existing_period:
      # Create new period
      create_period_query = """
      CREATE (p:Period {
        uri: $uri,
        start_date: $start_date,
        end_date: $end_date
      })
      RETURN p
      """

      self.repository.execute_query(
        create_period_query,
        {
          "uri": period_uri,
          "start_date": start_date.isoformat(),
          "end_date": end_date.isoformat(),
        },
      )
    # Period already exists, no need to create

    # Connect fact to period
    connect_period_query = """
    MATCH (f:Fact), (p:Period)
    WHERE f.identifier = $fact_id AND p.uri = $period_uri
    CREATE (f)-[:HAS_PERIOD]->(p)
    """

    self.repository.execute_query(
      connect_period_query,
      {
        "fact_id": fact["identifier"],
        "period_uri": period_uri,
      },
    )

  def make_entity(self, fact):
    """Create or find entity using Kuzu operations."""
    entity_uri = self.entity["uri"]

    # Check if entity exists
    check_entity_query = "MATCH (e:Entity) WHERE e.uri = $uri RETURN e"
    existing_entity = self.repository.execute_query(
      check_entity_query, {"uri": entity_uri}
    )

    if not existing_entity:
      # Create new entity
      create_entity_query = """
      CREATE (e:Entity {
        uri: $uri,
        scheme: $scheme,
        identifier: $identifier
      })
      RETURN e
      """

      self.repository.execute_query(
        create_entity_query,
        {
          "uri": entity_uri,
          "scheme": URIConstants.ROBOSYSTEMS_BASE_URI,
          "identifier": self.entity["identifier"],
        },
      )
    # Entity already exists, no need to create

    # Connect fact to entity
    connect_entity_query = """
    MATCH (f:Fact), (e:Entity)
    WHERE f.identifier = $fact_id AND e.uri = $entity_uri
    CREATE (f)-[:HAS_ENTITY]->(e)
    """

    self.repository.execute_query(
      connect_entity_query,
      {
        "fact_id": fact["identifier"],
        "entity_uri": entity_uri,
      },
    )

  def make_units(self, fact):
    """Create or find unit using Kuzu operations."""
    unit_uri = f"{URIConstants.ISO_4217_URI}#USD"

    # Check if unit exists
    check_unit_query = "MATCH (u:Unit) WHERE u.uri = $uri RETURN u"
    existing_unit = self.repository.execute_query(check_unit_query, {"uri": unit_uri})

    if not existing_unit:
      # Create new unit
      value = "USD"
      measure = PrefixConstants.ISO_4217_PREFIX + ":" + value

      create_unit_query = """
      CREATE (u:Unit {
        uri: $uri,
        measure: $measure,
        value: $value
      })
      RETURN u
      """

      self.repository.execute_query(
        create_unit_query,
        {
          "uri": unit_uri,
          "measure": measure,
          "value": value,
        },
      )
    # Unit already exists, no need to create

    # Connect fact to unit
    connect_unit_query = """
    MATCH (f:Fact), (u:Unit)
    WHERE f.identifier = $fact_id AND u.uri = $unit_uri
    CREATE (f)-[:HAS_UNIT]->(u)
    """

    self.repository.execute_query(
      connect_unit_query,
      {
        "fact_id": fact["identifier"],
        "unit_uri": unit_uri,
      },
    )

  def make_fact_dimensions(self, fact):
    """Create fact dimensions using Kuzu operations."""
    axis_element = self.make_schedule_axis_element()
    axis_uri = axis_element["uri"]
    member_element = self.make_schedule_member_element()
    member_uri = member_element["uri"]
    axis_type = "segment"

    # Check if fact dimension exists
    check_fact_dim_query = """
    MATCH (fd:FactDimension)
    WHERE fd.axis_uri = $axis_uri
      AND fd.member_uri = $member_uri
      AND fd.type = $axis_type
    RETURN fd
    """

    existing_fact_dim = self.repository.execute_query(
      check_fact_dim_query,
      {
        "axis_uri": axis_uri,
        "member_uri": member_uri,
        "axis_type": axis_type,
      },
    )

    if not existing_fact_dim:
      # Create new fact dimension
      fact_dim_id = generate_uuid7()
      create_fact_dim_query = """
      CREATE (fd:FactDimension {
        identifier: $identifier,
        axis_uri: $axis_uri,
        member_uri: $member_uri,
        type: $type,
        is_explicit: $is_explicit,
        is_typed: $is_typed
      })
      RETURN fd
      """

      fact_dim_result = self.repository.execute_query(
        create_fact_dim_query,
        {
          "identifier": fact_dim_id,
          "axis_uri": axis_uri,
          "member_uri": member_uri,
          "type": axis_type,
          "is_explicit": True,
          "is_typed": False,
        },
      )
      fact_dim = fact_dim_result[0]["fd"]
    else:
      fact_dim = existing_fact_dim[0]["fd"]

    # Connect fact dimension to axis and member elements
    connect_elements_query = """
    MATCH (fd:FactDimension), (axis_e:Element), (member_e:Element)
    WHERE fd.identifier = $fact_dim_id
      AND axis_e.uri = $axis_uri
      AND member_e.uri = $member_uri
    CREATE (fd)-[:HAS_AXIS_ELEMENT]->(axis_e)
    CREATE (fd)-[:HAS_MEMBER_ELEMENT]->(member_e)
    """

    self.repository.execute_query(
      connect_elements_query,
      {
        "fact_dim_id": fact_dim["identifier"],
        "axis_uri": axis_uri,
        "member_uri": member_uri,
      },
    )

    # Connect fact to dimension
    connect_fact_query = """
    MATCH (f:Fact), (fd:FactDimension)
    WHERE f.identifier = $fact_id AND fd.identifier = $fact_dim_id
    CREATE (f)-[:HAS_DIMENSION]->(fd)
    """

    self.repository.execute_query(
      connect_fact_query,
      {
        "fact_id": fact["identifier"],
        "fact_dim_id": fact_dim["identifier"],
      },
    )
