"""
RoboSCM Schema Extension for LadybugDB

Supply chain management, procurement, inventory, and logistics.
Extends the base schema with supply chain-specific entities.
"""

from ..models import Node, Relationship, Property

# RoboSCM Extension Nodes
EXTENSION_NODES = [
  Node(
    name="Supplier",
    description="External suppliers and vendors",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="supplier_code", type="STRING"),
      Property(name="name", type="STRING"),
      Property(name="rating", type="DOUBLE"),  # 1-5 supplier rating
      Property(name="certification", type="STRING"),  # ISO, quality certifications
      Property(name="payment_terms", type="STRING"),  # NET30, NET60, etc.
      Property(name="currency", type="STRING"),
      Property(name="status", type="STRING"),  # active, inactive, suspended
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Product",
    description="Products and materials in the supply chain",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="sku", type="STRING"),
      Property(name="name", type="STRING"),
      Property(name="description", type="STRING"),
      Property(name="category", type="STRING"),
      Property(name="subcategory", type="STRING"),
      Property(name="unit_of_measure", type="STRING"),  # each, kg, liter, box
      Property(name="unit_cost", type="DOUBLE"),
      Property(name="list_price", type="DOUBLE"),
      Property(name="weight", type="DOUBLE"),
      Property(name="dimensions", type="STRING"),  # LxWxH
      Property(name="hazmat", type="BOOLEAN"),
      Property(name="shelf_life_days", type="INT64"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Warehouse",
    description="Storage and distribution facilities",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="warehouse_code", type="STRING"),
      Property(name="name", type="STRING"),
      Property(
        name="warehouse_type", type="STRING"
      ),  # distribution, manufacturing, storage
      Property(name="capacity_cubic_feet", type="DOUBLE"),
      Property(name="temperature_controlled", type="BOOLEAN"),
      Property(name="status", type="STRING"),  # active, inactive, maintenance
    ],
  ),
  Node(
    name="Inventory",
    description="Stock levels and inventory management",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="quantity_on_hand", type="INT64"),
      Property(name="quantity_available", type="INT64"),  # on_hand - reserved
      Property(name="quantity_reserved", type="INT64"),
      Property(name="reorder_level", type="INT64"),
      Property(name="max_stock_level", type="INT64"),
      Property(name="safety_stock", type="INT64"),
      Property(name="average_cost", type="DOUBLE"),
      Property(name="last_count_date", type="DATE"),
      Property(name="last_updated", type="TIMESTAMP"),
    ],
  ),
  Node(
    name="PurchaseOrder",
    description="Purchase orders to suppliers",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="po_number", type="STRING"),
      Property(name="order_date", type="DATE"),
      Property(name="requested_delivery_date", type="DATE"),
      Property(name="expected_delivery_date", type="DATE"),
      Property(name="total_amount", type="DOUBLE"),
      Property(name="currency", type="STRING"),
      Property(
        name="status", type="STRING"
      ),  # draft, sent, acknowledged, shipped, received, closed
      Property(name="terms", type="STRING"),
      Property(name="notes", type="STRING"),
      Property(name="created_by", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Contract",
    description="Supplier contracts and agreements",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="contract_number", type="STRING"),
      Property(name="contract_type", type="STRING"),  # purchase, service, lease
      Property(name="start_date", type="DATE"),
      Property(name="end_date", type="DATE"),
      Property(name="total_value", type="DOUBLE"),
      Property(name="currency", type="STRING"),
      Property(name="auto_renew", type="BOOLEAN"),
      Property(name="status", type="STRING"),  # draft, active, expired, terminated
      Property(name="terms", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Shipment",
    description="Shipping and logistics tracking",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="tracking_number", type="STRING"),
      Property(name="carrier", type="STRING"),  # FedEx, UPS, USPS, DHL
      Property(name="service_level", type="STRING"),  # ground, express, overnight
      Property(name="origin_address", type="STRING"),
      Property(name="destination_address", type="STRING"),
      Property(name="ship_date", type="DATE"),
      Property(name="expected_delivery", type="STRING"),
      Property(name="actual_delivery", type="STRING"),
      Property(name="shipping_cost", type="DOUBLE"),
      Property(name="weight", type="DOUBLE"),
      Property(
        name="status", type="STRING"
      ),  # created, shipped, in_transit, delivered, exception
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Demand",
    description="Demand forecasting and planning",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="demand_type", type="STRING"),  # forecast, actual, planned
      Property(name="quantity", type="DOUBLE"),
      Property(name="demand_date", type="DATE"),
      Property(name="priority", type="STRING"),  # low, medium, high, critical
      Property(name="source", type="STRING"),  # sales, production, forecast
      Property(name="confidence_level", type="DOUBLE"),  # 0-100 percentage
      Property(name="notes", type="STRING"),
    ],
  ),
  Node(
    name="Contact",
    description="Contact information for suppliers and warehouses",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="name", type="STRING"),
      Property(name="title", type="STRING"),
      Property(name="email", type="STRING"),
      Property(name="phone", type="STRING"),
      Property(name="mobile", type="STRING"),
      Property(name="department", type="STRING"),
      Property(name="is_primary", type="BOOLEAN"),
      Property(name="created_at", type="STRING"),
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="Address",
    description="Physical addresses for suppliers and warehouses",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="address_type", type="STRING"),  # billing, shipping, headquarters
      Property(name="street1", type="STRING"),
      Property(name="street2", type="STRING"),
      Property(name="city", type="STRING"),
      Property(name="state", type="STRING"),
      Property(name="postal_code", type="STRING"),
      Property(name="country", type="STRING"),
      Property(name="latitude", type="DOUBLE"),
      Property(name="longitude", type="DOUBLE"),
      Property(name="is_primary", type="BOOLEAN"),
      Property(name="created_at", type="STRING"),
      Property(name="updated_at", type="STRING"),
    ],
  ),
]

# RoboSCM Extension Relationships
EXTENSION_RELATIONSHIPS = [
  Relationship(
    name="ENTITY_HAS_SUPPLIER",
    from_node="Entity",
    to_node="Supplier",
    description="Entity works with suppliers",
    properties=[
      Property(
        name="relationship_type", type="STRING"
      ),  # primary, secondary, strategic
      Property(name="preferred_supplier", type="BOOLEAN"),
    ],
  ),
  Relationship(
    name="SUPPLIER_HAS_CONTACT",
    from_node="Supplier",
    to_node="Contact",
    description="Supplier contact persons",
    properties=[
      Property(
        name="contact_role", type="STRING"
      ),  # account_manager, technical, billing
      Property(name="is_primary", type="BOOLEAN"),
    ],
  ),
  Relationship(
    name="SUPPLIER_HAS_ADDRESS",
    from_node="Supplier",
    to_node="Address",
    description="Supplier addresses for shipping and billing",
    properties=[
      Property(name="address_role", type="STRING"),  # billing, shipping, corporate
    ],
  ),
  Relationship(
    name="SUPPLIER_PROVIDES_PRODUCT",
    from_node="Supplier",
    to_node="Product",
    description="Supplier supplies specific products",
    properties=[
      Property(name="lead_time_days", type="INT64"),
      Property(name="minimum_order_quantity", type="INT64"),
      Property(name="supplier_part_number", type="STRING"),
      Property(name="unit_price", type="DOUBLE"),
      Property(name="effective_date", type="DATE"),
      Property(name="expiration_date", type="DATE"),
    ],
  ),
  Relationship(
    name="ENTITY_HAS_WAREHOUSE",
    from_node="Entity",
    to_node="Warehouse",
    description="Entity operates warehouses",
    properties=[
      Property(name="warehouse_role", type="STRING"),  # primary, secondary, overflow
    ],
  ),
  Relationship(
    name="WAREHOUSE_HAS_INVENTORY",
    from_node="Warehouse",
    to_node="Inventory",
    description="Warehouse stores inventory",
    properties=[
      Property(name="location_code", type="STRING"),  # aisle, bin, shelf location
    ],
  ),
  Relationship(
    name="INVENTORY_OF_PRODUCT",
    from_node="Inventory",
    to_node="Product",
    description="Inventory holds specific products",
    properties=[
      Property(name="lot_number", type="STRING"),
      Property(name="expiration_date", type="DATE"),
    ],
  ),
  Relationship(
    name="ENTITY_HAS_PURCHASE_ORDER",
    from_node="Entity",
    to_node="PurchaseOrder",
    description="Entity creates purchase orders",
    properties=[
      Property(name="order_context", type="STRING"),
    ],
  ),
  Relationship(
    name="PURCHASE_ORDER_TO_SUPPLIER",
    from_node="PurchaseOrder",
    to_node="Supplier",
    description="Purchase order sent to supplier",
    properties=[
      Property(name="supplier_acknowledgment_date", type="DATE"),
    ],
  ),
  Relationship(
    name="PURCHASE_ORDER_CONTAINS_PRODUCT",
    from_node="PurchaseOrder",
    to_node="Product",
    description="Purchase order line items",
    properties=[
      Property(name="quantity_ordered", type="INT64"),
      Property(name="unit_price", type="DOUBLE"),
      Property(name="line_total", type="DOUBLE"),
      Property(name="quantity_received", type="INT64"),
    ],
  ),
  Relationship(
    name="ENTITY_HAS_CONTRACT",
    from_node="Entity",
    to_node="Contract",
    description="Entity contracts with suppliers",
    properties=[
      Property(name="contract_role", type="STRING"),  # buyer, seller, service_provider
    ],
  ),
  Relationship(
    name="CONTRACT_WITH_SUPPLIER",
    from_node="Contract",
    to_node="Supplier",
    description="Contract with specific supplier",
    properties=[
      Property(name="contract_role", type="STRING"),
    ],
  ),
  Relationship(
    name="SHIPMENT_FOR_PURCHASE_ORDER",
    from_node="Shipment",
    to_node="PurchaseOrder",
    description="Shipment fulfills purchase order",
    properties=[
      Property(name="shipment_type", type="STRING"),  # full, partial, final
    ],
  ),
  Relationship(
    name="SHIPMENT_CONTAINS_PRODUCT",
    from_node="Shipment",
    to_node="Product",
    description="Shipment contains specific products",
    properties=[
      Property(name="quantity_shipped", type="INT64"),
    ],
  ),
  Relationship(
    name="PRODUCT_HAS_DEMAND",
    from_node="Product",
    to_node="Demand",
    description="Product demand forecasting",
    properties=[
      Property(name="demand_context", type="STRING"),
    ],
  ),
  Relationship(
    name="WAREHOUSE_HAS_ADDRESS",
    from_node="Warehouse",
    to_node="Address",
    description="Warehouse physical location",
    properties=[
      Property(name="address_type", type="STRING"),  # facility, billing, mailing
    ],
  ),
]
