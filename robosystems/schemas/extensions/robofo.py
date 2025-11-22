"""
RoboFO Schema Extension for LadybugDB

Front office, CRM, sales, and marketing functionality.
Extends the base schema with sales and marketing-specific entities.
"""

from ..models import Node, Relationship, Property

# RoboFO Extension Nodes
EXTENSION_NODES = [
  Node(
    name="Lead",
    description="Sales leads and prospects",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(
        name="lead_source", type="STRING"
      ),  # website, referral, trade_show, cold_call
      Property(
        name="lead_status", type="STRING"
      ),  # new, contacted, qualified, unqualified, converted
      Property(name="lead_score", type="INT64"),  # 1-100 lead scoring
      Property(name="industry", type="STRING"),
      Property(
        name="entity_size", type="STRING"
      ),  # startup, small, medium, large, enterprise
      Property(name="annual_revenue", type="DOUBLE"),
      Property(name="estimated_budget", type="DOUBLE"),
      Property(name="pain_points", type="STRING"),
      Property(
        name="buying_timeline", type="STRING"
      ),  # immediate, 3_months, 6_months, 12_months
      Property(name="decision_maker", type="BOOLEAN"),
      Property(name="notes", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Opportunity",
    description="Sales opportunities in the pipeline",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="opportunity_name", type="STRING"),
      Property(
        name="stage", type="STRING"
      ),  # prospecting, qualification, proposal, negotiation, closed_won, closed_lost
      Property(name="probability", type="DOUBLE"),  # 0-100 percentage
      Property(name="amount", type="DOUBLE"),
      Property(name="currency", type="STRING"),
      Property(name="expected_close_date", type="DATE"),
      Property(name="actual_close_date", type="DATE"),
      Property(name="close_reason", type="STRING"),
      Property(name="competitor", type="STRING"),
      Property(name="next_step", type="STRING"),
      Property(name="notes", type="STRING"),
      Property(name="created_by", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Customer",
    description="Active customers and accounts",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="customer_number", type="STRING"),
      Property(
        name="customer_type", type="STRING"
      ),  # prospect, active, inactive, churned
      Property(name="industry", type="STRING"),
      Property(name="customer_since", type="DATE"),
      Property(name="last_order_date", type="DATE"),
      Property(name="total_lifetime_value", type="DOUBLE"),
      Property(name="average_order_value", type="DOUBLE"),
      Property(name="payment_terms", type="STRING"),
      Property(name="credit_limit", type="DOUBLE"),
      Property(name="risk_rating", type="STRING"),  # low, medium, high
      Property(name="notes", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Deal",
    description="Closed deals and transactions",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="deal_number", type="STRING"),
      Property(
        name="deal_type", type="STRING"
      ),  # new_business, upsell, renewal, cross_sell
      Property(name="deal_value", type="DOUBLE"),
      Property(name="currency", type="STRING"),
      Property(name="margin_percentage", type="DOUBLE"),
      Property(name="commission_rate", type="DOUBLE"),
      Property(name="contract_length_months", type="INT64"),
      Property(name="recurring_revenue", type="BOOLEAN"),
      Property(name="signed_date", type="DATE"),
      Property(name="effective_date", type="STRING"),
      Property(name="expiration_date", type="STRING"),
    ],
  ),
  Node(
    name="Campaign",
    description="Marketing campaigns and initiatives",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="campaign_name", type="STRING"),
      Property(
        name="campaign_type", type="STRING"
      ),  # email, social, ppc, trade_show, webinar
      Property(name="channel", type="STRING"),  # email, linkedin, google_ads, facebook
      Property(name="start_date", type="STRING"),
      Property(name="end_date", type="STRING"),
      Property(name="budget", type="DOUBLE"),
      Property(name="currency", type="STRING"),
      Property(name="target_audience", type="STRING"),
      Property(name="goal", type="STRING"),  # awareness, leads, sales, retention
      Property(name="status", type="STRING"),  # planned, active, paused, completed
      Property(name="created_by", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Activity",
    description="Sales and marketing activities",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(
        name="activity_type", type="STRING"
      ),  # call, email, meeting, demo, proposal, follow_up
      Property(name="subject", type="STRING"),
      Property(name="description", type="STRING"),
      Property(
        name="outcome", type="STRING"
      ),  # completed, scheduled, cancelled, no_response
      Property(name="duration_minutes", type="INT64"),
      Property(name="priority", type="STRING"),  # low, medium, high, urgent
      Property(name="due_date", type="STRING"),
      Property(name="completed_date", type="STRING"),
      Property(name="created_by", type="STRING"),
      Property(name="assigned_to", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Pipeline",
    description="Sales pipeline configuration and stages",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="pipeline_name", type="STRING"),
      Property(name="pipeline_type", type="STRING"),  # sales, marketing, support
      Property(name="stages", type="STRING"),  # JSON array of stage names
      Property(name="default_pipeline", type="BOOLEAN"),
      Property(name="active", type="BOOLEAN"),
    ],
  ),
  Node(
    name="Quote",
    description="Sales quotes and proposals",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="quote_number", type="STRING"),
      Property(name="quote_date", type="DATE"),
      Property(name="expiration_date", type="STRING"),
      Property(name="total_amount", type="DOUBLE"),
      Property(name="currency", type="STRING"),
      Property(name="discount_percentage", type="DOUBLE"),
      Property(name="tax_amount", type="DOUBLE"),
      Property(
        name="status", type="STRING"
      ),  # draft, sent, accepted, rejected, expired
      Property(name="terms", type="STRING"),
      Property(name="notes", type="STRING"),
      Property(name="created_by", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  # Moved from base schema - CRM/sales related nodes
  Node(
    name="Contact",
    description="Individual contacts across all business contexts",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="first_name", type="STRING"),
      Property(name="last_name", type="STRING"),
      Property(name="full_name", type="STRING"),
      Property(name="email", type="STRING"),
      Property(name="phone", type="STRING"),
      Property(name="mobile", type="STRING"),
      Property(name="title", type="STRING"),
      Property(
        name="contact_type", type="STRING"
      ),  # customer, supplier, employee, lead, partner
      Property(name="status", type="STRING"),  # active, inactive, archived
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="Address",
    description="Physical and mailing addresses for entities",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="address_line_1", type="STRING"),
      Property(name="address_line_2", type="STRING"),
      Property(name="city", type="STRING"),
      Property(name="state", type="STRING"),
      Property(name="postal_code", type="STRING"),
      Property(name="country", type="STRING"),
      Property(
        name="address_type", type="STRING"
      ),  # billing, shipping, mailing, office
      Property(name="is_primary", type="BOOLEAN"),
    ],
  ),
  Node(
    name="Document",
    description="Documents and files associated with entities",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="name", type="STRING"),
      Property(
        name="document_type", type="STRING"
      ),  # contract, report, filing, invoice
      Property(name="file_path", type="STRING"),
      Property(name="file_size", type="INT64"),
      Property(name="mime_type", type="STRING"),
      Property(name="checksum", type="STRING"),
      Property(name="version", type="STRING"),
      Property(name="status", type="STRING"),  # draft, final, archived
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="Event",
    description="Scheduled events and activities across all applications",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(
        name="event_type", type="STRING"
      ),  # meeting, call, email, task, deadline
      Property(name="title", type="STRING"),
      Property(name="description", type="STRING"),
      Property(name="event_date", type="STRING"),
      Property(name="duration_minutes", type="INT64"),
      Property(name="status", type="STRING"),  # scheduled, completed, cancelled
      Property(name="created_by", type="STRING"),
    ],
  ),
]

# RoboFO Extension Relationships
EXTENSION_RELATIONSHIPS = [
  Relationship(
    name="LEAD_HAS_CONTACT",
    from_node="Lead",
    to_node="Contact",
    description="Lead associated with contact person",
    properties=[
      Property(
        name="contact_role", type="STRING"
      ),  # primary, influencer, decision_maker
    ],
  ),
  Relationship(
    name="LEAD_CONVERTS_TO_OPPORTUNITY",
    from_node="Lead",
    to_node="Opportunity",
    description="Lead conversion to sales opportunity",
    properties=[
      Property(name="conversion_date", type="DATE"),
      Property(name="conversion_notes", type="STRING"),
    ],
  ),
  Relationship(
    name="OPPORTUNITY_HAS_CONTACT",
    from_node="Opportunity",
    to_node="Contact",
    description="Opportunity stakeholders and contacts",
    properties=[
      Property(
        name="contact_role", type="STRING"
      ),  # champion, influencer, decision_maker, blocker
      Property(name="influence_level", type="STRING"),  # high, medium, low
    ],
  ),
  Relationship(
    name="OPPORTUNITY_FOR_CUSTOMER",
    from_node="Opportunity",
    to_node="Customer",
    description="Opportunity with existing customer",
    properties=[
      Property(name="opportunity_context", type="STRING"),
    ],
  ),
  Relationship(
    name="OPPORTUNITY_BECOMES_DEAL",
    from_node="Opportunity",
    to_node="Deal",
    description="Opportunity closes as deal",
    properties=[
      Property(name="conversion_date", type="DATE"),
      Property(name="final_discount_percentage", type="DOUBLE"),
    ],
  ),
  Relationship(
    name="CUSTOMER_HAS_CONTACT",
    from_node="Customer",
    to_node="Contact",
    description="Customer contacts and stakeholders",
    properties=[
      Property(
        name="contact_role", type="STRING"
      ),  # primary, billing, technical, executive
      Property(name="is_primary", type="BOOLEAN"),
    ],
  ),
  Relationship(
    name="CUSTOMER_BECOMES_ENTITY",
    from_node="Customer",
    to_node="Entity",
    description="Customer promoted to full entity entity",
    properties=[
      Property(name="promotion_date", type="DATE"),
      Property(name="promotion_reason", type="STRING"),
    ],
  ),
  Relationship(
    name="CAMPAIGN_GENERATES_LEAD",
    from_node="Campaign",
    to_node="Lead",
    description="Campaign generates sales lead",
    properties=[
      Property(name="attribution_percentage", type="DOUBLE"),
    ],
  ),
  Relationship(
    name="ACTIVITY_FOR_LEAD",
    from_node="Activity",
    to_node="Lead",
    description="Sales activity for lead",
    properties=[
      Property(name="activity_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ACTIVITY_FOR_OPPORTUNITY",
    from_node="Activity",
    to_node="Opportunity",
    description="Sales activity for opportunity",
    properties=[
      Property(name="activity_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ACTIVITY_FOR_CUSTOMER",
    from_node="Activity",
    to_node="Customer",
    description="Sales activity for customer",
    properties=[
      Property(name="activity_context", type="STRING"),
    ],
  ),
  Relationship(
    name="OPPORTUNITY_IN_PIPELINE",
    from_node="Opportunity",
    to_node="Pipeline",
    description="Opportunity tracked in sales pipeline",
    properties=[
      Property(name="current_stage", type="STRING"),
      Property(name="stage_entry_date", type="DATE"),
    ],
  ),
  Relationship(
    name="OPPORTUNITY_HAS_QUOTE",
    from_node="Opportunity",
    to_node="Quote",
    description="Opportunity has associated quote",
    properties=[
      Property(name="quote_context", type="STRING"),
    ],
  ),
  Relationship(
    name="QUOTE_FOR_CUSTOMER",
    from_node="Quote",
    to_node="Customer",
    description="Quote provided to customer",
    properties=[
      Property(name="quote_context", type="STRING"),
    ],
  ),
  Relationship(
    name="USER_OWNS_LEAD",
    from_node="User",
    to_node="Lead",
    description="User ownership of sales lead",
    properties=[
      Property(name="ownership_type", type="STRING"),  # owner, collaborator, follower
    ],
  ),
  Relationship(
    name="USER_OWNS_OPPORTUNITY",
    from_node="User",
    to_node="Opportunity",
    description="User ownership of sales opportunity",
    properties=[
      Property(name="ownership_type", type="STRING"),  # owner, collaborator, follower
    ],
  ),
  Relationship(
    name="USER_MANAGES_CUSTOMER",
    from_node="User",
    to_node="Customer",
    description="User account management responsibility",
    properties=[
      Property(
        name="management_role", type="STRING"
      ),  # account_manager, support, technical
    ],
  ),
  # Moved from base schema - relationships for Contact, Address, Document, Event
  Relationship(
    name="CONTACT_HAS_ADDRESS",
    from_node="Contact",
    to_node="Address",
    description="Contact associated with addresses",
    properties=[
      Property(name="address_context", type="STRING"),  # primary, secondary, billing
    ],
  ),
  Relationship(
    name="ENTITY_HAS_ADDRESS",
    from_node="Entity",
    to_node="Address",
    description="Entity addresses for various purposes",
    properties=[
      Property(
        name="address_context", type="STRING"
      ),  # headquarters, branch, registered
    ],
  ),
  Relationship(
    name="ENTITY_HAS_DOCUMENT",
    from_node="Entity",
    to_node="Document",
    description="Documents associated with companies",
    properties=[
      Property(name="document_context", type="STRING"),
      Property(name="access_level", type="STRING"),  # public, private, restricted
    ],
  ),
  Relationship(
    name="USER_HAS_DOCUMENT",
    from_node="User",
    to_node="Document",
    description="Documents associated with users",
    properties=[
      Property(name="document_context", type="STRING"),
      Property(name="access_level", type="STRING"),
    ],
  ),
  Relationship(
    name="CONTACT_HAS_DOCUMENT",
    from_node="Contact",
    to_node="Document",
    description="Documents associated with contacts",
    properties=[
      Property(name="document_context", type="STRING"),
      Property(name="access_level", type="STRING"),
    ],
  ),
  Relationship(
    name="ENTITY_HAS_EVENT",
    from_node="Entity",
    to_node="Event",
    description="Events associated with companies",
    properties=[
      Property(name="event_context", type="STRING"),
      Property(name="participant_role", type="STRING"),
    ],
  ),
  Relationship(
    name="USER_HAS_EVENT",
    from_node="User",
    to_node="Event",
    description="Events associated with users",
    properties=[
      Property(name="event_context", type="STRING"),
      Property(name="participant_role", type="STRING"),
    ],
  ),
  Relationship(
    name="CONTACT_HAS_EVENT",
    from_node="Contact",
    to_node="Event",
    description="Events associated with contacts",
    properties=[
      Property(name="event_context", type="STRING"),
      Property(name="participant_role", type="STRING"),
    ],
  ),
  Relationship(
    name="DOCUMENT_DERIVED_FROM",
    from_node="Document",
    to_node="Document",
    description="Document derivation and lineage tracking",
    properties=[
      Property(
        name="derivation_type", type="STRING"
      ),  # copy, summary, analysis, report
      Property(name="derivation_date", type="STRING"),
      Property(name="transformation_notes", type="STRING"),
    ],
  ),
]
