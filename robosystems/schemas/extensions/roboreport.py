"""
RoboReport Schema Extension for LadybugDB

Regulatory filing, compliance, and submission management.
Extends the base schema with regulatory-specific entities.
"""

from ..models import Node, Relationship, Property

# RoboReport Extension Nodes
EXTENSION_NODES = [
  Node(
    name="Regulation",
    description="Regulatory requirements and compliance rules",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="regulation_code", type="STRING"),  # SOX-404, 10-K, 10-Q, DEF-14A
      Property(name="regulation_name", type="STRING"),
      Property(name="regulatory_body", type="STRING"),  # SEC, FINRA, CFTC, EPA, OSHA
      Property(name="description", type="STRING"),
      Property(
        name="frequency", type="STRING"
      ),  # annual, quarterly, monthly, as_needed, event_driven
      Property(name="mandatory", type="BOOLEAN"),
      Property(name="effective_date", type="STRING"),
      Property(name="sunset_date", type="DATE"),
      Property(name="penalty_amount", type="DOUBLE"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Filing",
    description="Regulatory filings and submissions",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="filing_number", type="STRING"),
      Property(
        name="filing_type", type="STRING"
      ),  # periodic, event_driven, amendment, correction
      Property(
        name="filing_status", type="STRING"
      ),  # draft, under_review, submitted, accepted, rejected
      Property(name="filing_period", type="STRING"),  # Q1_2024, FY_2023, monthly_202401
      Property(name="due_date", type="STRING"),
      Property(name="submitted_date", type="STRING"),
      Property(name="accepted_date", type="STRING"),
      Property(name="confirmation_number", type="STRING"),
      Property(name="filing_fee", type="DOUBLE"),
      Property(name="currency", type="STRING"),
      Property(name="priority", type="STRING"),  # low, medium, high, urgent
      Property(name="notes", type="STRING"),
      Property(name="created_by", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Submission",
    description="Electronic submissions to regulatory bodies",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="submission_id", type="STRING"),  # external system ID
      Property(name="submission_method", type="STRING"),  # edgar, email, portal, mail
      Property(name="submission_date", type="TIMESTAMP"),
      Property(name="acknowledgment_received", type="BOOLEAN"),
      Property(name="acknowledgment_date", type="TIMESTAMP"),
      Property(
        name="processing_status", type="STRING"
      ),  # received, processing, accepted, rejected, review
      Property(name="review_comments", type="STRING"),
      Property(name="resubmission_required", type="BOOLEAN"),
      Property(name="final_status", type="STRING"),  # accepted, rejected, withdrawn
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Deadline",
    description="Regulatory deadlines and milestones",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(
        name="deadline_type", type="STRING"
      ),  # filing, response, payment, review
      Property(name="deadline_date", type="STRING"),
      Property(name="reminder_date", type="STRING"),
      Property(name="reminder_sent", type="BOOLEAN"),
      Property(name="extended_deadline", type="STRING"),
      Property(name="extension_reason", type="STRING"),
      Property(name="status", type="STRING"),  # upcoming, met, missed, extended
      Property(name="impact_level", type="STRING"),  # low, medium, high, critical
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Compliance",
    description="Compliance assessments and status tracking",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(
        name="compliance_area", type="STRING"
      ),  # financial, environmental, safety, privacy
      Property(
        name="compliance_status", type="STRING"
      ),  # compliant, non_compliant, in_progress, unknown
      Property(name="assessment_date", type="STRING"),
      Property(name="next_assessment_date", type="STRING"),
      Property(name="risk_level", type="STRING"),  # low, medium, high, critical
      Property(name="remediation_required", type="BOOLEAN"),
      Property(name="remediation_deadline", type="STRING"),
      Property(name="compliance_score", type="DOUBLE"),  # 0-100
      Property(name="notes", type="STRING"),
      Property(name="assessed_by", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Review",
    description="Review processes for filings and compliance",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(
        name="review_type", type="STRING"
      ),  # internal, external, regulatory, audit
      Property(
        name="review_status", type="STRING"
      ),  # not_started, in_progress, completed, approved, rejected
      Property(name="reviewer", type="STRING"),
      Property(name="review_date", type="STRING"),
      Property(name="completed_date", type="STRING"),
      Property(name="findings", type="STRING"),
      Property(name="recommendations", type="STRING"),
      Property(name="action_required", type="BOOLEAN"),
      Property(name="follow_up_date", type="STRING"),
      Property(
        name="approval_level", type="STRING"
      ),  # manager, director, c_level, board
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Approval",
    description="Approval workflows and sign-offs",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(
        name="approval_type", type="STRING"
      ),  # filing, document, process, exception
      Property(
        name="approval_status", type="STRING"
      ),  # pending, approved, rejected, conditional
      Property(name="approver", type="STRING"),
      Property(name="approval_date", type="STRING"),
      Property(name="conditions", type="STRING"),
      Property(name="expiration_date", type="STRING"),
      Property(name="approval_notes", type="STRING"),
      Property(name="escalation_required", type="BOOLEAN"),
      Property(name="escalation_level", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="RegulatoryContact",
    description="Contacts at regulatory agencies",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="agency", type="STRING"),
      Property(name="department", type="STRING"),
      Property(name="role", type="STRING"),  # examiner, analyst, reviewer, supervisor
      Property(name="specialization", type="STRING"),
      Property(name="preferred_contact_method", type="STRING"),  # email, phone, portal
      Property(name="response_time_days", type="INT64"),
      Property(name="notes", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
]

# RoboReport Extension Relationships
EXTENSION_RELATIONSHIPS = [
  Relationship(
    name="ENTITY_SUBJECT_TO_REGULATION",
    from_node="Entity",
    to_node="Regulation",
    description="Entity subject to regulatory requirements",
    properties=[
      Property(name="applicability_start_date", type="STRING"),
      Property(name="applicability_end_date", type="STRING"),
      Property(name="exemption_status", type="STRING"),
    ],
  ),
  Relationship(
    name="REGULATION_REQUIRES_FILING",
    from_node="Regulation",
    to_node="Filing",
    description="Regulation mandates specific filing",
    properties=[
      Property(name="filing_context", type="STRING"),
    ],
  ),
  Relationship(
    name="FILING_HAS_SUBMISSION",
    from_node="Filing",
    to_node="Submission",
    description="Filing submitted through submission process",
    properties=[
      Property(name="submission_attempt", type="INT64"),  # 1st, 2nd attempt
    ],
  ),
  Relationship(
    name="FILING_HAS_DEADLINE",
    from_node="Filing",
    to_node="Deadline",
    description="Filing has regulatory deadlines",
    properties=[
      Property(name="deadline_context", type="STRING"),
    ],
  ),
  Relationship(
    name="FILING_REQUIRES_REVIEW",
    from_node="Filing",
    to_node="Review",
    description="Filing requires review process",
    properties=[
      Property(name="review_context", type="STRING"),
      Property(name="mandatory_review", type="BOOLEAN"),
    ],
  ),
  Relationship(
    name="REVIEW_REQUIRES_APPROVAL",
    from_node="Review",
    to_node="Approval",
    description="Review requires approval to proceed",
    properties=[
      Property(name="approval_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ENTITY_HAS_COMPLIANCE",
    from_node="Entity",
    to_node="Compliance",
    description="Entity compliance status and assessments",
    properties=[
      Property(name="compliance_context", type="STRING"),
    ],
  ),
  Relationship(
    name="COMPLIANCE_BASED_ON_REGULATION",
    from_node="Compliance",
    to_node="Regulation",
    description="Compliance assessment based on regulation",
    properties=[
      Property(name="assessment_scope", type="STRING"),
    ],
  ),
  Relationship(
    name="USER_REVIEWS_FILING",
    from_node="User",
    to_node="Filing",
    description="User responsible for filing review",
    properties=[
      Property(name="review_role", type="STRING"),  # preparer, reviewer, approver
      Property(name="responsibility_level", type="STRING"),
    ],
  ),
  Relationship(
    name="CONTACT_IS_REGULATORY_CONTACT",
    from_node="Contact",
    to_node="RegulatoryContact",
    description="Contact person at regulatory agency",
    properties=[
      Property(name="contact_context", type="STRING"),
    ],
  ),
  Relationship(
    name="FILING_COMMUNICATES_WITH_CONTACT",
    from_node="Filing",
    to_node="RegulatoryContact",
    description="Filing involves communication with regulatory contact",
    properties=[
      Property(
        name="communication_type", type="STRING"
      ),  # inquiry, submission, follow_up
      Property(name="communication_date", type="STRING"),
    ],
  ),
  Relationship(
    name="DOCUMENT_SUPPORTS_FILING",
    from_node="Document",
    to_node="Filing",
    description="Document supports filing submission",
    properties=[
      Property(name="document_role", type="STRING"),  # exhibit, schedule, cover_letter
      Property(name="required", type="BOOLEAN"),
    ],
  ),
  Relationship(
    name="DEADLINE_FOR_COMPLIANCE",
    from_node="Deadline",
    to_node="Compliance",
    description="Deadline related to compliance requirement",
    properties=[
      Property(name="compliance_context", type="STRING"),
    ],
  ),
  Relationship(
    name="SUBMISSION_RECEIVES_REVIEW",
    from_node="Submission",
    to_node="Review",
    description="Submission undergoes regulatory review",
    properties=[
      Property(name="review_stage", type="STRING"),  # initial, detailed, final
    ],
  ),
]
