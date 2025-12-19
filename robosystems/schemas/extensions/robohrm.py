"""
RoboHRM Schema Extension for LadybugDB

Human resource management, employees, payroll, and benefits.
Extends the base schema with HR-specific entities.
"""

from ..models import Node, Property, Relationship

# RoboHRM Extension Nodes
EXTENSION_NODES = [
  Node(
    name="Employee",
    description="Entity employees and workforce",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="employee_number", type="STRING"),
      Property(name="first_name", type="STRING"),
      Property(name="last_name", type="STRING"),
      Property(name="full_name", type="STRING"),
      Property(name="email", type="STRING"),
      Property(name="phone", type="STRING"),
      Property(name="hire_date", type="STRING"),
      Property(name="termination_date", type="STRING"),
      Property(
        name="employment_status", type="STRING"
      ),  # active, terminated, on_leave, suspended
      Property(
        name="employment_type", type="STRING"
      ),  # full_time, part_time, contractor, intern
      Property(name="work_location", type="STRING"),  # remote, office, hybrid
      Property(name="manager_id", type="STRING"),
      Property(name="salary", type="DOUBLE"),
      Property(name="hourly_rate", type="DOUBLE"),
      Property(name="currency", type="STRING"),
      Property(
        name="pay_frequency", type="STRING"
      ),  # weekly, bi_weekly, monthly, annually
      Property(name="tax_id", type="STRING"),  # SSN or equivalent
      Property(name="emergency_contact", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Department",
    description="Organizational departments and divisions",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="department_code", type="STRING"),
      Property(name="department_name", type="STRING"),
      Property(name="description", type="STRING"),
      Property(name="cost_center", type="STRING"),
      Property(name="budget", type="DOUBLE"),
      Property(name="head_of_department", type="STRING"),  # employee_id
      Property(name="parent_department_id", type="STRING"),
      Property(name="active", type="BOOLEAN"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Position",
    description="Job positions and roles within organization",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="position_code", type="STRING"),
      Property(name="job_title", type="STRING"),
      Property(name="job_description", type="STRING"),
      Property(
        name="job_level", type="STRING"
      ),  # entry, junior, senior, lead, manager, director
      Property(
        name="job_family", type="STRING"
      ),  # engineering, sales, marketing, operations
      Property(name="minimum_salary", type="DOUBLE"),
      Property(name="maximum_salary", type="DOUBLE"),
      Property(name="required_skills", type="STRING"),
      Property(name="preferred_skills", type="STRING"),
      Property(name="education_requirement", type="STRING"),
      Property(name="experience_years", type="INT64"),
      Property(name="reports_to_position", type="STRING"),
      Property(name="active", type="BOOLEAN"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Payroll",
    description="Payroll processing and compensation",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="payroll_period", type="STRING"),  # 2024-01-15, 2024-01-31
      Property(name="pay_date", type="DATE"),
      Property(name="gross_pay", type="DOUBLE"),
      Property(name="net_pay", type="DOUBLE"),
      Property(name="federal_tax", type="DOUBLE"),
      Property(name="state_tax", type="DOUBLE"),
      Property(name="social_security", type="DOUBLE"),
      Property(name="medicare", type="DOUBLE"),
      Property(name="insurance_deduction", type="DOUBLE"),
      Property(name="retirement_contribution", type="DOUBLE"),
      Property(name="other_deductions", type="DOUBLE"),
      Property(name="overtime_hours", type="DOUBLE"),
      Property(name="overtime_pay", type="DOUBLE"),
      Property(name="bonus", type="DOUBLE"),
      Property(name="commission", type="DOUBLE"),
      Property(name="currency", type="STRING"),
      Property(name="processed_date", type="DATE"),
      Property(name="processed_by", type="STRING"),
    ],
  ),
  Node(
    name="Benefit",
    description="Employee benefits and compensation packages",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="benefit_code", type="STRING"),
      Property(name="benefit_name", type="STRING"),
      Property(
        name="benefit_type", type="STRING"
      ),  # health, dental, vision, life, disability, retirement
      Property(
        name="benefit_category", type="STRING"
      ),  # insurance, time_off, retirement, wellness
      Property(name="provider", type="STRING"),
      Property(name="description", type="STRING"),
      Property(name="employee_cost", type="DOUBLE"),
      Property(name="employer_cost", type="DOUBLE"),
      Property(
        name="coverage_level", type="STRING"
      ),  # employee, employee_spouse, family
      Property(name="waiting_period_days", type="INT64"),
      Property(name="active", type="BOOLEAN"),
      Property(name="effective_date", type="STRING"),
      Property(name="termination_date", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Performance",
    description="Employee performance reviews and evaluations",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="review_period", type="STRING"),  # Q1_2024, Annual_2023
      Property(
        name="review_type", type="STRING"
      ),  # annual, quarterly, probationary, project
      Property(name="review_date", type="STRING"),
      Property(name="reviewer", type="STRING"),  # employee_id of reviewer
      Property(name="overall_rating", type="DOUBLE"),  # 1-5 scale
      Property(name="goals_met", type="BOOLEAN"),
      Property(name="strengths", type="STRING"),
      Property(name="areas_for_improvement", type="STRING"),
      Property(name="career_development_goals", type="STRING"),
      Property(name="promotion_ready", type="BOOLEAN"),
      Property(name="raise_recommended", type="BOOLEAN"),
      Property(name="raise_percentage", type="DOUBLE"),
      Property(name="next_review_date", type="STRING"),
      Property(name="comments", type="STRING"),
      Property(name="employee_comments", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Training",
    description="Training programs and certifications",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="training_code", type="STRING"),
      Property(name="training_name", type="STRING"),
      Property(
        name="training_type", type="STRING"
      ),  # mandatory, optional, certification, skill_development
      Property(
        name="training_category", type="STRING"
      ),  # compliance, technical, leadership, safety
      Property(name="provider", type="STRING"),
      Property(name="duration_hours", type="DOUBLE"),
      Property(name="cost", type="DOUBLE"),
      Property(
        name="delivery_method", type="STRING"
      ),  # online, in_person, virtual, self_paced
      Property(name="completion_required", type="BOOLEAN"),
      Property(name="certification_earned", type="STRING"),
      Property(name="expiration_date", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="TimeOff",
    description="Time off requests and leave management",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(
        name="time_off_type", type="STRING"
      ),  # vacation, sick, personal, bereavement, jury_duty
      Property(name="start_date", type="STRING"),
      Property(name="end_date", type="STRING"),
      Property(name="total_days", type="DOUBLE"),
      Property(
        name="approval_status", type="STRING"
      ),  # pending, approved, rejected, cancelled
      Property(name="approved_by", type="STRING"),
      Property(name="approval_date", type="STRING"),
      Property(name="reason", type="STRING"),
      Property(name="paid", type="BOOLEAN"),
      Property(name="balance_before", type="DOUBLE"),
      Property(name="balance_after", type="DOUBLE"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
]

# RoboHRM Extension Relationships
EXTENSION_RELATIONSHIPS = [
  Relationship(
    name="EMPLOYEE_IS_USER",
    from_node="Employee",
    to_node="User",
    description="Employee has system user account",
    properties=[
      Property(name="account_creation_date", type="DATE"),
      Property(name="access_level", type="STRING"),
    ],
  ),
  Relationship(
    name="EMPLOYEE_HAS_CONTACT",
    from_node="Employee",
    to_node="Contact",
    description="Employee contact information",
    properties=[
      Property(
        name="contact_relationship", type="STRING"
      ),  # self, emergency, reference
      Property(name="is_primary", type="BOOLEAN"),
    ],
  ),
  Relationship(
    name="EMPLOYEE_IN_DEPARTMENT",
    from_node="Employee",
    to_node="Department",
    description="Employee assigned to department",
    properties=[
      Property(name="start_date", type="STRING"),
      Property(name="end_date", type="STRING"),
      Property(name="allocation_percentage", type="DOUBLE"),  # for shared employees
    ],
  ),
  Relationship(
    name="EMPLOYEE_HAS_POSITION",
    from_node="Employee",
    to_node="Position",
    description="Employee holds specific position",
    properties=[
      Property(name="start_date", type="STRING"),
      Property(name="end_date", type="STRING"),
      Property(name="promotion", type="BOOLEAN"),
    ],
  ),
  Relationship(
    name="EMPLOYEE_REPORTS_TO_EMPLOYEE",
    from_node="Employee",
    to_node="Employee",
    description="Employee reporting relationships",
    properties=[
      Property(name="reporting_start_date", type="DATE"),
      Property(name="reporting_end_date", type="DATE"),
      Property(name="reporting_type", type="STRING"),  # direct, dotted_line, project
    ],
  ),
  Relationship(
    name="EMPLOYEE_HAS_PAYROLL",
    from_node="Employee",
    to_node="Payroll",
    description="Employee payroll records",
    properties=[
      Property(name="payroll_context", type="STRING"),
    ],
  ),
  Relationship(
    name="EMPLOYEE_ENROLLED_IN_BENEFIT",
    from_node="Employee",
    to_node="Benefit",
    description="Employee benefit enrollment",
    properties=[
      Property(name="enrollment_date", type="DATE"),
      Property(name="effective_date", type="STRING"),
      Property(name="termination_date", type="STRING"),
      Property(name="coverage_level", type="STRING"),
      Property(name="employee_contribution", type="DOUBLE"),
    ],
  ),
  Relationship(
    name="EMPLOYEE_HAS_PERFORMANCE",
    from_node="Employee",
    to_node="Performance",
    description="Employee performance reviews",
    properties=[
      Property(name="performance_context", type="STRING"),
    ],
  ),
  Relationship(
    name="EMPLOYEE_COMPLETED_TRAINING",
    from_node="Employee",
    to_node="Training",
    description="Employee training completion",
    properties=[
      Property(name="enrollment_date", type="DATE"),
      Property(name="completion_date", type="STRING"),
      Property(name="score", type="DOUBLE"),
      Property(name="certification_number", type="STRING"),
      Property(name="expiration_date", type="STRING"),
    ],
  ),
  Relationship(
    name="EMPLOYEE_HAS_TIME_OFF",
    from_node="Employee",
    to_node="TimeOff",
    description="Employee time off requests",
    properties=[
      Property(name="time_off_context", type="STRING"),
    ],
  ),
  Relationship(
    name="DEPARTMENT_HAS_POSITION",
    from_node="Department",
    to_node="Position",
    description="Department contains positions",
    properties=[
      Property(name="position_allocation", type="STRING"),  # full, shared, project
    ],
  ),
  Relationship(
    name="ENTITY_HAS_EMPLOYEE",
    from_node="Entity",
    to_node="Employee",
    description="Entity employs workers",
    properties=[
      Property(name="employment_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ENTITY_HAS_DEPARTMENT",
    from_node="Entity",
    to_node="Department",
    description="Entity organizational structure",
    properties=[
      Property(name="department_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ENTITY_OFFERS_BENEFIT",
    from_node="Entity",
    to_node="Benefit",
    description="Entity benefit offerings",
    properties=[
      Property(name="benefit_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ENTITY_PROVIDES_TRAINING",
    from_node="Entity",
    to_node="Training",
    description="Entity training programs",
    properties=[
      Property(name="training_context", type="STRING"),
    ],
  ),
  Relationship(
    name="EMPLOYEE_HAS_ADDRESS",
    from_node="Employee",
    to_node="Address",
    description="Employee addresses",
    properties=[
      Property(name="address_type", type="STRING"),  # home, mailing, emergency
    ],
  ),
  Relationship(
    name="DEPARTMENT_HAS_ADDRESS",
    from_node="Department",
    to_node="Address",
    description="Department office locations",
    properties=[
      Property(name="address_type", type="STRING"),  # office, satellite, remote
    ],
  ),
]
