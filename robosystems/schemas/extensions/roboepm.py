"""
RoboEPM Schema Extension for LadybugDB

Enterprise performance management, KPIs, budgets, and forecasts.
Extends the base schema with performance management-specific entities.
"""

from ..models import Node, Property, Relationship

# RoboEPM Extension Nodes
EXTENSION_NODES = [
  Node(
    name="KPI",
    description="Key Performance Indicators and metrics",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="kpi_code", type="STRING"),
      Property(name="kpi_name", type="STRING"),
      Property(name="description", type="STRING"),
      Property(
        name="kpi_category", type="STRING"
      ),  # financial, operational, customer, employee
      Property(
        name="measurement_unit", type="STRING"
      ),  # percentage, dollars, count, ratio
      Property(name="calculation_method", type="STRING"),
      Property(name="data_source", type="STRING"),  # manual, automated, calculated
      Property(
        name="frequency", type="STRING"
      ),  # daily, weekly, monthly, quarterly, annually
      Property(name="target_value", type="DOUBLE"),
      Property(name="threshold_green", type="DOUBLE"),  # excellent performance
      Property(name="threshold_yellow", type="DOUBLE"),  # acceptable performance
      Property(name="threshold_red", type="DOUBLE"),  # poor performance
      Property(name="higher_is_better", type="BOOLEAN"),
      Property(name="active", type="BOOLEAN"),
      Property(name="owner", type="STRING"),  # employee responsible for KPI
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Budget",
    description="Financial budgets and planning",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="budget_name", type="STRING"),
      Property(
        name="budget_type", type="STRING"
      ),  # operating, capital, project, department
      Property(name="budget_period", type="STRING"),  # FY2024, Q1_2024, monthly_202401
      Property(name="budget_status", type="STRING"),  # draft, approved, locked, revised
      Property(name="total_budget", type="DOUBLE"),
      Property(name="currency", type="STRING"),
      Property(name="approved_by", type="STRING"),
      Property(name="approval_date", type="STRING"),
      Property(name="effective_start_date", type="STRING"),
      Property(name="effective_end_date", type="STRING"),
      Property(name="revision_number", type="INT64"),
      Property(name="notes", type="STRING"),
      Property(name="created_by", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Forecast",
    description="Financial and operational forecasts",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="forecast_name", type="STRING"),
      Property(
        name="forecast_type", type="STRING"
      ),  # revenue, expense, cash_flow, headcount
      Property(
        name="forecast_method", type="STRING"
      ),  # bottom_up, top_down, statistical, regression
      Property(
        name="forecast_period", type="STRING"
      ),  # Q1_2024, FY2024, rolling_12_months
      Property(name="forecast_value", type="DOUBLE"),
      Property(name="confidence_level", type="DOUBLE"),  # 0-100 percentage
      Property(name="variance_from_budget", type="DOUBLE"),
      Property(name="variance_percentage", type="DOUBLE"),
      Property(name="assumptions", type="STRING"),
      Property(name="risk_factors", type="STRING"),
      Property(name="forecast_date", type="STRING"),
      Property(name="forecaster", type="STRING"),
      Property(name="approved", type="BOOLEAN"),
      Property(name="approved_by", type="STRING"),
      Property(name="approval_date", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Target",
    description="Performance targets and goals",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="target_name", type="STRING"),
      Property(
        name="target_type", type="STRING"
      ),  # revenue, cost, efficiency, quality, growth
      Property(name="target_period", type="STRING"),  # Q1_2024, FY2024, monthly_202401
      Property(name="target_value", type="DOUBLE"),
      Property(name="measurement_unit", type="STRING"),
      Property(name="baseline_value", type="DOUBLE"),
      Property(name="stretch_target", type="DOUBLE"),
      Property(name="minimum_acceptable", type="DOUBLE"),
      Property(name="weight", type="DOUBLE"),  # importance weighting 0-1
      Property(name="achievement_date", type="DATE"),
      Property(name="actual_value", type="DOUBLE"),
      Property(name="achievement_percentage", type="DOUBLE"),
      Property(
        name="status", type="STRING"
      ),  # not_started, in_progress, achieved, missed, exceeded
      Property(name="owner", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Metric",
    description="Actual performance measurements",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="metric_date", type="DATE"),
      Property(
        name="metric_period", type="STRING"
      ),  # daily, weekly, monthly, quarterly
      Property(name="actual_value", type="DOUBLE"),
      Property(name="target_value", type="DOUBLE"),
      Property(name="variance", type="DOUBLE"),
      Property(name="variance_percentage", type="DOUBLE"),
      Property(
        name="performance_status", type="STRING"
      ),  # excellent, good, acceptable, poor, critical
      Property(name="trend", type="STRING"),  # improving, stable, declining
      Property(name="data_quality", type="STRING"),  # high, medium, low
      Property(name="notes", type="STRING"),
      Property(name="measured_by", type="STRING"),
      Property(name="measurement_date", type="STRING"),
    ],
  ),
  Node(
    name="Dashboard",
    description="Performance dashboards and visualizations",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="dashboard_name", type="STRING"),
      Property(
        name="dashboard_type", type="STRING"
      ),  # executive, departmental, operational, project
      Property(name="description", type="STRING"),
      Property(name="layout_config", type="STRING"),  # JSON configuration
      Property(
        name="refresh_frequency", type="STRING"
      ),  # real_time, hourly, daily, weekly
      Property(name="auto_refresh", type="BOOLEAN"),
      Property(name="access_level", type="STRING"),  # public, private, restricted
      Property(name="owner", type="STRING"),
      Property(name="viewers", type="STRING"),  # JSON array of user IDs
      Property(name="active", type="BOOLEAN"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Scorecard",
    description="Balanced scorecards and performance summaries",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="scorecard_name", type="STRING"),
      Property(
        name="scorecard_type", type="STRING"
      ),  # balanced, operational, strategic, departmental
      Property(name="scoring_method", type="STRING"),  # weighted, equal, custom
      Property(name="total_score", type="DOUBLE"),
      Property(name="max_possible_score", type="DOUBLE"),
      Property(name="performance_rating", type="STRING"),  # excellent, good, fair, poor
      Property(name="period", type="STRING"),  # Q1_2024, FY2024
      Property(name="owner", type="STRING"),
      Property(name="reviewers", type="STRING"),  # JSON array of user IDs
      Property(name="status", type="STRING"),  # draft, published, archived
      Property(name="last_updated", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
  Node(
    name="Goal",
    description="Strategic goals and objectives",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="goal_name", type="STRING"),
      Property(
        name="goal_type", type="STRING"
      ),  # strategic, operational, project, personal
      Property(name="description", type="STRING"),
      Property(name="priority", type="STRING"),  # low, medium, high, critical
      Property(name="start_date", type="STRING"),
      Property(name="target_date", type="STRING"),
      Property(name="completion_date", type="STRING"),
      Property(
        name="status", type="STRING"
      ),  # not_started, in_progress, completed, cancelled
      Property(name="progress_percentage", type="DOUBLE"),
      Property(name="success_criteria", type="STRING"),
      Property(name="obstacles", type="STRING"),
      Property(name="owner", type="STRING"),
      Property(name="updated_at", type="STRING"),  # Keep as STRING to match base schema
    ],
  ),
]

# RoboEPM Extension Relationships
EXTENSION_RELATIONSHIPS = [
  Relationship(
    name="ENTITY_HAS_KPI",
    from_node="Entity",
    to_node="KPI",
    description="Entity tracks key performance indicators",
    properties=[
      Property(name="kpi_context", type="STRING"),
      Property(name="implementation_date", type="DATE"),
    ],
  ),
  Relationship(
    name="KPI_HAS_METRIC",
    from_node="KPI",
    to_node="Metric",
    description="KPI measured by specific metrics",
    properties=[
      Property(name="metric_context", type="STRING"),
    ],
  ),
  Relationship(
    name="KPI_HAS_TARGET",
    from_node="KPI",
    to_node="Target",
    description="KPI has performance targets",
    properties=[
      Property(name="target_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ENTITY_HAS_BUDGET",
    from_node="Entity",
    to_node="Budget",
    description="Entity financial budgets",
    properties=[
      Property(name="budget_context", type="STRING"),
    ],
  ),
  Relationship(
    name="BUDGET_HAS_FORECAST",
    from_node="Budget",
    to_node="Forecast",
    description="Budget based on forecasts",
    properties=[
      Property(name="forecast_context", type="STRING"),
      Property(name="variance_analysis", type="STRING"),
    ],
  ),
  Relationship(
    name="DASHBOARD_DISPLAYS_KPI",
    from_node="Dashboard",
    to_node="KPI",
    description="Dashboard visualizes KPIs",
    properties=[
      Property(name="widget_config", type="STRING"),  # JSON configuration
      Property(name="display_order", type="INT64"),
    ],
  ),
  Relationship(
    name="SCORECARD_INCLUDES_KPI",
    from_node="Scorecard",
    to_node="KPI",
    description="Scorecard includes KPIs",
    properties=[
      Property(name="weight", type="DOUBLE"),
      Property(name="score", type="DOUBLE"),
      Property(name="performance_rating", type="STRING"),
    ],
  ),
  Relationship(
    name="USER_OWNS_KPI",
    from_node="User",
    to_node="KPI",
    description="User responsible for KPI",
    properties=[
      Property(name="ownership_start_date", type="STRING"),
      Property(name="ownership_end_date", type="STRING"),
      Property(
        name="responsibility_level", type="STRING"
      ),  # primary, secondary, contributor
    ],
  ),
  Relationship(
    name="USER_OWNS_DASHBOARD",
    from_node="User",
    to_node="Dashboard",
    description="User owns dashboard",
    properties=[
      Property(name="ownership_type", type="STRING"),  # owner, editor, viewer
    ],
  ),
  Relationship(
    name="GOAL_SUPPORTS_KPI",
    from_node="Goal",
    to_node="KPI",
    description="Goal contributes to KPI achievement",
    properties=[
      Property(name="contribution_weight", type="DOUBLE"),
    ],
  ),
  Relationship(
    name="GOAL_HAS_TARGET",
    from_node="Goal",
    to_node="Target",
    description="Goal has measurable targets",
    properties=[
      Property(name="target_alignment", type="STRING"),
    ],
  ),
  Relationship(
    name="PERIOD_FOR_BUDGET",
    from_node="Period",
    to_node="Budget",
    description="Budget applies to time period",
    properties=[
      Property(name="budget_period_context", type="STRING"),
    ],
  ),
  Relationship(
    name="PERIOD_FOR_FORECAST",
    from_node="Period",
    to_node="Forecast",
    description="Forecast applies to time period",
    properties=[
      Property(name="forecast_period_context", type="STRING"),
    ],
  ),
  Relationship(
    name="PERIOD_FOR_METRIC",
    from_node="Period",
    to_node="Metric",
    description="Metric measured for time period",
    properties=[
      Property(name="measurement_period_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ENTITY_HAS_GOAL",
    from_node="Entity",
    to_node="Goal",
    description="Entity strategic goals",
    properties=[
      Property(name="goal_context", type="STRING"),
    ],
  ),
  Relationship(
    name="SCORECARD_TRACKS_GOAL",
    from_node="Scorecard",
    to_node="Goal",
    description="Scorecard tracks goal progress",
    properties=[
      Property(name="tracking_context", type="STRING"),
    ],
  ),
]
