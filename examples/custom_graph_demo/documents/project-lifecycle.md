---
title: Project Lifecycle & Governance Policy
tags: governance, lifecycle, status, approval, projects
folder: project-management
---

# Project Lifecycle & Governance Policy

This document defines the stages, transitions, and governance requirements for all projects in the innovation consortium. Every project follows a four-stage lifecycle tracked in the knowledge graph via the `status` property on Project nodes.

## Stage Definitions

### PLANNING

The project has been proposed and is undergoing feasibility analysis. A sponsoring company has committed initial interest but full budget allocation has not been approved.

**Entry criteria:**
- Project proposal submitted with theme, estimated budget, and target timeline
- At least one sponsoring company identified (COMPANY_SPONSORS_PROJECT relationship created)
- Sponsorship level assigned (Strategic, Operational, Research, or Pilot)

**Activities:**
- Define scope, deliverables, and success metrics
- Identify required roles (Data Scientist, Systems Architect, Robotics Engineer, etc.)
- Estimate budget between $500K and $6M based on scope
- Assign a Project Manager as the primary point of contact
- Establish cross-company collaboration agreements if multiple sponsors

**Exit criteria:**
- Budget committed by sponsor(s) — recorded as `budget_committed` on the sponsorship relationship
- Core team of at least 3 people assigned (PERSON_WORKS_ON_PROJECT relationships created)
- Steering committee approval for projects over $2M

### ACTIVE

The project is in execution. Team members are actively contributing hours and deliverables are being produced.

**Entry criteria:**
- All PLANNING exit criteria met
- Team members have confirmed weekly hour allocations (4–28 hours/week per person)
- Each team member has an assigned contribution type: Design, Implementation, Research, Testing, or Operations

**Activities:**
- Weekly status updates from the Project Manager
- Monthly budget review against `budget_committed`
- Quarterly sponsor review for Strategic and Operational sponsorship levels
- Cross-company collaboration meetings for multi-sponsor projects

**Monitoring:**
- Track actual hours vs. allocated hours per team member
- Monitor budget burn rate against committed sponsorship amounts
- Identify resource conflicts where team members are assigned to 3+ concurrent projects

**Exit criteria (to COMPLETED):**
- All deliverables accepted by sponsors
- Final budget reconciliation completed
- Lessons learned documented

**Exit criteria (to ON_HOLD):**
- Sponsor requests pause due to budget constraints or strategic reprioritization
- Key resource unavailable (e.g., lead Robotics Engineer reassigned)
- External dependency blocking progress

### ON_HOLD

The project is temporarily paused. Team assignments remain in the graph but active contribution is suspended.

**Entry criteria:**
- Documented reason for hold (budget, resource, dependency, or strategic)
- Sponsor acknowledgment of hold status

**Activities:**
- Monthly check-in with sponsor to reassess readiness to resume
- Preserve team assignments — do not reassign team members unless hold exceeds 90 days
- After 90 days on hold, conduct formal review: resume as ACTIVE or transition to COMPLETED (cancelled)

**Exit criteria (to ACTIVE):**
- Blocking condition resolved
- Sponsor reconfirms budget commitment
- Team availability confirmed

### COMPLETED

The project has finished — either successfully delivered or formally cancelled after extended hold.

**Entry criteria:**
- All deliverables accepted, OR formal cancellation approved by sponsor
- Final budget reconciliation completed
- Team members released (hours_per_week effectively zero, but relationships preserved for history)

**Post-completion:**
- Project relationships remain in the graph for historical analysis
- Cross-company collaboration patterns are available for future project planning
- Budget actuals vs. committed amounts inform future estimation

## Stage Transitions

```
PLANNING ──────► ACTIVE ──────► COMPLETED
                   │                 ▲
                   ▼                 │
                ON_HOLD ─────────────┘
```

Only these transitions are valid:
- PLANNING → ACTIVE (approved and staffed)
- ACTIVE → COMPLETED (delivered or cancelled)
- ACTIVE → ON_HOLD (paused)
- ON_HOLD → ACTIVE (resumed)
- ON_HOLD → COMPLETED (cancelled after extended hold)

Projects cannot move backward from ACTIVE to PLANNING. If scope changes fundamentally, create a new project.

## Budget Governance

| Budget Range | Approval Required | Review Frequency |
|---|---|---|
| Under $1M | Sponsor company lead | Quarterly |
| $1M – $3M | Sponsor + consortium steering committee | Monthly |
| Over $3M | Full consortium board | Monthly + mid-project gate review |

Budget committed by sponsors (recorded on COMPANY_SPONSORS_PROJECT relationship) typically represents 40–90% of the total project budget. The remainder is covered by in-kind contributions (team member hours from sponsoring companies).

## Useful Graph Queries

### Projects by status
```cypher
MATCH (p:Project)
RETURN p.status AS status, count(p) AS project_count
ORDER BY project_count DESC
```

### Projects at risk (ON_HOLD or over budget)
```cypher
MATCH (c:Company)-[s:COMPANY_SPONSORS_PROJECT]->(p:Project)
WHERE p.status = 'ON_HOLD'
RETURN p.name, p.status, p.budget, s.budget_committed, c.name AS sponsor
```
