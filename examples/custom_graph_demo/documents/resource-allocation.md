---
title: Resource Allocation & Staffing Guidelines
tags: staffing, allocation, hours, capacity, team
folder: project-management
---

# Resource Allocation & Staffing Guidelines

This document governs how team members are assigned to projects across the innovation consortium. Allocation is tracked in the knowledge graph via PERSON_WORKS_ON_PROJECT relationships with `hours_per_week` and `contribution` properties.

## Allocation Model

Each team member is employed by a single company (PERSON_WORKS_FOR_COMPANY) but may contribute to multiple projects across the consortium. Weekly hour commitments range from 4 to 28 hours per project.

### Capacity Limits

| Scenario | Max Projects | Max Total Hours/Week |
|---|---|---|
| Full-time on one project | 1 | 28 |
| Primary + secondary projects | 2 | 28 (e.g., 20 + 8) |
| Multi-project contributor | 3 | 28 (e.g., 16 + 8 + 4) |
| Advisory/consulting role | Up to 4 | 12 (4 hours each max) |

No individual should be assigned to more than 3 active projects simultaneously. Assignments to a 4th project require manager approval and are limited to advisory roles (4 hours/week max).

### Minimum Viable Allocation

- A meaningful contribution requires at least 4 hours/week
- Allocations below 4 hours are not tracked — use ad-hoc collaboration instead
- When reducing someone's hours on a project, either keep them at 4+ or remove the assignment entirely

## Contribution Types

Every project assignment includes a `contribution` type that defines the team member's primary responsibility:

### Design

Responsible for system architecture, UX/UI design, or solution design. Typically assigned to Systems Architects and Product Designers.

**Expected outputs:** Architecture documents, design specs, prototypes, technical decision records.

### Implementation

Responsible for building and coding the solution. Typically assigned to Robotics Engineers, AI Researchers, and Field Engineers.

**Expected outputs:** Working code, hardware integrations, deployed systems, technical documentation.

### Research

Responsible for exploration, feasibility analysis, and innovation. Typically assigned to Data Scientists and AI Researchers.

**Expected outputs:** Research findings, feasibility reports, proof-of-concept results, published papers.

### Testing

Responsible for quality assurance, validation, and verification. Can be assigned to any technical role.

**Expected outputs:** Test plans, test results, bug reports, performance benchmarks.

### Operations

Responsible for deployment, monitoring, and ongoing support. Typically assigned to Operations Leads and Field Engineers.

**Expected outputs:** Deployment plans, runbooks, monitoring dashboards, incident reports.

## Staffing Requirements by Project Theme

Different project themes have different staffing needs based on the nature of the work:

| Project Theme | Key Roles | Minimum Team |
|---|---|---|
| Autonomous Delivery | Robotics Engineer, AI Researcher, Operations Lead | 4 people |
| Predictive Maintenance | Data Scientist, Field Engineer, Systems Architect | 3 people |
| Climate Analytics | Data Scientist, Systems Architect, Business Analyst | 3 people |
| Personalized Learning | Product Designer, AI Researcher, Data Scientist | 3 people |
| Sustainable Energy | Field Engineer, Robotics Engineer, Operations Lead | 4 people |
| Robotic Process Automation | Robotics Engineer, Systems Architect, Business Analyst | 3 people |
| Supply Chain Visibility | Data Scientist, Business Analyst, Operations Lead | 3 people |
| Advanced Materials | AI Researcher, Robotics Engineer, Field Engineer | 4 people |
| Smart City Infrastructure | Systems Architect, Operations Lead, Project Manager | 4 people |

Every project must have at least one Project Manager assigned regardless of theme.

## Cross-Company Collaboration

When team members from different companies work on the same project, additional coordination is required:

### IP and Confidentiality

- Cross-company assignments require a signed collaboration agreement between the sponsoring companies
- Each company's IP contribution is documented in the sponsorship relationship
- Proprietary data stays within the originating company's systems unless explicitly shared

### Communication

- Cross-company projects hold a weekly sync meeting (minimum)
- A shared communication channel is created for all team members
- The Project Manager is responsible for bridging company cultures and processes

### Conflict Resolution

If a team member's home company and their project sponsor have conflicting priorities:
1. The team member's direct manager (home company) has final say on time allocation
2. The Project Manager escalates to the steering committee if the project is at risk
3. Reallocation of hours across projects is negotiated, not imposed

## Useful Graph Queries

### Team members with high allocation
```cypher
MATCH (p:Person)-[w:PERSON_WORKS_ON_PROJECT]->(proj:Project)
WHERE proj.status = 'ACTIVE'
WITH p, sum(w.hours_per_week) AS total_hours, count(proj) AS project_count
WHERE total_hours > 24 OR project_count > 2
RETURN p.name, p.title, total_hours, project_count
ORDER BY total_hours DESC
```

### Cross-company project teams
```cypher
MATCH (p1:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c1:Company),
      (p2:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c2:Company),
      (p1)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project),
      (p2)-[:PERSON_WORKS_ON_PROJECT]->(proj)
WHERE c1.identifier <> c2.identifier AND p1.identifier < p2.identifier
RETURN proj.name, c1.name AS company_a, c2.name AS company_b, count(*) AS collaborators
ORDER BY collaborators DESC
```

### Contribution type distribution
```cypher
MATCH (p:Person)-[w:PERSON_WORKS_ON_PROJECT]->(proj:Project)
RETURN w.contribution AS contribution_type, count(*) AS assignments, avg(w.hours_per_week) AS avg_hours
ORDER BY assignments DESC
```
