---
title: Cross-Company Collaboration Protocol
tags: collaboration, cross-company, partnerships, consortium, governance
folder: project-management
---

# Cross-Company Collaboration Protocol

This document defines the rules and best practices for collaboration across companies within the innovation consortium. The consortium currently includes companies across 9 industries: Robotics, Aerospace, Healthcare, Education, Entertainment, Energy, Logistics, Finance, and Agriculture.

## Consortium Structure

### Membership

Each company in the consortium is tracked as a Company node in the knowledge graph with properties including `industry`, `location`, and `founded_year`. Companies join the consortium to access shared talent, co-fund projects, and accelerate innovation through cross-industry partnerships.

### Governance

- **Steering Committee:** One representative per company, meets quarterly
- **Technical Council:** Senior technical staff (Systems Architects, AI Researchers) from each company, meets monthly
- **Project Boards:** Per-project governance, composition depends on sponsorship level

## Collaboration Models

### Single-Sponsor Projects

One company funds and directs the project. Team members from other companies may be assigned but the sponsoring company has decision authority.

- Sponsorship level: typically Strategic or Operational
- IP ownership: sponsor company
- Team members from other companies contribute under secondment agreements
- The sponsor's representative on the steering committee reports progress

### Multi-Sponsor Projects

Two or more companies co-fund a project. These are the most common arrangement for large initiatives ($3M+).

**Rules:**
- Lead sponsor is the company with the highest `budget_committed`
- Each sponsor designates a liaison (typically a Business Analyst or Project Manager)
- Decision-making follows a proportional model: vote weight scales with budget commitment
- Deadlocks are escalated to the steering committee
- IP is shared proportionally unless a specific agreement overrides

### Consortium-Wide Initiatives

Projects that benefit all members, typically in foundational infrastructure or research.

- Funded by equal contributions from all companies
- Governed by the steering committee directly
- Outputs are shared as consortium commons
- Examples: Smart City Infrastructure platforms, shared Climate Analytics datasets

## Team Assembly for Cross-Company Projects

### Staffing Process

1. Project Manager identifies required roles and contribution types (Design, Implementation, Research, Testing, Operations)
2. Each sponsoring company nominates candidates from their roster
3. Candidates are evaluated for interest alignment (interests property on Person nodes) and availability (hours_per_week across existing assignments)
4. Assignments are created with agreed hours_per_week (4–28) and contribution type
5. Cross-company team onboarding meeting held before work begins

### Location Considerations

Team members are distributed across locations: San Francisco, Austin, Seattle, Boston, Denver, New York, Atlanta, Chicago, Los Angeles, Portland, Pittsburgh, and Miami. Cross-company projects must account for timezone spread.

| Time Zone Spread | Meeting Policy |
|---|---|
| Same timezone | Daily standups acceptable |
| 1–2 hour spread (e.g., Pacific + Mountain) | Daily standups with flexible window |
| 3 hour spread (e.g., Pacific + Eastern) | Limit sync meetings to core hours (11am–3pm ET) |
| Full US spread (Pacific to Eastern) | Async-first with 2 sync meetings/week max |

### Communication Standards

- All cross-company projects use the consortium's shared communication platform
- Project-specific channels are created with access limited to assigned team members
- Sensitive discussions (budget, IP, strategic direction) happen in sponsor-only channels
- The Project Manager is responsible for cross-company communication bridging

## Intellectual Property

### Default IP Framework

| Sponsorship Level | IP Ownership | License to Consortium |
|---|---|---|
| Strategic | Sponsor company | Limited — specific use cases only |
| Operational | Sponsor company | Broad — operational use by all members |
| Research | Shared (all sponsors) | Full — consortium commons |
| Pilot | Sponsor company | None until converted to full project |

### Data Sharing

- Each company's proprietary data remains within their systems
- Derived insights and aggregated results may be shared per project agreement
- The knowledge graph contains only non-proprietary metadata (company name, industry, location, team assignments)
- Raw project outputs (code, models, datasets) follow the IP framework above

## Conflict Resolution

### Priority Conflicts

When a team member's home company needs them back and the project sponsor disagrees:

1. **First 2 weeks:** Project Manager and home company manager negotiate informally
2. **Week 3:** Escalate to both companies' steering committee representatives
3. **Week 4:** Steering committee makes binding decision, considering project criticality and team member replaceability

### Budget Disputes

When sponsors disagree on budget allocation or spending priorities:

1. Review original sponsorship agreement and committed amounts
2. Lead sponsor proposes resolution
3. If rejected, steering committee mediates
4. Unresolved disputes result in project moving to ON_HOLD status until agreement is reached

### Quality Disputes

When sponsors disagree on whether deliverables meet acceptance criteria:

1. Technical Council reviews the deliverables against the original specification
2. Technical Council recommendation is advisory
3. Sponsors vote proportionally (by budget commitment)
4. If no majority, lead sponsor's position prevails

## Measuring Collaboration Effectiveness

### Key Metrics (queryable from the graph)

- **Cross-company project count:** How many projects involve team members from 2+ companies
- **Collaboration density:** Average number of cross-company pairs per project
- **Industry diversity:** Number of distinct industries represented per project
- **Resource sharing ratio:** Percentage of team members working outside their home company's sponsored projects

### Useful Graph Queries

#### Cross-company collaboration summary
```cypher
MATCH (p1:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c1:Company),
      (p2:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c2:Company),
      (p1)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project),
      (p2)-[:PERSON_WORKS_ON_PROJECT]->(proj)
WHERE c1.identifier <> c2.identifier AND p1.identifier < p2.identifier
RETURN proj.name AS project,
       count(DISTINCT c1) + count(DISTINCT c2) AS companies_involved,
       count(*) AS cross_company_pairs
ORDER BY cross_company_pairs DESC
```

#### Companies that collaborate most frequently
```cypher
MATCH (p1:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c1:Company),
      (p2:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c2:Company),
      (p1)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project),
      (p2)-[:PERSON_WORKS_ON_PROJECT]->(proj)
WHERE c1.identifier < c2.identifier
RETURN c1.name AS company_a, c1.industry AS industry_a,
       c2.name AS company_b, c2.industry AS industry_b,
       count(DISTINCT proj) AS shared_projects
ORDER BY shared_projects DESC
```

#### Industry diversity per project
```cypher
MATCH (p:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c:Company),
      (p)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project)
RETURN proj.name, collect(DISTINCT c.industry) AS industries, count(DISTINCT c.industry) AS industry_count
ORDER BY industry_count DESC
```
