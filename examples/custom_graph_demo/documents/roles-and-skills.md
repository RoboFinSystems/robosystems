---
title: Roles, Skills & Career Development Guide
tags: roles, skills, career, hiring, interests, titles
folder: project-management
---

# Roles, Skills & Career Development Guide

This document defines the roles within the innovation consortium, their responsibilities, typical project assignments, and career development paths. Role assignments are tracked in the knowledge graph via Person node properties (`title`, `interests`) and relationship properties (`contribution` on PERSON_WORKS_ON_PROJECT, `role` on PERSON_WORKS_FOR_COMPANY).

## Role Definitions

### Robotics Engineer

**Focus:** Physical systems, hardware integration, autonomous systems, and embedded software.

**Typical project themes:** Autonomous Delivery, Robotic Process Automation, Advanced Materials, Sustainable Energy.

**Typical contributions:** Implementation, Testing.

**Key interests:** Robotics, IoT, Automation, Edge Computing.

**Career path:** Robotics Engineer → Senior Robotics Engineer → Systems Architect → Technical Director.

### Systems Architect

**Focus:** End-to-end system design, technical architecture, integration patterns, and infrastructure decisions.

**Typical project themes:** Smart City Infrastructure, Predictive Maintenance, Supply Chain Visibility.

**Typical contributions:** Design, Implementation.

**Key interests:** IoT, Edge Computing, Automation, Supply Chain.

**Career path:** Systems Architect → Principal Architect → Technical Director → CTO.

### Data Scientist

**Focus:** Statistical analysis, machine learning model development, data pipeline design, and insight generation.

**Typical project themes:** Climate Analytics, Predictive Maintenance, Supply Chain Visibility, Personalized Learning.

**Typical contributions:** Research, Implementation, Design.

**Key interests:** Machine Learning, Computer Vision, Healthcare, Sustainability.

**Career path:** Data Scientist → Senior Data Scientist → AI Researcher → Chief Data Officer.

### Project Manager

**Focus:** Project coordination, stakeholder management, timeline tracking, and team facilitation.

**Typical project themes:** All — every project requires at least one Project Manager.

**Typical contributions:** Operations, Design (process design).

**Key interests:** Varies — PMs are generalists who develop domain expertise through project assignments.

**Career path:** Project Manager → Senior PM → Program Manager → VP of Operations.

### Product Designer

**Focus:** User experience, interface design, prototyping, and user research.

**Typical project themes:** Personalized Learning, Smart City Infrastructure, Robotic Process Automation.

**Typical contributions:** Design, Research, Testing.

**Key interests:** AR/VR, Human Factors, Automation.

**Career path:** Product Designer → Senior Designer → Design Lead → VP of Product.

### AI Researcher

**Focus:** Novel algorithm development, model architecture, and pushing the boundaries of what's possible with AI/ML.

**Typical project themes:** Personalized Learning, Predictive Maintenance, Autonomous Delivery, Climate Analytics.

**Typical contributions:** Research, Implementation.

**Key interests:** Machine Learning, Computer Vision, Robotics, Edge Computing.

**Career path:** AI Researcher → Senior Researcher → Research Lead → Chief Scientist.

### Operations Lead

**Focus:** Deployment, monitoring, reliability, and operational efficiency of delivered systems.

**Typical project themes:** Smart City Infrastructure, Sustainable Energy, Autonomous Delivery, Supply Chain Visibility.

**Typical contributions:** Operations, Testing.

**Key interests:** Automation, IoT, Supply Chain, Edge Computing.

**Career path:** Operations Lead → Senior Operations → Director of Operations → COO.

### Business Analyst

**Focus:** Requirements gathering, data analysis, business case development, and stakeholder communication.

**Typical project themes:** Supply Chain Visibility, Robotic Process Automation, Climate Analytics.

**Typical contributions:** Research, Design, Operations.

**Key interests:** Supply Chain, Healthcare, Sustainability, Human Factors.

**Career path:** Business Analyst → Senior Analyst → Product Manager → VP of Strategy.

### Field Engineer

**Focus:** On-site deployment, hardware installation, field testing, and customer-facing technical support.

**Typical project themes:** Autonomous Delivery, Sustainable Energy, Advanced Materials, Predictive Maintenance.

**Typical contributions:** Implementation, Testing, Operations.

**Key interests:** Robotics, IoT, Aerospace, Edge Computing.

**Career path:** Field Engineer → Senior Field Engineer → Operations Lead → Technical Director.

## Interest Areas and Project Matching

Team members list their interests which inform project assignments. The consortium tracks 12 interest areas:

| Interest | Related Project Themes | Common Roles |
|---|---|---|
| Robotics | Autonomous Delivery, Robotic Process Automation | Robotics Engineer, Field Engineer |
| Machine Learning | Predictive Maintenance, Climate Analytics, Personalized Learning | Data Scientist, AI Researcher |
| Computer Vision | Autonomous Delivery, Smart City Infrastructure | AI Researcher, Robotics Engineer |
| IoT | Smart City Infrastructure, Predictive Maintenance | Systems Architect, Operations Lead |
| Supply Chain | Supply Chain Visibility, Autonomous Delivery | Business Analyst, Operations Lead |
| Healthcare | Personalized Learning, Climate Analytics | Data Scientist, Business Analyst |
| Aerospace | Advanced Materials, Autonomous Delivery | Robotics Engineer, Field Engineer |
| Sustainability | Sustainable Energy, Climate Analytics | Data Scientist, Business Analyst |
| Edge Computing | Smart City Infrastructure, Autonomous Delivery | Systems Architect, Robotics Engineer |
| AR/VR | Personalized Learning, Smart City Infrastructure | Product Designer, AI Researcher |
| Human Factors | Personalized Learning, Robotic Process Automation | Product Designer, Business Analyst |
| Automation | Robotic Process Automation, Supply Chain Visibility | Systems Architect, Operations Lead |

When staffing a new project, prioritize team members whose interests overlap with the project theme — they contribute more effectively and develop faster.

## Useful Graph Queries

### People by role and company
```cypher
MATCH (p:Person)-[w:PERSON_WORKS_FOR_COMPANY]->(c:Company)
RETURN p.title AS role, c.name AS company, count(p) AS headcount
ORDER BY role, company
```

### Find people with specific interests for project staffing
```cypher
MATCH (p:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c:Company)
WHERE p.interests ILIKE '%Machine Learning%'
RETURN p.name, p.title, c.name AS company, p.interests
ORDER BY p.title
```

### Role distribution across projects
```cypher
MATCH (p:Person)-[w:PERSON_WORKS_ON_PROJECT]->(proj:Project)
RETURN p.title AS role, w.contribution, count(*) AS assignments
ORDER BY role, assignments DESC
```
