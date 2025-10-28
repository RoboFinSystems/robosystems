#!/usr/bin/env python3
"""
Generate Custom Graph Demo Data (Generic Graph Version)

This script produces parquet files for a people/companies/projects knowledge graph:

Node tables (written to data/nodes):
  - Person.parquet
  - Company.parquet
  - Project.parquet

Relationship tables (written to data/relationships):
  - PERSON_WORKS_FOR_COMPANY.parquet
  - PERSON_WORKS_ON_PROJECT.parquet
  - COMPANY_SPONSORS_PROJECT.parquet

Usage:
    uv run 03_generate_data.py
    uv run 03_generate_data.py --count 120
    uv run 03_generate_data.py --regenerate
    uv run 03_generate_data.py --seed 1234
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


DATA_DIR = Path(__file__).parent / "data"
NODES_DIR = DATA_DIR / "nodes"
RELATIONSHIPS_DIR = DATA_DIR / "relationships"
DEFAULT_CREDENTIALS_FILE = Path(__file__).parent / "credentials" / "config.json"

# Reasonable defaults for the demo
DEFAULT_PERSON_COUNT = 60
MIN_COMPANIES = 4
MIN_PROJECTS = 6


@dataclass
class Company:
  identifier: str
  name: str
  industry: str
  location: str
  founded_year: int


@dataclass
class Project:
  identifier: str
  name: str
  status: str
  budget: float
  start_date: str
  end_date: str
  sponsor_company: str


@dataclass
class Person:
  identifier: str
  name: str
  age: int
  title: str
  interests: str
  location: str
  works_for: str
  start_date: str


class CustomGraphDataGenerator:
  """Generate nodes and relationships for the custom generic graph demo."""

  def __init__(
    self,
    person_count: int,
    regenerate: bool,
    seed: int | None,
    credentials_path: Path = DEFAULT_CREDENTIALS_FILE,
  ):
    self.person_count = max(6, person_count)
    self.company_count = max(MIN_COMPANIES, math.ceil(self.person_count / 8))
    self.project_count = max(MIN_PROJECTS, math.ceil(self.person_count / 10))

    self.nodes_dir = NODES_DIR
    self.relationships_dir = RELATIONSHIPS_DIR
    self.nodes_dir.mkdir(parents=True, exist_ok=True)
    self.relationships_dir.mkdir(parents=True, exist_ok=True)
    self.credentials_path = credentials_path

    credentials = self._load_credentials()
    self.graph_id = credentials.get("graph_id")
    if not self.graph_id:
      raise RuntimeError(
        "Graph ID not found. Run 02_create_graph.py before generating data."
      )

    if seed is None:
      # Derive a deterministic seed from the graph_id for repeatable datasets
      seed = sum(ord(char) for char in self.graph_id) % (2**32)
    self.random = random.Random(seed)

    if regenerate:
      self._cleanup_existing_files()

    self._companies: list[Company] = []
    self._projects: list[Project] = []
    self._people: list[Person] = []

  def _load_credentials(self) -> dict:
    if not self.credentials_path.exists():
      raise RuntimeError(
        f"Credentials not found at {self.credentials_path}. "
        "Run 01_setup_credentials.py and 02_create_graph.py first."
      )
    with self.credentials_path.open() as fh:
      return json.load(fh)

  def _cleanup_existing_files(self) -> None:
    """Remove previously generated parquet files."""
    print("\n🧹 Cleaning previous data files...")
    for directory in (self.nodes_dir, self.relationships_dir):
      for parquet_file in directory.glob("*.parquet"):
        parquet_file.unlink()

  def _write_parquet(self, records: pd.DataFrame, output_path: Path, label: str) -> None:
    """Write a pandas DataFrame to parquet with logs."""
    table = pa.Table.from_pandas(records, preserve_index=False)
    pq.write_table(table, output_path)
    relative = output_path.relative_to(Path(__file__).parent)
    print(f"   ✅ {label}: {len(records):,} rows -> {relative}")

  def _random_choice(self, items: Iterable[str]) -> str:
    items = list(items)
    return items[self.random.randrange(len(items))]

  def _random_date(self, start_year: int = 2014, end_year: int = 2024) -> str:
    """Return a random ISO date string between Jan 1 start_year and Dec 31 end_year."""
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta_days = (end - start).days
    day_offset = self.random.randrange(delta_days)
    return (start + timedelta(days=day_offset)).isoformat()

  def _generate_companies(self) -> list[Company]:
    industries = [
      "Robotics",
      "Aerospace",
      "Healthcare",
      "Education",
      "Entertainment",
      "Energy",
      "Logistics",
      "Finance",
      "Agriculture",
    ]
    locations = [
      "San Francisco, CA",
      "Austin, TX",
      "Seattle, WA",
      "Boston, MA",
      "Denver, CO",
      "New York, NY",
      "Atlanta, GA",
      "Chicago, IL",
      "Los Angeles, CA",
    ]

    self.random.shuffle(industries)
    companies: list[Company] = []
    for idx in range(self.company_count):
      identifier = f"company_{idx + 1:03d}"
      industry = industries[idx % len(industries)]
      name = f"{industry} Innovators {idx + 1}"
      company = Company(
        identifier=identifier,
        name=name,
        industry=industry,
        location=self._random_choice(locations),
        founded_year=self.random.randint(2005, 2022),
      )
      companies.append(company)
    return companies

  def _generate_projects(self, companies: list[Company]) -> list[Project]:
    statuses = ["PLANNING", "ACTIVE", "ON_HOLD", "COMPLETED"]
    project_themes = [
      "Autonomous Delivery",
      "Predictive Maintenance",
      "Climate Analytics",
      "Personalized Learning",
      "Sustainable Energy",
      "Robotic Process Automation",
      "Supply Chain Visibility",
      "Advanced Materials",
      "Smart City Infrastructure",
    ]
    projects: list[Project] = []
    for idx in range(self.project_count):
      identifier = f"project_{idx + 1:03d}"
      theme = project_themes[idx % len(project_themes)]
      sponsor = self._random_choice(companies).identifier
      start = self._random_date(2018, 2024)
      end_year = min(int(start[:4]) + self.random.randint(0, 3), 2025)
      end = self._random_date(end_year, 2026)
      project = Project(
        identifier=identifier,
        name=f"{theme} Initiative {idx + 1}",
        status=self._random_choice(statuses),
        budget=round(self.random.uniform(0.5, 6.0), 2) * 1_000_000,
        start_date=start,
        end_date=end,
        sponsor_company=sponsor,
      )
      projects.append(project)
    return projects

  def _generate_people(self, companies: list[Company]) -> list[Person]:
    titles = [
      "Robotics Engineer",
      "Systems Architect",
      "Data Scientist",
      "Project Manager",
      "Product Designer",
      "AI Researcher",
      "Operations Lead",
      "Business Analyst",
      "Field Engineer",
    ]
    interest_pool = [
      "Robotics",
      "Machine Learning",
      "Computer Vision",
      "IoT",
      "Supply Chain",
      "Healthcare",
      "Aerospace",
      "Sustainability",
      "Edge Computing",
      "AR/VR",
      "Human Factors",
      "Automation",
    ]
    cities = [
      "San Francisco, CA",
      "Austin, TX",
      "Los Angeles, CA",
      "Portland, OR",
      "Pittsburgh, PA",
      "Chicago, IL",
      "Miami, FL",
      "Denver, CO",
      "Boston, MA",
      "Atlanta, GA",
    ]

    people: list[Person] = []
    for idx in range(self.person_count):
      identifier = f"person_{idx + 1:03d}"
      company = self._random_choice(companies)
      interests = self.random.sample(interest_pool, k=3)
      person = Person(
        identifier=identifier,
        name=f"{self._random_choice(['Alex', 'Jordan', 'Taylor', 'Morgan', 'Casey', 'Riley', 'Charlie', 'Jamie', 'Cameron', 'Peyton'])} "
        f"{self._random_choice(['Lee', 'Nguyen', 'Garcia', 'Patel', 'Johnson', 'Singh', 'Taylor', 'Khan', 'Silva', 'Kim'])}",
        age=self.random.randint(24, 60),
        title=self._random_choice(titles),
        interests=json.dumps(interests),
        location=self._random_choice(cities),
        works_for=company.identifier,
        start_date=self._random_date(2015, 2024),
      )
      people.append(person)
    return people

  def generate_nodes(self) -> None:
    """Create node parquet files for Person, Company, and Project."""
    print("\n👥 Generating node tables...")
    self._companies = self._generate_companies()
    self._projects = self._generate_projects(self._companies)
    self._people = self._generate_people(self._companies)

    company_df = pd.DataFrame([company.__dict__ for company in self._companies])
    project_df = pd.DataFrame([project.__dict__ for project in self._projects])
    person_df = pd.DataFrame([person.__dict__ for person in self._people])

    self._write_parquet(company_df, self.nodes_dir / "Company.parquet", "Company")
    self._write_parquet(project_df, self.nodes_dir / "Project.parquet", "Project")
    self._write_parquet(person_df, self.nodes_dir / "Person.parquet", "Person")

  def generate_relationships(self) -> None:
    """Create relationship parquet files based on generated nodes."""
    if not (self._companies and self._projects and self._people):
      raise RuntimeError("Nodes must be generated before relationships.")

    print("\n🔗 Generating relationship tables...")

    works_for_records = []
    for person in self._people:
      works_for_records.append(
        {
          "from": person.identifier,
          "to": person.works_for,
          "role": person.title,
          "started_on": person.start_date,
        }
      )

    works_on_records = []
    for person in self._people:
      assignments = self.random.sample(
        self._projects, k=self.random.randint(1, min(3, len(self._projects)))
      )
      for project in assignments:
        works_on_records.append(
          {
            "from": person.identifier,
            "to": project.identifier,
            "hours_per_week": self.random.randint(4, 28),
            "contribution": self._random_choice(
              ["Design", "Implementation", "Research", "Testing", "Operations"]
            ),
          }
        )

    sponsors_records = []
    for project in self._projects:
      sponsors_records.append(
        {
          "from": project.sponsor_company,
          "to": project.identifier,
          "sponsorship_level": self._random_choice(
            ["Strategic", "Operational", "Research", "Pilot"]
          ),
          "budget_committed": round(project.budget * self.random.uniform(0.4, 0.9), 2),
        }
      )

    works_for_df = pd.DataFrame(works_for_records)
    works_on_df = pd.DataFrame(works_on_records)
    sponsors_df = pd.DataFrame(sponsors_records)

    self._write_parquet(
      works_for_df,
      self.relationships_dir / "PERSON_WORKS_FOR_COMPANY.parquet",
      "PERSON_WORKS_FOR_COMPANY",
    )
    self._write_parquet(
      works_on_df,
      self.relationships_dir / "PERSON_WORKS_ON_PROJECT.parquet",
      "PERSON_WORKS_ON_PROJECT",
    )
    self._write_parquet(
      sponsors_df,
      self.relationships_dir / "COMPANY_SPONSORS_PROJECT.parquet",
      "COMPANY_SPONSORS_PROJECT",
    )

  def run(self) -> None:
    self.generate_nodes()
    self.generate_relationships()


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(
    description="Generate parquet files for the custom generic graph demo"
  )
  parser.add_argument(
    "--count",
    type=int,
    default=DEFAULT_PERSON_COUNT,
    help=f"Number of people to generate (default: {DEFAULT_PERSON_COUNT})",
  )
  parser.add_argument(
    "--regenerate",
    action="store_true",
    help="Remove existing parquet files before generating new data",
  )
  parser.add_argument(
    "--seed",
    type=int,
    help="Optional random seed (overrides graph-based seed)",
  )
  parser.add_argument(
    "--credentials-file",
    default=str(DEFAULT_CREDENTIALS_FILE),
    help="Path to credentials file (default: credentials/config.json)",
  )
  return parser.parse_args()


def main() -> None:
  args = parse_args()
  try:
    generator = CustomGraphDataGenerator(
      person_count=args.count,
      regenerate=args.regenerate,
      seed=args.seed,
      credentials_path=Path(args.credentials_file).expanduser(),
    )
    generator.run()
    print("\n✅ Data generation complete!")
  except Exception as exc:  # noqa: BLE001
    print(f"\n❌ Data generation failed: {exc}")
    sys.exit(1)


if __name__ == "__main__":
  main()
