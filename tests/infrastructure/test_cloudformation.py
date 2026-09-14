"""CloudFormation template validation tests.

Catches issues like template size limits before deployment.
"""

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CFN_DIR = REPO_ROOT / "cloudformation"
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

# AWS CloudFormation template size limits (bytes)
# https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cloudformation-limits.html
#
# Two different ceilings apply depending on how a template reaches CloudFormation:
# passed inline in the API request (--template-body), or fetched from S3
# (--template-url). Which one a template gets is not a property of the template —
# it is a property of how the deploy workflow invokes it, so that is what this
# test reads rather than a hand-maintained list that would drift.
CFN_TEMPLATE_BODY_LIMIT = 51_200
CFN_TEMPLATE_S3_LIMIT = 1_048_576

# --template-body file://cloudformation/<name>.yaml
_INLINE_RE = re.compile(r"--template-body\s+file://cloudformation/([\w.-]+)\.yaml")
# The S3 path cannot be read off the --template-url flag: the URL is a step
# output built at deploy time, so the flag carries no template name. The upload
# is the identifying signal — `aws s3 cp cloudformation/<name>.yaml` — and it
# only counts as an S3 deploy if that same workflow also passes --template-url.
_UPLOAD_RE = re.compile(r"s3\s+cp\s+cloudformation/([\w.-]+)\.yaml")
_URL_FLAG = "--template-url"


def _templates_by_deploy_mechanism() -> tuple[set[str], set[str]]:
  """Return (inline, s3) template stems as the deploy workflows actually invoke them."""
  inline: set[str] = set()
  s3: set[str] = set()
  for workflow in WORKFLOW_DIR.glob("*.yml"):
    text = workflow.read_text()
    inline.update(_INLINE_RE.findall(text))
    if _URL_FLAG in text:
      s3.update(_UPLOAD_RE.findall(text))
  return inline, s3


@pytest.mark.unit
class TestCloudFormationTemplateSizes:
  """Ensure each CloudFormation template fits the limit its deploy path imposes."""

  @pytest.fixture
  def templates(self):
    return sorted(CFN_DIR.glob("*.yaml"))

  def test_templates_exist(self, templates):
    """Sanity check that we found templates to validate."""
    assert len(templates) > 0, f"No .yaml templates found in {CFN_DIR}"

  def test_deploy_mechanisms_are_discoverable(self):
    """The regexes must actually match the workflows, or every check below passes vacuously."""
    inline, s3 = _templates_by_deploy_mechanism()
    assert inline, (
      "No --template-body invocations found in .github/workflows. Either the "
      "deploy workflows changed shape or the parsing here is stale — this test "
      "cannot enforce anything until it can see how templates are deployed."
    )
    assert not (inline & s3), (
      f"Templates deployed both inline and from S3: {sorted(inline & s3)}. One "
      f"deploy path per template, or the applicable size limit is ambiguous."
    )

  @pytest.mark.parametrize(
    "template",
    sorted(CFN_DIR.glob("*.yaml")),
    ids=lambda p: p.name,
  )
  def test_template_under_size_limit(self, template):
    """Each template must fit the limit for the mechanism its workflow uses.

    Templates no workflow deploys (or that are deployed by hand) are held to the
    inline limit — it is the stricter of the two, and reaching for S3 should be a
    deliberate change to a deploy workflow rather than something a template
    drifts into.
    """
    inline, s3 = _templates_by_deploy_mechanism()
    stem = template.stem

    if stem in s3:
      limit, mechanism, remedy = (
        CFN_TEMPLATE_S3_LIMIT,
        "--template-url (S3)",
        "Split the stack — this template is past what S3 accepts too.",
      )
    else:
      limit, mechanism, remedy = (
        CFN_TEMPLATE_BODY_LIMIT,
        "--template-body (inline)",
        "Trim verbose outputs, or upload it to S3 and switch its workflow to "
        "--template-url (see the api.yaml upload step in deploy-api.yml).",
      )

    size = template.stat().st_size
    assert size <= limit, (
      f"{template.name} is {size:,} bytes, exceeding the {limit:,} byte "
      f"{mechanism} limit by {size - limit:,} bytes. {remedy}"
    )


# ---------------------------------------------------------------------------
# Alarm-to-emitter dimension agreement
# ---------------------------------------------------------------------------
#
# CloudWatch matches an alarm to a metric on the EXACT dimension set. An alarm
# whose dimensions differ from what the emitter publishes resolves to no stream
# at all and, under TreatMissingData: notBreaching, reads OK forever. That is not
# hypothetical here: `robosystems-graph-{env}-database-capacity` was dimensionless
# against a [NodeType=writer] emitter and sat OK from 2025-12-23 to 2026-09-14,
# through a fleet that filled up and refused a signup. The same shape had already
# been found on AllocationFailures.
#
# cfn-lint cannot see this — the template is valid YAML either way — so it is
# guarded here instead, by reading the dimension names out of the emitter source
# and checking every graph alarm against them.

import ast  # noqa: E402

EMITTER_SOURCES = (
  REPO_ROOT / "robosystems" / "operations" / "graph" / "infrastructure.py",
  REPO_ROOT / "robosystems" / "middleware" / "graph" / "allocation_manager.py",
)
ALARM_TEMPLATES = ("graph-infra", "graph-ladybug")


def _literal_dimension_names(dims_node: ast.AST) -> frozenset[str] | None:
  """Dimension `Name` values from a `Dimensions` list literal, or None if dynamic."""
  if not isinstance(dims_node, ast.List):
    return None
  names: set[str] = set()
  for element in dims_node.elts:
    if not isinstance(element, ast.Dict):
      return None
    for key, value in zip(element.keys, element.values, strict=False):
      if (
        isinstance(key, ast.Constant)
        and key.value == "Name"
        and isinstance(value, ast.Constant)
      ):
        names.add(value.value)
  return frozenset(names)


def _published_dimension_sets() -> dict[str, set[frozenset[str]]]:
  """Map metric name -> the dimension-name sets the emitters publish it under."""
  published: dict[str, set[frozenset[str]]] = {}
  for source in EMITTER_SOURCES:
    tree = ast.parse(source.read_text())
    for node in ast.walk(tree):
      if not isinstance(node, ast.Dict):
        continue
      entries = {
        key.value: value
        for key, value in zip(node.keys, node.values, strict=False)
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
      }
      name_node = entries.get("MetricName")
      if not isinstance(name_node, ast.Constant) or not isinstance(
        name_node.value, str
      ):
        continue
      dims_node = entries.get("Dimensions")
      dims = frozenset() if dims_node is None else _literal_dimension_names(dims_node)
      if dims is None:  # built dynamically; nothing static to assert against
        continue
      published.setdefault(name_node.value, set()).add(dims)
  return published


def _template_alarms(stem: str) -> list[tuple[str, str, frozenset[str]]]:
  """(&resource, metric name, dimension names) for each alarm in a template.

  Read with a regex rather than a YAML loader: these templates are full of
  CloudFormation short tags (!Sub, !Ref, !GetAtt) that safe_load rejects, and a
  tag-tolerant loader is more machinery than this needs.
  """
  text = (CFN_DIR / f"{stem}.yaml").read_text()
  alarms: list[tuple[str, str, frozenset[str]]] = []
  for block_match in re.finditer(
    r"^  (\w+):\n    Type: AWS::CloudWatch::Alarm\n(.*?)(?=^  \w+:\n    Type:|\Z)",
    text,
    re.MULTILINE | re.DOTALL,
  ):
    resource, body = block_match.group(1), block_match.group(2)
    metric_match = re.search(r"^      MetricName:\s*(\S+)\s*$", body, re.MULTILINE)
    if not metric_match:  # metric-math / Metrics Insights alarm
      continue
    dims_match = re.search(
      r"^      Dimensions:\n((?:^        .*\n)+)", body, re.MULTILINE
    )
    names = (
      frozenset(re.findall(r"- Name:\s*(\S+)", dims_match.group(1)))
      if dims_match
      else frozenset()
    )
    alarms.append((resource, metric_match.group(1), names))
  return alarms


@pytest.mark.unit
class TestAlarmDimensionsMatchEmitters:
  """Every graph alarm must watch a dimension set its emitter actually publishes."""

  def test_emitters_and_alarms_are_both_discoverable(self):
    """Guard the guard: a parser that silently finds nothing would pass everything."""
    assert "TenantSlotsFree" in _published_dimension_sets()
    found = [a for stem in ALARM_TEMPLATES for a in _template_alarms(stem)]
    assert len(found) >= 5, f"alarm parser found only {len(found)} alarms"

  def test_every_alarm_dimension_set_is_published(self):
    published = _published_dimension_sets()
    mismatches = []
    for stem in ALARM_TEMPLATES:
      for resource, metric, dims in _template_alarms(stem):
        if metric not in published:
          continue  # AWS-owned metric (EC2, ASG, ELB); not ours to match
        if dims not in published[metric]:
          mismatches.append(
            f"{stem}.yaml {resource}: alarm watches {metric} on "
            f"{sorted(dims) or '[] (no dimensions)'}, but the emitter publishes it "
            f"only on {[sorted(d) or '[]' for d in published[metric]]}"
          )
    assert not mismatches, "alarm/emitter dimension mismatch:\n" + "\n".join(mismatches)

  def test_tenant_slots_free_alarm_targets_standard(self):
    """The capacity floor alarm must be per-tier, not fleet-wide."""
    alarms = {r: (m, d) for r, m, d in _template_alarms("graph-infra")}
    metric, dims = alarms["TenantSlotsFreeStandardAlarm"]
    assert metric == "TenantSlotsFree"
    assert dims == frozenset({"ClusterTier"})

  def test_database_capacity_alarm_is_dimensioned(self):
    """Regression: this alarm was dimensionless against a [NodeType] emitter."""
    alarms = {r: (m, d) for r, m, d in _template_alarms("graph-infra")}
    metric, dims = alarms["DatabaseCapacityAlarm"]
    assert metric == "DatabaseUtilizationPercent"
    assert dims == frozenset({"NodeType"})
