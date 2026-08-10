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
