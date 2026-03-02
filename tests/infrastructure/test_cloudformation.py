"""CloudFormation template validation tests.

Catches issues like template size limits before deployment.
"""

from pathlib import Path

import pytest

CFN_DIR = Path(__file__).resolve().parents[2] / "cloudformation"

# AWS CloudFormation template body size limit (bytes)
# https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cloudformation-limits.html
CFN_TEMPLATE_BODY_LIMIT = 51_200


@pytest.mark.unit
class TestCloudFormationTemplateSizes:
  """Ensure all CloudFormation templates stay under the inline body size limit."""

  @pytest.fixture
  def templates(self):
    return sorted(CFN_DIR.glob("*.yaml"))

  def test_templates_exist(self, templates):
    """Sanity check that we found templates to validate."""
    assert len(templates) > 0, f"No .yaml templates found in {CFN_DIR}"

  @pytest.mark.parametrize(
    "template",
    sorted(CFN_DIR.glob("*.yaml")),
    ids=lambda p: p.name,
  )
  def test_template_under_size_limit(self, template):
    """Each template must be under the 51,200 byte inline body limit."""
    size = template.stat().st_size
    headroom = CFN_TEMPLATE_BODY_LIMIT - size
    assert size <= CFN_TEMPLATE_BODY_LIMIT, (
      f"{template.name} is {size:,} bytes, exceeding the "
      f"{CFN_TEMPLATE_BODY_LIMIT:,} byte limit by {-headroom:,} bytes. "
      f"Trim verbose outputs or switch to --template-url with S3."
    )
