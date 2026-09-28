"""Every tag an operation uses is declared in config/openapi_tags.py.

An undeclared tag still generates, but it lands at the end of the reference
navigation with no description, and a tag becomes a Python SDK module path
and a published docs URL, so it is settled once a release ships it.
"""

from robosystems.config.openapi_tags import MAIN_API_TAGS


def test_every_operation_tag_is_declared(client):
  spec = client.get("/openapi.json").json()
  declared = {tag["name"] for tag in MAIN_API_TAGS}
  used = {
    tag
    for path in spec["paths"].values()
    for operation in path.values()
    if isinstance(operation, dict)
    for tag in operation.get("tags", [])
  }
  assert used - declared == set()
