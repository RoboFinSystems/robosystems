"""Write commands for Information Blocks — the generic construction entry.

One public command (:func:`create_information_block`) dispatches by
``block_type`` to the registered handler. No business logic lives here
— the command is pure routing. Domain-specific creation still happens
in the block type's handler module (``schedule.py`` for Phase a).

The command is mounted as the ``create-information-block`` CQRS
operation in ``routers/extensions/roboledger/operations.py``. The REST
registrar's error_map routes ``ValueError → 422``; Pydantic validation
errors raised by ``create_request_model.model_validate`` also surface
as 422 through FastAPI's default handling.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from robosystems.models.api.information_block import (
  CreateInformationBlockRequest,
  InformationBlockEnvelope,
)
from robosystems.operations.information_block import registry as registry_module


def create_information_block(
  session: Session,
  body: CreateInformationBlockRequest,
  created_by: str,
) -> InformationBlockEnvelope:
  """Create a block of the given type and return its full envelope.

  Shape-validates the opaque payload against the registry entry's
  ``create_request_model``. Raises :class:`ValueError` for an unknown
  ``block_type`` (→ 422 via the REST error map). Pydantic validation
  errors from the payload shape-check propagate directly.
  """
  try:
    entry = registry_module.get(body.block_type)
  except KeyError as exc:
    # Translate to ValueError so the registrar error_map (ValueError: 422)
    # and the MCP translator see a uniform failure mode.
    raise ValueError(str(exc)) from exc

  typed_payload = entry.create_request_model.model_validate(body.payload)
  structure_id = entry.dispatch_create(session, typed_payload, created_by)

  envelope = entry.dispatch_build_envelope(session, structure_id)
  if envelope is None:
    # The handler just created this block; build_envelope returning None
    # means the handler is buggy, not a real miss. Surface it as a
    # 500-shaped server error rather than a silent success.
    raise RuntimeError(
      f"dispatch_build_envelope returned None for freshly-created "
      f"{body.block_type} block {structure_id}"
    )
  return envelope


__all__ = [
  "create_information_block",
]
