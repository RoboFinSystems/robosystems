"""Create/update/delete Information Block commands: pure routing by
``block_type`` to the registered handler.

Errors map ``ValueError`` → 422, ``NotImplementedError`` → 501,
``ScheduleNotFoundError`` → 404.
"""

from __future__ import annotations

from pydantic import BaseModel
from sqlalchemy.orm import Session

from robosystems.models.api.information_block import (
  CreateInformationBlockRequest,
  DeleteInformationBlockRequest,
  DeleteInformationBlockResponse,
  InformationBlockEnvelope,
  UpdateInformationBlockRequest,
)
from robosystems.operations.information_block import registry as registry_module


def _get_entry_or_422(block_type: str):
  """Registry lookup with uniform ``ValueError → 422`` translation."""
  try:
    return registry_module.get(block_type)
  except KeyError as exc:
    raise ValueError(str(exc)) from exc


def _coerce_payload(request_model: type[BaseModel], payload):
  """Validate a dict ``payload`` against the request model; already-typed
  payloads pass through."""
  if isinstance(payload, BaseModel):
    return payload
  return request_model.model_validate(payload)


def create_information_block(
  session: Session,
  body: CreateInformationBlockRequest,
  created_by: str,
) -> InformationBlockEnvelope:
  """Create a block of the given type and return its full envelope."""
  entry = _get_entry_or_422(body.block_type)

  typed_payload = _coerce_payload(entry.create_request_model, body.payload)
  structure_id = entry.dispatch_create(session, typed_payload, created_by)

  envelope = entry.dispatch_build_envelope(session, structure_id)
  if envelope is None:
    # A just-created block must build; None is a handler bug.
    raise RuntimeError(
      f"dispatch_build_envelope returned None for freshly-created "
      f"{body.block_type} block {structure_id}"
    )
  return envelope


def update_information_block(
  session: Session,
  body: UpdateInformationBlockRequest,
  created_by: str,
) -> InformationBlockEnvelope:
  """Update a block in place and return the refreshed envelope.

  ``created_by`` is the registrar's kwarg name; handlers call it
  ``updated_by``.
  """
  entry = _get_entry_or_422(body.block_type)

  typed_payload = _coerce_payload(entry.update_request_model, body.payload)
  structure_id = entry.dispatch_update(session, typed_payload, created_by)

  envelope = entry.dispatch_build_envelope(session, structure_id)
  if envelope is None:
    raise RuntimeError(
      f"dispatch_build_envelope returned None for just-updated "
      f"{body.block_type} block {structure_id}"
    )
  return envelope


def delete_information_block(
  session: Session,
  body: DeleteInformationBlockRequest,
  created_by: str,
) -> DeleteInformationBlockResponse:
  """Delete a block; the response carries the name captured before removal."""
  entry = _get_entry_or_422(body.block_type)

  typed_payload = _coerce_payload(entry.delete_request_model, body.payload)

  # Build the envelope before deletion to capture the name. A missing
  # structure is left to dispatch_delete to reject.
  structure_id_attr = getattr(typed_payload, "structure_id", None)
  pre_delete_envelope: InformationBlockEnvelope | None = None
  if structure_id_attr is not None:
    pre_delete_envelope = entry.dispatch_build_envelope(session, structure_id_attr)

  deleted_id = entry.dispatch_delete(session, typed_payload, created_by)
  # name="" is reachable only if a delete succeeds on a missing structure.
  return DeleteInformationBlockResponse(
    deleted=True,
    structure_id=deleted_id,
    block_type=body.block_type,
    name=pre_delete_envelope.name if pre_delete_envelope else "",
  )


__all__ = [
  "create_information_block",
  "delete_information_block",
  "update_information_block",
]
