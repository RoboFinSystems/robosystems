"""Write commands for Information Blocks — the generic construction entry.

Three public commands dispatch by ``block_type`` to the registered
handler: :func:`create_information_block`,
:func:`update_information_block`, :func:`delete_information_block`.
No business logic lives here — the commands are pure routing.
Domain-specific mutation still happens in the block type's handler
module (``schedule.py`` for the Schedule family; ``statement.py`` for
the statement family, which raises ``NotImplementedError``).

The commands are mounted as the ``create-information-block``,
``update-information-block``, and ``delete-information-block`` CQRS
operations in ``routers/extensions/roboledger/operations.py``. The REST
registrar's error_map routes ``ValueError → 422``,
``NotImplementedError → 501``, and the Schedule-domain
``ScheduleNotFoundError → 404``. Pydantic validation errors raised by
the per-type request model validation also surface as 422 through
FastAPI's default handling.
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
  """Validate ``payload`` against the registry entry's request model.

  Already-typed payloads (BaseModel instances supplied by a typed
  discriminated-union arm at the API boundary) are returned as-is —
  the API-level validation already checked them against the right
  shape. Dict payloads from untyped arms are validated here.
  """
  if isinstance(payload, BaseModel):
    return payload
  return request_model.model_validate(payload)


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
  entry = _get_entry_or_422(body.block_type)

  typed_payload = _coerce_payload(entry.create_request_model, body.payload)
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


def update_information_block(
  session: Session,
  body: UpdateInformationBlockRequest,
  created_by: str,
) -> InformationBlockEnvelope:
  """Update a block in place and return the refreshed envelope.

  Mirrors :func:`create_information_block` dispatch flow. Block types
  whose Structures are library-seeded and immutable (the statement
  family) surface :class:`NotImplementedError` → HTTP 501.

  The ``created_by`` kwarg name matches the registrar convention (see
  :class:`~robosystems.middleware.extensions.OperationSpec`), which
  passes ``created_by=str(user.id)`` for every registered op. It's
  propagated positionally to ``dispatch_update`` where the handler
  still names its parameter ``updated_by`` for semantic clarity.
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
  """Delete a block and return a thin confirmation response.

  The envelope is gone once the block is deleted, so the response
  carries just ``deleted=True`` + the structure_id, block_type, and
  name captured before removal — enough for audit trails and client
  bookkeeping.
  """
  entry = _get_entry_or_422(body.block_type)

  typed_payload = _coerce_payload(entry.delete_request_model, body.payload)

  # Build the envelope BEFORE deletion so we can surface name + id on
  # the response. build_envelope returning None here means the payload
  # pointed at a missing or wrong-type structure — translate to the
  # registrar's 404 via the domain's not-found exception when possible,
  # otherwise surface as ValueError → 422.
  structure_id_attr = getattr(typed_payload, "structure_id", None)
  pre_delete_envelope: InformationBlockEnvelope | None = None
  if structure_id_attr is not None:
    pre_delete_envelope = entry.dispatch_build_envelope(session, structure_id_attr)

  deleted_id = entry.dispatch_delete(session, typed_payload, created_by)
  # name="" only reaches callers when dispatch_delete succeeds without a
  # pre-delete envelope — no registered block type does that (Schedule's
  # delete raises ScheduleNotFoundError on missing rows; statement types
  # raise NotImplementedError). A block type with an idempotent delete
  # would need a better default here.
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
