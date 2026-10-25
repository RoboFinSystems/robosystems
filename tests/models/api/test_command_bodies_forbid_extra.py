"""A write command whose fields all default refuses a body that names none
of them, so a request built for another endpoint cannot run it."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.billing.subscription import CancelSubscriptionRequest
from robosystems.models.api.extensions.agent import (
  AcceptSuggestionsRequest,
  LearnClassificationDefaultsRequest,
)
from robosystems.models.api.extensions.fiscal_calendar import InitializeLedgerRequest
from robosystems.models.api.extensions.schedules import PromoteObligationsRequest
from robosystems.models.api.graphs.backups import BackupCreateRequest
from robosystems.models.api.graphs.connections import SyncConnectionRequest
from robosystems.models.api.graphs.operations import IndexDocumentOp, MaterializeOp
from robosystems.models.api.operations import OperationResumeRequest

FOREIGN_BODY = {"query": "query Q { x }", "variables": {}, "operationName": "Q"}

MODELS = [
  AcceptSuggestionsRequest,
  BackupCreateRequest,
  CancelSubscriptionRequest,
  IndexDocumentOp,
  InitializeLedgerRequest,
  LearnClassificationDefaultsRequest,
  MaterializeOp,
  OperationResumeRequest,
  PromoteObligationsRequest,
  SyncConnectionRequest,
]


@pytest.mark.unit
@pytest.mark.parametrize("model", MODELS, ids=lambda m: m.__name__)
def test_foreign_body_is_refused(model):
  with pytest.raises(ValidationError):
    model.model_validate(FOREIGN_BODY)


@pytest.mark.unit
@pytest.mark.parametrize("model", MODELS, ids=lambda m: m.__name__)
def test_an_empty_body_still_means_the_defaults(model):
  model.model_validate({})
