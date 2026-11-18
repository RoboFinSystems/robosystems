"""External asset spec for transactional email, which the API sends itself: each
send is reported here so the Assets tab keeps the record a job run used to be."""

from dagster import AssetSpec

from robosystems.operations.notifications import EMAIL_SENT_ASSET_KEY

transactional_email_sent_source = AssetSpec(
  key=EMAIL_SENT_ASSET_KEY,
  description=(
    "Transactional emails sent directly by the API. A send that fails there "
    "runs as send_email_job instead and is recorded as that run."
  ),
  group_name="notifications",
  metadata={
    "pipeline": "notifications",
    "stage": "email",
  },
  kinds={"email"},
)
