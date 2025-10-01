"""Adapters for external services and schemas."""

from .ses import SESEmailService, ses_service

# Keep the old names for backward compatibility during migration
sns_service = ses_service
SNSEmailService = SESEmailService

__all__ = ["SESEmailService", "ses_service", "sns_service", "SNSEmailService"]
