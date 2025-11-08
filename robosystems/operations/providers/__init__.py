"""External service provider integrations."""

from .payment_provider import get_payment_provider, PaymentProvider

__all__ = ["get_payment_provider", "PaymentProvider"]
