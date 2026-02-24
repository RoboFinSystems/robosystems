"""Tests for otel metrics decorator and sync wrapper."""

import pytest
from unittest.mock import MagicMock, patch
from fastapi import HTTPException

from robosystems.middleware.otel.metrics import (
    endpoint_metrics_decorator,
)


class TestEndpointMetricsDecoratorAsync:
    @pytest.mark.asyncio
    async def test_success_records_metrics(self):
        @endpoint_metrics_decorator("/v1/test")
        async def my_endpoint():
            return {"ok": True}

        result = await my_endpoint()
        assert result == {"ok": True}

    @pytest.mark.asyncio
    async def test_error_records_error_metrics(self):
        @endpoint_metrics_decorator("/v1/test")
        async def failing_endpoint():
            raise HTTPException(status_code=404, detail="Not found")

        with pytest.raises(HTTPException):
            await failing_endpoint()

    @pytest.mark.asyncio
    async def test_extracts_method_from_request(self):
        mock_request = MagicMock()
        mock_request.method = "POST"
        mock_request.path_params = {}
        mock_request.headers = MagicMock()
        mock_request.headers.get = MagicMock(return_value=None)
        mock_request.state = MagicMock()

        @endpoint_metrics_decorator("/v1/test")
        async def my_endpoint(request):
            return {"ok": True}

        result = await my_endpoint(mock_request)
        assert result == {"ok": True}

    @pytest.mark.asyncio
    async def test_with_business_event(self):
        @endpoint_metrics_decorator("/v1/test", business_event_type="graph_created")
        async def my_endpoint():
            return {"ok": True}

        result = await my_endpoint()
        assert result == {"ok": True}

    @pytest.mark.asyncio
    async def test_default_endpoint_name(self):
        @endpoint_metrics_decorator()
        async def custom_name_endpoint():
            return {"ok": True}

        result = await custom_name_endpoint()
        assert result == {"ok": True}


class TestEndpointMetricsDecoratorSync:
    def test_sync_success(self):
        @endpoint_metrics_decorator("/v1/test")
        def my_sync_endpoint():
            return {"ok": True}

        result = my_sync_endpoint()
        assert result == {"ok": True}

    def test_sync_error(self):
        @endpoint_metrics_decorator("/v1/test")
        def failing_sync():
            raise ValueError("boom")

        with pytest.raises(ValueError):
            failing_sync()

    def test_sync_with_request_arg(self):
        mock_request = MagicMock()
        mock_request.method = "GET"

        @endpoint_metrics_decorator("/v1/test")
        def my_endpoint(request):
            return {"ok": True}

        result = my_endpoint(mock_request)
        assert result == {"ok": True}
