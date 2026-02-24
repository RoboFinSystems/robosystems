import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from robosystems.middleware.graph.streaming_wrapper import (
    StreamingRepositoryWrapper,
    add_streaming_support,
)


class TestStreamingRepositoryWrapper:
    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_streaming_with_async_generator(self):
        """Client.query returns an async generator that yields chunks."""
        chunks = [
            {
                "chunk_index": 0,
                "data": [{"a": 1}],
                "is_last_chunk": False,
                "row_count": 1,
                "total_rows_sent": 1,
            },
            {
                "chunk_index": 1,
                "data": [{"a": 2}],
                "is_last_chunk": True,
                "row_count": 1,
                "total_rows_sent": 2,
            },
        ]

        async def mock_async_gen():
            for chunk in chunks:
                yield chunk

        client = AsyncMock()
        client.graph_id = "kg_test"
        client.query = AsyncMock(return_value=mock_async_gen())

        wrapper = StreamingRepositoryWrapper(client)
        result_chunks = []
        async for chunk in wrapper.execute_query_streaming("MATCH (n) RETURN n"):
            result_chunks.append(chunk)

        assert len(result_chunks) == 2
        assert result_chunks[0]["chunk_index"] == 0
        assert result_chunks[1]["is_last_chunk"] is True
        assert "execution_time_ms" in result_chunks[1]

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_streaming_adds_chunk_index_if_missing(self):
        """Chunks without chunk_index get one assigned."""

        async def mock_async_gen():
            yield {"data": [{"a": 1}], "is_last_chunk": True, "total_rows_sent": 1}

        client = AsyncMock()
        client.graph_id = "kg_test"
        client.query = AsyncMock(return_value=mock_async_gen())

        wrapper = StreamingRepositoryWrapper(client)
        result_chunks = []
        async for chunk in wrapper.execute_query_streaming("MATCH (n) RETURN n"):
            result_chunks.append(chunk)

        assert result_chunks[0]["chunk_index"] == 0

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_streaming_adds_timing_to_last_chunk(self):
        """Last chunk gets execution_time_ms if not already present."""

        async def mock_async_gen():
            yield {
                "chunk_index": 0,
                "data": [{"a": 1}],
                "is_last_chunk": True,
                "total_rows_sent": 1,
            }

        client = AsyncMock()
        client.graph_id = "kg_test"
        client.query = AsyncMock(return_value=mock_async_gen())

        wrapper = StreamingRepositoryWrapper(client)
        result_chunks = []
        async for chunk in wrapper.execute_query_streaming("MATCH (n) RETURN n"):
            result_chunks.append(chunk)

        assert "execution_time_ms" in result_chunks[0]
        assert result_chunks[0]["execution_time_ms"] >= 0

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_streaming_preserves_existing_timing(self):
        """If chunk already has execution_time_ms, don't overwrite."""

        async def mock_async_gen():
            yield {
                "chunk_index": 0,
                "data": [],
                "is_last_chunk": True,
                "execution_time_ms": 42.0,
                "total_rows_sent": 0,
            }

        client = AsyncMock()
        client.graph_id = "kg_test"
        client.query = AsyncMock(return_value=mock_async_gen())

        wrapper = StreamingRepositoryWrapper(client)
        result_chunks = []
        async for chunk in wrapper.execute_query_streaming("MATCH (n) RETURN n"):
            result_chunks.append(chunk)

        assert result_chunks[0]["execution_time_ms"] == 42.0

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_streaming_error_yields_error_chunk(self):
        """Exception during streaming yields an error chunk."""
        client = AsyncMock()
        client.graph_id = "kg_test"
        client.query = AsyncMock(side_effect=RuntimeError("connection lost"))

        wrapper = StreamingRepositoryWrapper(client)
        result_chunks = []
        async for chunk in wrapper.execute_query_streaming("MATCH (n) RETURN n"):
            result_chunks.append(chunk)

        assert len(result_chunks) == 1
        assert result_chunks[0]["error"] == "connection lost"
        assert result_chunks[0]["error_type"] == "RuntimeError"
        assert result_chunks[0]["is_last_chunk"] is True
        assert result_chunks[0]["chunk_index"] == 0
        assert "execution_time_ms" in result_chunks[0]

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_fallback_to_convert_to_chunks(self):
        """Non-async result from client.query is converted to chunks."""
        # Return a plain list instead of async generator
        client = AsyncMock()
        client.graph_id = "kg_test"
        result_data = [{"name": "Alice"}, {"name": "Bob"}]
        client.query = AsyncMock(return_value=result_data)

        wrapper = StreamingRepositoryWrapper(client)
        result_chunks = []
        async for chunk in wrapper.execute_query_streaming(
            "MATCH (n) RETURN n", chunk_size=10
        ):
            result_chunks.append(chunk)

        assert len(result_chunks) == 1
        assert result_chunks[0]["data"] == result_data
        assert result_chunks[0]["is_last_chunk"] is True
        assert result_chunks[0]["columns"] == ["name"]

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_no_query_method_uses_execute_query(self):
        """Client without query method falls back to execute_query."""
        client = MagicMock(spec=[])
        client.execute_query = AsyncMock(
            return_value=[{"id": 1}, {"id": 2}]
        )

        wrapper = StreamingRepositoryWrapper(client)
        result_chunks = []
        async for chunk in wrapper.execute_query_streaming("MATCH (n) RETURN n"):
            result_chunks.append(chunk)

        assert len(result_chunks) == 1
        assert result_chunks[0]["row_count"] == 2

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_no_methods_raises_attribute_error(self):
        """Client with neither query nor execute_query raises."""
        client = MagicMock(spec=[])

        wrapper = StreamingRepositoryWrapper(client)
        with pytest.raises(AttributeError):
            async for _ in wrapper.execute_query_streaming("MATCH (n) RETURN n"):
                pass


class TestConvertToChunks:
    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_dict_result_with_data_key(self):
        client = MagicMock()
        wrapper = StreamingRepositoryWrapper(client)

        result = {
            "data": [{"a": 1}, {"a": 2}, {"a": 3}],
            "columns": ["a"],
        }

        chunks = []
        async for chunk in wrapper._convert_to_chunks(result, chunk_size=2, start_time=0):
            chunks.append(chunk)

        assert len(chunks) == 2
        assert chunks[0]["data"] == [{"a": 1}, {"a": 2}]
        assert chunks[0]["columns"] == ["a"]
        assert chunks[0]["is_last_chunk"] is False
        assert chunks[1]["data"] == [{"a": 3}]
        assert chunks[1]["columns"] == []  # Only first chunk has columns
        assert chunks[1]["is_last_chunk"] is True

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_list_result(self):
        client = MagicMock()
        wrapper = StreamingRepositoryWrapper(client)

        result = [{"x": 10}, {"x": 20}]

        chunks = []
        async for chunk in wrapper._convert_to_chunks(result, chunk_size=100, start_time=0):
            chunks.append(chunk)

        assert len(chunks) == 1
        assert chunks[0]["data"] == result
        assert chunks[0]["columns"] == ["x"]
        assert chunks[0]["is_last_chunk"] is True
        assert "execution_time_ms" in chunks[0]

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_unknown_format_yields_empty(self):
        client = MagicMock()
        wrapper = StreamingRepositoryWrapper(client)

        chunks = []
        async for chunk in wrapper._convert_to_chunks(
            "not a valid result", chunk_size=100, start_time=0
        ):
            chunks.append(chunk)

        # Empty data yields no chunks (range(0, 0, chunk_size) is empty)
        assert len(chunks) == 0

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_empty_list_yields_no_chunks(self):
        client = MagicMock()
        wrapper = StreamingRepositoryWrapper(client)

        chunks = []
        async for chunk in wrapper._convert_to_chunks([], chunk_size=100, start_time=0):
            chunks.append(chunk)

        assert len(chunks) == 0

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_chunk_index_increments(self):
        client = MagicMock()
        wrapper = StreamingRepositoryWrapper(client)

        result = [{"v": i} for i in range(5)]

        chunks = []
        async for chunk in wrapper._convert_to_chunks(result, chunk_size=2, start_time=0):
            chunks.append(chunk)

        assert len(chunks) == 3
        assert chunks[0]["chunk_index"] == 0
        assert chunks[1]["chunk_index"] == 1
        assert chunks[2]["chunk_index"] == 2
        assert chunks[0]["row_count"] == 2
        assert chunks[1]["row_count"] == 2
        assert chunks[2]["row_count"] == 1


class TestAddStreamingSupport:
    @pytest.mark.unit
    def test_client_already_has_streaming(self):
        client = MagicMock()
        client.execute_query_streaming = MagicMock()

        result = add_streaming_support(client)
        assert result is client

    @pytest.mark.unit
    def test_adds_streaming_method(self):
        client = MagicMock(spec=["execute_query"])
        assert not hasattr(client, "execute_query_streaming")

        result = add_streaming_support(client)
        assert hasattr(result, "execute_query_streaming")
        assert result is client
