"""A stored PDF's text layer, read for `DocumentService.read_file_text`.

Runs as its own process (``python -I pdf_text.py <request>``): the PDF on
stdin, the request as JSON in argv, the pages as JSON on stdout. It imports
nothing from robosystems, so it runs without the application on its path.
"""

from __future__ import annotations

import io
import json
import logging
import os
import resource
import sys
import warnings
from typing import Any, BinaryIO, TextIO

UNREADABLE = 3


def cap_memory(memory_bytes: int) -> None:
  """Bound this process's address space; macOS does not enforce the limit."""
  try:
    resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
  except (ValueError, OSError):
    pass


def lower_priority() -> None:
  """Yield the CPU to the process that started this one."""
  try:
    os.nice(10)
  except OSError:
    pass


def read_pages(data: bytes, request: dict[str, Any]) -> dict[str, Any]:
  from pypdf import PdfReader, apply_configuration

  stream = request["stream_bytes"]
  with apply_configuration(
    zlib_maximum_output_length=stream,
    lzw_maximum_output_length=stream,
    run_length_maximum_output_length=stream,
    brotli_maximum_output_length=stream,
    jbig2_maximum_output_length=stream,
    array_based_stream_maximum_output_length=stream,
    image_maximum_buffer_size=stream,
    stream_decoding_work_maximum_length=4 * stream,
  ):
    reader = PdfReader(io.BytesIO(data))
    page_count = len(reader.pages)
    first = request["first_page"]
    last = min(page_count, first + request["max_pages"] - 1)
    budget = request["chars_per_read"]
    pages: list[tuple[int, str]] = []
    for number in range(first, last + 1):
      if budget <= 0:
        break
      text = reader.pages[number - 1].extract_text() or ""
      text = text[: min(request["chars_per_page"], budget)]
      budget -= len(text)
      pages.append((number, text))
  return {"page_count": page_count, "pages": pages}


def serve(request: dict[str, Any], stdin: BinaryIO, stdout: TextIO) -> int:
  # A malformed PDF fails in many ways (a bad xref, a cyclic object, a stream
  # past its limit); every one means the file is unreadable.
  try:
    result = read_pages(stdin.read(), request)
  except Exception as exc:
    sys.stderr.write(repr(exc)[:500])
    return UNREADABLE
  json.dump(result, stdout)
  return 0


if __name__ == "__main__":
  logging.disable(logging.CRITICAL)
  warnings.simplefilter("ignore")
  _request = json.loads(sys.argv[1])
  cap_memory(_request["memory_bytes"])
  lower_priority()
  sys.exit(serve(_request, sys.stdin.buffer, sys.stdout))
