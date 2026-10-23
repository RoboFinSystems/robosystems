"""The history an operator is seeded with is a transcript Converse accepts."""

from __future__ import annotations

from robosystems.operations.operators.base import OperatorMode
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.tool_loop import _seed_history


def _ctx(history: list[dict]) -> OperatorContext:
  return OperatorContext(
    graph_id="kg_test",
    user_id="usr_test",
    query="next question",
    mode=OperatorMode.STANDARD,
    history=history,
  )


def test_three_whole_exchanges_survive():
  history = []
  for i in range(3):
    history += [
      {"role": "user", "content": f"q{i}"},
      {"role": "assistant", "content": f"a{i}"},
    ]
  turns = _seed_history(_ctx(history))
  assert [(t.role, t.content) for t in turns] == [
    ("user", "q0"),
    ("assistant", "a0"),
    ("user", "q1"),
    ("assistant", "a1"),
    ("user", "q2"),
    ("assistant", "a2"),
  ]


def test_a_window_that_opens_on_an_answer_is_trimmed_to_a_question():
  # Three exchanges and an unanswered question: the six-turn window opens on
  # an answer and closes on a question, neither of which the model accepts.
  history = []
  for i in range(3):
    history += [
      {"role": "user", "content": f"q{i}"},
      {"role": "assistant", "content": f"a{i}"},
    ]
  history.append({"role": "user", "content": "q3"})
  turns = _seed_history(_ctx(history))
  assert [(t.role, t.content) for t in turns] == [
    ("user", "q1"),
    ("assistant", "a1"),
    ("user", "q2"),
    ("assistant", "a2"),
  ]


def test_consecutive_same_role_turns_merge_and_a_trailing_question_drops():
  history = [
    {"role": "user", "content": "first"},
    {"role": "user", "content": "second"},
    {"role": "assistant", "content": "answer"},
    {"role": "user", "content": "unanswered"},
  ]
  turns = _seed_history(_ctx(history))
  assert [(t.role, t.content) for t in turns] == [
    ("user", "first\n\nsecond"),
    ("assistant", "answer"),
  ]


def test_unknown_roles_and_empty_turns_are_coerced_or_dropped():
  history = [
    {"role": "system", "content": "a system turn"},
    {"role": "assistant", "content": "   "},
    {"role": "assistant", "content": "reply"},
  ]
  turns = _seed_history(_ctx(history))
  assert [(t.role, t.content) for t in turns] == [
    ("user", "a system turn"),
    ("assistant", "reply"),
  ]


def test_no_history_seeds_nothing():
  assert _seed_history(_ctx([])) == []
