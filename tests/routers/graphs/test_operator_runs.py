"""GET /v1/graphs/{graph_id}/audit/{threads,runs,runs/{id}} — the record of the
graph's AI operator runs. An admin reads every user's; a member reads their
own; nobody reads another graph's."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from robosystems.models.core import (
  Graph,
  GraphRole,
  GraphUser,
  OperatorRun,
  Org,
  OrgRole,
  OrgType,
  OrgUser,
  User,
)
from robosystems.operations.graph.operator_runs import thread_history

pytestmark = pytest.mark.asyncio


def _graph(session, owner_id: str) -> Graph:
  org = Org.create(
    name=f"Runs Org {uuid4().hex[:6]}", org_type=OrgType.TEAM, session=session
  )
  OrgUser.create(org_id=org.id, user_id=owner_id, role=OrgRole.OWNER, session=session)
  return Graph.create(
    graph_id=f"kg{uuid4().hex[:16]}",
    org_id=org.id,
    graph_name="Recorded Graph",
    graph_type="generic",
    session=session,
  )


def _member(session, graph: Graph, password_hash: str) -> User:
  member = User(
    email=f"member+{uuid4().hex[:8]}@example.com",
    name="Member",
    password_hash=password_hash,
  )
  session.add(member)
  session.commit()
  OrgUser.create(
    org_id=graph.org_id, user_id=member.id, role=OrgRole.MEMBER, session=session
  )
  GraphUser.create(
    user_id=member.id, graph_id=graph.graph_id, role=GraphRole.MEMBER, session=session
  )
  return member


def _run(
  session,
  graph_id: str,
  user_id: str,
  *,
  minutes_ago: int = 0,
  started_at: datetime | None = None,
  thread_id: str | None = "thr_a",
  question: str = "q",
  answer: str | None = "a",
  status: str = "completed",
  operator_type: str = "analyst",
) -> OperatorRun:
  started = started_at or datetime.now(UTC) - timedelta(minutes=minutes_ago)
  row = OperatorRun(
    operation_id=f"op_{uuid4().hex}",
    thread_id=thread_id,
    graph_id=graph_id,
    user_id=user_id,
    operator_type=operator_type,
    mode="standard",
    status=status,
    question=question,
    history=[{"role": "user", "content": "earlier"}],
    transcript=[
      {"call": 1, "input": [{"role": "user", "content": question}], "output": []}
    ],
    answer=answer,
    error=None if status == "completed" else "boom",
    result={
      "credits_consumed": 7.5,
      "call_count": 2,
      "tools_called": ["read-graph-cypher"],
      "writes": [{"operation": "remember", "id": "mem_1"}]
      if operator_type == "author"
      else [],
      "cypher": "MATCH (n) RETURN n",
    },
    started_at=started,
    finished_at=started + timedelta(seconds=4),
    duration_ms=4000.0,
  )
  session.add(row)
  session.commit()
  return row


class TestListRuns:
  async def test_an_admin_reads_newest_first_with_cost_and_writes(
    self, async_client, test_db, test_user
  ):
    graph = _graph(test_db, test_user.id)
    older = _run(test_db, graph.graph_id, test_user.id, minutes_ago=5)
    newer = _run(
      test_db, graph.graph_id, test_user.id, minutes_ago=1, operator_type="author"
    )

    response = await async_client.get(f"/v1/graphs/{graph.graph_id}/audit/runs")

    assert response.status_code == 200
    data = response.json()
    assert data["graph_id"] == graph.graph_id
    assert [r["operation_id"] for r in data["runs"]] == [
      newer.operation_id,
      older.operation_id,
    ]
    first = data["runs"][0]
    assert first["credits_consumed"] == 7.5
    assert first["call_count"] == 2
    assert first["tools_called"] == ["read-graph-cypher"]
    assert first["write_count"] == 1
    assert first["answer"] == "a"
    assert "transcript" not in first
    assert data["next_cursor"] is None

  async def test_pages_and_filters_by_thread(self, async_client, test_db, test_user):
    graph = _graph(test_db, test_user.id)
    ids = [
      _run(test_db, graph.graph_id, test_user.id, minutes_ago=m).operation_id
      for m in (3, 2, 1)
    ]
    _run(test_db, graph.graph_id, test_user.id, minutes_ago=4, thread_id="thr_b")
    url = f"/v1/graphs/{graph.graph_id}/audit/runs"

    first = (
      await async_client.get(url, params={"thread_id": "thr_a", "limit": 2})
    ).json()
    second = (
      await async_client.get(
        url, params={"thread_id": "thr_a", "limit": 2, "cursor": first["next_cursor"]}
      )
    ).json()

    seen = [r["operation_id"] for r in first["runs"] + second["runs"]]
    assert seen == list(reversed(ids))
    assert second["next_cursor"] is None

  async def test_a_member_sees_only_their_own_runs(
    self, async_client, test_db, test_user
  ):
    owner = User(
      email=f"owner+{uuid4().hex[:8]}@example.com",
      name="Owner",
      password_hash=test_user.password_hash,
    )
    test_db.add(owner)
    test_db.commit()
    graph = _graph(test_db, owner.id)
    OrgUser.create(
      org_id=graph.org_id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=test_user.id,
      graph_id=graph.graph_id,
      role=GraphRole.MEMBER,
      session=test_db,
    )
    mine = _run(test_db, graph.graph_id, test_user.id, minutes_ago=1)
    theirs = _run(test_db, graph.graph_id, owner.id, minutes_ago=2, thread_id="thr_o")
    url = f"/v1/graphs/{graph.graph_id}/audit/runs"

    listed = (await async_client.get(url)).json()
    assert [r["operation_id"] for r in listed["runs"]] == [mine.operation_id]

    refused = await async_client.get(url, params={"user_id": owner.id})
    assert refused.status_code == 403

    detail = await async_client.get(f"{url}/{theirs.operation_id}")
    assert detail.status_code == 404

    threads = (
      await async_client.get(f"/v1/graphs/{graph.graph_id}/audit/threads")
    ).json()
    assert [t["thread_id"] for t in threads["threads"]] == ["thr_a"]

  async def test_a_stranger_is_refused(self, async_client, test_db, test_user):
    owner = User(
      email=f"owner+{uuid4().hex[:8]}@example.com",
      name="Owner",
      password_hash=test_user.password_hash,
    )
    test_db.add(owner)
    test_db.commit()
    graph = _graph(test_db, owner.id)

    response = await async_client.get(f"/v1/graphs/{graph.graph_id}/audit/runs")

    assert response.status_code == 403

  async def test_another_graphs_runs_are_not_visible(
    self, async_client, test_db, test_user
  ):
    mine = _graph(test_db, test_user.id)
    theirs = _graph(test_db, test_user.id)
    run = _run(test_db, theirs.graph_id, test_user.id, minutes_ago=1)

    listed = await async_client.get(f"/v1/graphs/{mine.graph_id}/audit/runs")
    detail = await async_client.get(
      f"/v1/graphs/{mine.graph_id}/audit/runs/{run.operation_id}"
    )

    assert listed.json()["runs"] == []
    assert detail.status_code == 404


class TestRunDetail:
  async def test_the_detail_carries_the_paper_trail(
    self, async_client, test_db, test_user
  ):
    graph = _graph(test_db, test_user.id)
    run = _run(
      test_db, graph.graph_id, test_user.id, minutes_ago=1, question="How many?"
    )

    response = await async_client.get(
      f"/v1/graphs/{graph.graph_id}/audit/runs/{run.operation_id}"
    )

    assert response.status_code == 200
    data = response.json()
    assert data["question"] == "How many?"
    assert data["history"] == [{"role": "user", "content": "earlier"}]
    assert data["transcript"][0]["input"][0]["content"] == "How many?"
    assert data["result"]["cypher"] == "MATCH (n) RETURN n"
    assert data["status"] == "completed"


class TestListThreads:
  async def test_threads_are_titled_by_their_first_question_and_ordered_by_activity(
    self, async_client, test_db, test_user
  ):
    graph = _graph(test_db, test_user.id)
    _run(test_db, graph.graph_id, test_user.id, minutes_ago=30, question="Opening A")
    _run(test_db, graph.graph_id, test_user.id, minutes_ago=20, question="Follow-up A")
    _run(
      test_db,
      graph.graph_id,
      test_user.id,
      minutes_ago=10,
      thread_id="thr_b",
      question="Opening B",
      status="failed",
      answer=None,
      operator_type="author",
    )
    # An API call made without a thread is not a thread.
    _run(test_db, graph.graph_id, test_user.id, minutes_ago=1, thread_id=None)

    response = await async_client.get(f"/v1/graphs/{graph.graph_id}/audit/threads")

    assert response.status_code == 200
    threads = response.json()["threads"]
    assert [t["thread_id"] for t in threads] == ["thr_b", "thr_a"]
    b, a = threads
    assert (b["title"], b["run_count"], b["last_status"], b["last_operator_type"]) == (
      "Opening B",
      1,
      "failed",
      "author",
    )
    assert (a["title"], a["run_count"], a["last_status"]) == (
      "Opening A",
      2,
      "completed",
    )
    assert a["first_run_at"] < a["last_run_at"]

  async def test_threads_page_without_skipping(self, async_client, test_db, test_user):
    graph = _graph(test_db, test_user.id)
    for i, m in enumerate((3, 2, 1)):
      _run(test_db, graph.graph_id, test_user.id, minutes_ago=m, thread_id=f"thr_{i}")
    url = f"/v1/graphs/{graph.graph_id}/audit/threads"

    first = (await async_client.get(url, params={"limit": 2})).json()
    second = (
      await async_client.get(url, params={"limit": 2, "cursor": first["next_cursor"]})
    ).json()

    seen = [t["thread_id"] for t in first["threads"] + second["threads"]]
    assert seen == ["thr_2", "thr_1", "thr_0"]
    assert second["next_cursor"] is None


class TestSharedThreadIds:
  """The caller mints the thread id, so two users on one graph can share one.
  Each still holds their own thread: titled, counted and ended by their own
  runs, and a member never sees the other's."""

  async def test_two_users_sharing_an_id_hold_two_threads(
    self, async_client, test_db, test_user
  ):
    graph = _graph(test_db, test_user.id)
    member = _member(test_db, graph, test_user.password_hash)
    _run(
      test_db, graph.graph_id, test_user.id, minutes_ago=30, question="Admin opening"
    )
    _run(
      test_db,
      graph.graph_id,
      member.id,
      minutes_ago=10,
      question="Member opening",
      status="failed",
      answer=None,
    )

    threads = (
      await async_client.get(f"/v1/graphs/{graph.graph_id}/audit/threads")
    ).json()["threads"]

    assert [
      (t["user_id"], t["title"], t["run_count"], t["last_status"]) for t in threads
    ] == [
      (member.id, "Member opening", 1, "failed"),
      (test_user.id, "Admin opening", 1, "completed"),
    ]

  async def test_a_member_sees_only_their_own_side_of_a_shared_id(
    self, async_client, test_db, test_user
  ):
    owner = User(
      email=f"owner+{uuid4().hex[:8]}@example.com",
      name="Owner",
      password_hash=test_user.password_hash,
    )
    test_db.add(owner)
    test_db.commit()
    graph = _graph(test_db, owner.id)
    OrgUser.create(
      org_id=graph.org_id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=test_user.id,
      graph_id=graph.graph_id,
      role=GraphRole.MEMBER,
      session=test_db,
    )
    _run(test_db, graph.graph_id, owner.id, minutes_ago=30, question="Owner opening")
    _run(
      test_db,
      graph.graph_id,
      test_user.id,
      minutes_ago=10,
      question="My opening",
      status="failed",
      answer=None,
    )

    threads = (
      await async_client.get(f"/v1/graphs/{graph.graph_id}/audit/threads")
    ).json()["threads"]

    assert [(t["user_id"], t["title"], t["last_status"]) for t in threads] == [
      (test_user.id, "My opening", "failed")
    ]

  async def test_threads_sharing_an_id_and_a_time_page_without_skipping(
    self, async_client, test_db, test_user
  ):
    graph = _graph(test_db, test_user.id)
    member = _member(test_db, graph, test_user.password_hash)
    at = datetime.now(UTC) - timedelta(minutes=5)
    _run(test_db, graph.graph_id, test_user.id, started_at=at)
    _run(test_db, graph.graph_id, member.id, started_at=at)
    url = f"/v1/graphs/{graph.graph_id}/audit/threads"

    first = (await async_client.get(url, params={"limit": 1})).json()
    second = (
      await async_client.get(url, params={"limit": 1, "cursor": first["next_cursor"]})
    ).json()

    seen = [
      (t["thread_id"], t["user_id"]) for t in first["threads"] + second["threads"]
    ]
    assert sorted(seen) == sorted([("thr_a", test_user.id), ("thr_a", member.id)])
    assert second["next_cursor"] is None


class TestThreadHistory:
  def test_the_last_three_completed_exchanges_oldest_first(self, test_db, test_user):
    graph = _graph(test_db, test_user.id)
    for i, m in enumerate((50, 40, 30, 20, 10)):
      _run(
        test_db,
        graph.graph_id,
        test_user.id,
        minutes_ago=m,
        question=f"q{i}",
        answer=f"a{i}",
      )
    _run(
      test_db,
      graph.graph_id,
      test_user.id,
      minutes_ago=5,
      question="failed one",
      answer=None,
      status="failed",
    )

    history = thread_history(test_db, graph.graph_id, test_user.id, "thr_a")

    assert history == [
      {"role": "user", "content": "q2"},
      {"role": "assistant", "content": "a2"},
      {"role": "user", "content": "q3"},
      {"role": "assistant", "content": "a3"},
      {"role": "user", "content": "q4"},
      {"role": "assistant", "content": "a4"},
    ]

  def test_a_thread_of_another_user_or_graph_seeds_nothing(self, test_db, test_user):
    graph = _graph(test_db, test_user.id)
    _run(test_db, graph.graph_id, test_user.id, minutes_ago=1)

    assert thread_history(test_db, graph.graph_id, "usr_other", "thr_a") == []
    assert thread_history(test_db, "kg_other", test_user.id, "thr_a") == []
    assert thread_history(test_db, graph.graph_id, test_user.id, "thr_missing") == []

  def test_long_answers_are_clipped(self, test_db, test_user):
    graph = _graph(test_db, test_user.id)
    _run(test_db, graph.graph_id, test_user.id, minutes_ago=1, answer="x" * 5000)

    history = thread_history(test_db, graph.graph_id, test_user.id, "thr_a")

    assert len(history[1]["content"]) == 2000

  def test_long_questions_are_clipped_like_answers(self, test_db, test_user):
    graph = _graph(test_db, test_user.id)
    _run(test_db, graph.graph_id, test_user.id, minutes_ago=1, question="q" * 5000)

    history = thread_history(test_db, graph.graph_id, test_user.id, "thr_a")

    assert len(history[0]["content"]) == 2000
