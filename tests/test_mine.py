"""My own tests, one or more per hidden check in the README (H01-H14)."""
import json
import time
from datetime import date

import pytest
from fastapi.testclient import TestClient

from app.budget import DailyBudget, actual_cost_usd, estimate_cost_usd
from app.fakes import FakeProvider
from app.main import StreamSettings, create_app
from app.stream import explain_stream

pytestmark = pytest.mark.anyio

FAST = dict(ttft_timeout_s=0.2, total_timeout_s=2.0, max_output_tokens=50)
DAY = date(2026, 11, 16)


async def collect(gen):
    return [ev async for ev in gen]


def budget(limit=1.0, today=lambda: DAY):
    return DailyBudget(limit_usd=limit, today=today)


def sse_events(response):
    events, name, data = [], None, []
    for line in response.iter_lines():
        if line.startswith("event:"):
            name = line[6:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].strip())
        elif line == "" and data:
            events.append((name, json.loads("\n".join(data))))
            name, data = None, []
    return events


def post(client, tenant="bayyina", text="The Supplier shall indemnify the Buyer.", lang="en"):
    headers = {} if tenant is None else {"X-Tenant-ID": tenant}
    return client.post("/v1/clauses/explain", headers=headers,
                       json={"clause_text": text, "language": lang})


# H01
async def test_meta_tokens_done_in_order_with_cost():
    p = FakeProvider("primary", ["A ", "B ", "C"])
    b = budget()
    ev = await collect(explain_stream("prompt", [p], tenant="t", budget=b, **FAST))
    assert [e.kind for e in ev] == ["meta", "token", "token", "token", "done"]
    assert ev[0].data == {"provider": "primary"}
    assert "".join(e.data["text"] for e in ev if e.kind == "token") == "A B C"
    done = ev[-1].data
    assert done["provider"] == "primary" and done["output_tokens"] == 3
    assert done["cost_usd"] == pytest.approx(actual_cost_usd(p, "prompt", 3))
    assert p.closed == 1


# H02
def test_http_sse_named_json_events_and_arabic():
    text = ["يلتزم ", "المورد ", "بالتعويض"]
    app = create_app([FakeProvider("primary", text)], budget(), StreamSettings(**FAST))
    with TestClient(app).stream("POST", "/v1/clauses/explain", headers={"X-Tenant-ID": "bayyina"},
                                json={"clause_text": "يلتزم المورد بتعويض المشتري", "language": "ar"}) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        events = sse_events(r)
    assert [n for n, _ in events] == ["meta", "token", "token", "token", "done"]
    assert "".join(d["text"] for n, d in events if n == "token") == "".join(text)


# H03
async def test_slow_first_token_falls_back_directly():
    slow = FakeProvider("primary", ["SLOW"], first_token_delay=1.0)
    backup = FakeProvider("backup", ["ok"])
    ev = await collect(explain_stream("prompt", [slow, backup], tenant="t", budget=budget(), **FAST))
    assert ev[0].data["provider"] == "backup"
    assert all(e.data.get("text") != "SLOW" for e in ev)
    assert ev[-1].kind == "done" and slow.closed == 1


def test_slow_first_token_falls_back_over_http():
    slow = FakeProvider("primary", ["SLOW"], hang=True)
    backup = FakeProvider("backup", ["ok"])
    app = create_app([slow, backup], budget(), StreamSettings(**FAST))
    with TestClient(app).stream("POST", "/v1/clauses/explain", headers={"X-Tenant-ID": "b"},
                                json={"clause_text": "x", "language": "en"}) as r:
        events = sse_events(r)
    assert events[0] == ("meta", {"provider": "backup"})
    assert ("token", {"text": "SLOW"}) not in events


# H04
@pytest.mark.parametrize("bad", [dict(fail_before_first=True), dict(tokens=[])])
async def test_failure_before_first_token_moves_on(bad):
    first = FakeProvider("primary", **bad) if "tokens" not in bad else FakeProvider("primary", [])
    backup = FakeProvider("backup", ["ok"])
    ev = await collect(explain_stream("prompt", [first, backup], tenant="t", budget=budget(), **FAST))
    assert [e.kind for e in ev] == ["meta", "token", "done"]
    assert ev[0].data["provider"] == "backup"


# H05
async def test_no_switching_after_first_token():
    p = FakeProvider("primary", ["a", "b", "c", "d"], fail_after_tokens=2)
    backup = FakeProvider("backup", ["x"])
    ev = await collect(explain_stream("prompt", [p, backup], tenant="t", budget=budget(), **FAST))
    assert [e.kind for e in ev] == ["meta", "token", "token", "error"]
    assert ev[-1].data == {"code": "upstream_interrupted", "partial": True}
    assert backup.calls == 0


# H06
async def test_deadline_stops_a_slow_stream_on_time():
    p = FakeProvider("primary", ["a"] * 50, token_delay=0.1)
    start = time.monotonic()
    ev = await collect(explain_stream("prompt", [p], tenant="t", budget=budget(),
                                      ttft_timeout_s=0.2, total_timeout_s=0.35, max_output_tokens=50))
    assert time.monotonic() - start < 0.5
    assert ev[-1].data == {"code": "deadline_exceeded", "partial": True}
    assert p.closed == 1


async def test_deadline_passes_during_fallback():
    providers = [FakeProvider(f"p{i}", hang=True) for i in range(4)]
    start = time.monotonic()
    ev = await collect(explain_stream("prompt", providers, tenant="t", budget=budget(),
                                      ttft_timeout_s=0.2, total_timeout_s=0.3, max_output_tokens=50))
    assert time.monotonic() - start < 0.45
    assert [e.kind for e in ev] == ["error"]
    assert ev[0].data == {"code": "deadline_exceeded", "partial": False}
    assert providers[2].calls == 0


# H07
async def test_all_providers_failing_is_one_error_event():
    ps = [FakeProvider("a", fail_before_first=True), FakeProvider("b", [])]
    ev = await collect(explain_stream("prompt", ps, tenant="t", budget=budget(), **FAST))
    assert ev == [type(ev[0])("error", {"code": "all_providers_failed"})]


# H08
@pytest.mark.parametrize("pin,pout,chars,cap", [(2.0, 10.0, 400, 300), (0.5, 1.5, 4001, 1), (3.0, 15.0, 3, 400)])
def test_estimate_is_exact_worst_case(pin, pout, chars, cap):
    p = FakeProvider("p", price_in_per_mtok=pin, price_out_per_mtok=pout)
    prompt = "x" * chars
    assert estimate_cost_usd(p, prompt, cap) == pytest.approx(
        (p.count_tokens(prompt) * pin + cap * pout) / 1_000_000)


# H09
async def test_unaffordable_provider_is_skipped_not_called():
    expensive = FakeProvider("primary", price_in_per_mtok=1000.0, price_out_per_mtok=5000.0)
    cheap = FakeProvider("backup", ["ok"])
    ev = await collect(explain_stream("prompt", [expensive, cheap], tenant="t", budget=budget(0.01), **FAST))
    assert ev[0].data["provider"] == "backup" and expensive.calls == 0


async def test_nothing_affordable_is_budget_exhausted():
    p = FakeProvider("p", price_out_per_mtok=1e9)
    ev = await collect(explain_stream("prompt", [p], tenant="t", budget=budget(0.01), **FAST))
    assert [e.data for e in ev] == [{"code": "budget_exhausted"}] and p.calls == 0


# H10
async def test_budget_settled_at_actual_cost():
    b = budget(1.0)
    failing = FakeProvider("primary", fail_before_first=True)
    ok = FakeProvider("backup", ["a", "b"])
    ev = await collect(explain_stream("prompt", [failing, ok], tenant="t", budget=b, **FAST))
    cost = actual_cost_usd(ok, "prompt", 2)
    assert ev[-1].data["cost_usd"] == pytest.approx(cost)
    assert b.remaining("t") == pytest.approx(1.0 - cost)


# H11
def test_budget_exhausted_is_429_before_streaming():
    p = FakeProvider("primary")
    b = budget(0.01)
    assert b.try_reserve("bayyina", 0.01)
    r = post(TestClient(create_app([p], b, StreamSettings(**FAST))))
    assert r.status_code == 429
    assert r.headers["content-type"].startswith("application/problem+json")
    assert r.json()["type"] == "/problems/budget-exhausted" and r.json()["status"] == 429
    assert p.calls == 0
    r2 = post(TestClient(create_app([p], b, StreamSettings(**FAST))), tenant="other")
    assert r2.status_code == 200 and p.calls == 1


# H12
def test_budget_resets_on_a_new_day():
    day = [DAY]
    b = budget(1.0, today=lambda: day[0])
    assert b.try_reserve("t", 0.9)
    b.settle("t", 0.9, 0.9)
    assert b.remaining("t") == pytest.approx(0.1)
    day[0] = date(2026, 11, 17)
    assert b.remaining("t") == pytest.approx(1.0)


def test_reserve_refuses_more_than_remaining():
    b = budget(1.0)
    assert not b.try_reserve("t", 1.5) and b.remaining("t") == 1.0
    assert b.try_reserve("t", 1.0) and b.remaining("t") == 0.0


# H13
@pytest.mark.parametrize("kw", [dict(text=""), dict(text="x" * 8001), dict(lang="fr"),
                                dict(tenant=None), dict(tenant="t" * 65)])
def test_validation_before_any_provider_call(kw):
    p = FakeProvider("primary")
    r = post(TestClient(create_app([p], budget(), StreamSettings(**FAST))), **kw)
    assert r.status_code == 422 and p.calls == 0


# H14
async def test_closing_early_closes_the_provider_and_settles():
    p = FakeProvider("primary", ["a", "b", "c", "d"])
    b = budget(1.0)
    gen = explain_stream("prompt", [p], tenant="t", budget=b, **FAST)
    assert (await anext(gen)).kind == "meta"
    assert (await anext(gen)).kind == "token"
    await gen.aclose()
    assert p.closed == 1
    assert b.remaining("t") == pytest.approx(1.0 - actual_cost_usd(p, "prompt", p.emitted))
