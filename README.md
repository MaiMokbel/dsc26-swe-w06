# dsc26-swe-w06 · Bayyina clause explainer with a budget


## Set up

```bash

git init -b main
uv venv --python 3.12
source .venv/bin/activate          ### Windows: .venv\Scripts\activate
uv pip install -r requirements.txt  
pytest -q                           
```

Run the API by hand with `uvicorn` once `create_app` works, for example from a short script
that builds `create_app([FakeProvider("primary")], DailyBudget(1.0))`, and watch tokens arrive
with `curl -N -X POST localhost:8000/v1/clauses/explain -H 'X-Tenant-ID: bayyina' -H 'Content-Type: application/json' -d '{"clause_text":"The Supplier shall indemnify the Buyer.","language":"en"}'`.

## Files

```
app/__init__.py       
app/providers.py      
app/fakes.py          
                      
app/budget.py        
app/stream.py        
app/main.py          
tests/test_public.py  
AI_USAGE.md           
requirements.txt      
```



### 1. `app/budget.py`

- `estimate_cost_usd(provider, prompt, max_output_tokens) -> float`: the worst case —
  `(count_tokens(prompt) * price_in_per_mtok + max_output_tokens * price_out_per_mtok) / 1,000,000`.
- `actual_cost_usd(provider, prompt, output_tokens) -> float`: the same formula with the
  tokens actually produced.
- `class DailyBudget(limit_usd: float, today: Callable[[], date])` with:
  - `remaining(tenant) -> float`
  - `try_reserve(tenant, amount) -> bool`: reserves the amount and returns `True` only if
    `amount <= remaining(tenant)`; returns `False` and reserves nothing otherwise.
  - `settle(tenant, reserved, actual) -> None`: replaces a reservation with the actual cost.
  - Budgets are per tenant **per `today()` date**; a new date starts from zero (so changing
    what `today()` returns resets every tenant's spend).

### 2. `app/stream.py`

`Event` (already defined for you: a frozen dataclass with `kind: str` and `data: dict`) and

```python
async def explain_stream(prompt: str, providers: Sequence[LLMProvider], *, tenant: str,
                         budget: DailyBudget, ttft_timeout_s: float, total_timeout_s: float,
                         max_output_tokens: int) -> AsyncIterator[Event]: ...
```


### 3. `app/main.py`

`StreamSettings` is given (`ttft_timeout_s=2.0`, `total_timeout_s=20.0`,
`max_output_tokens=400`). You write:

```python
def create_app(providers: Sequence[LLMProvider], budget: DailyBudget,
               settings: StreamSettings | None = None) -> FastAPI: ...
```

- `POST /v1/clauses/explain`. Header `X-Tenant-ID` (required, 1–64 chars). Body
  `{"clause_text": <1–8000 chars>, "language": "ar" | "en"}`. Build the prompt with
  `app.providers.build_prompt(clause_text, language)`.
- **Before streaming starts:** invalid input (missing/empty/too-long `clause_text`, an
  unknown `language`, a missing or too-long `X-Tenant-ID`) is a normal `422`. If the tenant
  cannot afford even the cheapest provider's worst-case estimate, a normal `429` with
  `application/problem+json` body `{"type": "/problems/budget-exhausted", "title": ..., "status": 429}`.
  **No provider is called** in either case.
- Otherwise, return a `text/event-stream` response (`fastapi.sse.EventSourceResponse`,
  yielding `fastapi.sse.ServerSentEvent(event=ev.kind, data=ev.data)` for each `Event` from
  `explain_stream`) with events named `meta`, `token`, `done` and `error`, each event's `data`
  serialised as JSON. **Once this response starts, the status code is 200 and cannot
  change** — anything that goes wrong after that point is one of `explain_stream`'s `error`
  events, never a different HTTP status.
- `create_app` returns a **new** app each time (no shared state between two apps built from
  the same `providers`/`budget` objects is fine — the budget and provider objects you were
  given are intentionally shared on purpose, so a per-tenant budget persists across calls).

