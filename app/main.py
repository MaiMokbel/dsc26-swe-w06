"""The HTTP layer around explain_stream.

Keep `StreamSettings` and `create_app` exactly as named; the public and hidden tests import
them."""
from collections.abc import AsyncIterator, Sequence
from contextlib import aclosing
from dataclasses import dataclass
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, Request
from fastapi.responses import JSONResponse
from fastapi.sse import EventSourceResponse, ServerSentEvent
from pydantic import BaseModel, Field

from app.budget import DailyBudget, estimate_cost_usd
from app.providers import LLMProvider, build_prompt
from app.stream import explain_stream


@dataclass
class StreamSettings:
    ttft_timeout_s: float = 2.0
    total_timeout_s: float = 20.0
    max_output_tokens: int = 400


class ExplainRequest(BaseModel):
    clause_text: str = Field(min_length=1, max_length=8000)
    language: Literal["ar", "en"]


class BudgetExhausted(Exception):
    pass


def create_app(providers: Sequence[LLMProvider], budget: DailyBudget,
               settings: StreamSettings | None = None) -> FastAPI:
    """Return a NEW FastAPI app exposing POST /v1/clauses/explain.

    - Header X-Tenant-ID (required, 1-64 chars). Body {"clause_text": 1-8000 chars,
      "language": "ar" | "en"}. Build the prompt with app.providers.build_prompt.
    - Before streaming: invalid input is 422; if the tenant cannot afford even the
      cheapest provider's worst-case estimate, 429 application/problem+json with type
      /problems/budget-exhausted. Providers are not called in either case.
    - Otherwise, a text/event-stream response (use fastapi.sse.EventSourceResponse and
      ServerSentEvent) whose events are explain_stream's events, named meta, token, done
      and error, each event's data as JSON.
    """
    settings = settings or StreamSettings()
    providers = list(providers)
    app = FastAPI(title="Bayyina clause explainer")

    @app.exception_handler(BudgetExhausted)
    async def _budget_exhausted(_: Request, __: BudgetExhausted):
        return JSONResponse({"type": "/problems/budget-exhausted",
                             "title": "Daily AI budget for this client matter is used up",
                             "status": 429},
                            status_code=429, media_type="application/problem+json")

    def checked_prompt(body: ExplainRequest,
                       x_tenant_id: Annotated[str, Header(min_length=1, max_length=64)]) -> str:
        """Runs after FastAPI has validated the body and header (422 otherwise) and before
        the stream starts, so it can still answer with a real 429."""
        prompt = build_prompt(body.clause_text, body.language)
        cheapest = min((estimate_cost_usd(p, prompt, settings.max_output_tokens) for p in providers),
                       default=None)
        if cheapest is None or cheapest > budget.remaining(x_tenant_id):
            raise BudgetExhausted()
        return prompt

    @app.post("/v1/clauses/explain", response_class=EventSourceResponse)
    async def explain(prompt: Annotated[str, Depends(checked_prompt)],
                      x_tenant_id: Annotated[str, Header(min_length=1, max_length=64)]
                      ) -> AsyncIterator[ServerSentEvent]:
        events = explain_stream(prompt, providers, tenant=x_tenant_id, budget=budget,
                                ttft_timeout_s=settings.ttft_timeout_s,
                                total_timeout_s=settings.total_timeout_s,
                                max_output_tokens=settings.max_output_tokens)
        async with aclosing(events):   # client gone -> the provider stream is closed too
            async for ev in events:
                yield ServerSentEvent(event=ev.kind, data=ev.data)

    return app
