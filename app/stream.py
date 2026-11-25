"""The streaming core: provider fallback, two deadlines, budget reserve/settle.

Keep `Event` and `explain_stream` exactly as named; the public and hidden tests import them."""
import asyncio
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field

from app.budget import DailyBudget, actual_cost_usd, estimate_cost_usd
from app.providers import LLMProvider, ProviderError


@dataclass(frozen=True)
class Event:
    kind: str                       # "meta" | "token" | "done" | "error"
    data: dict = field(default_factory=dict)


async def _close(stream) -> None:
    """Close the provider's stream so it stops producing (and billing) tokens."""
    aclose = getattr(stream, "aclose", None)
    if aclose is not None:
        try:
            await aclose()
        except Exception:  # a provider failing while closing must not hide our own result
            pass


async def _next_token(stream, timeout: float):
    """Wait for the next token for at most `timeout` seconds.

    The timeout only wraps the await on the provider, never a `yield` of ours, so it can
    not fire while the consumer is running."""
    if timeout <= 0:
        raise TimeoutError
    return await asyncio.wait_for(anext(stream), timeout)


async def explain_stream(prompt: str, providers: Sequence[LLMProvider], *, tenant: str,
                         budget: DailyBudget, ttft_timeout_s: float, total_timeout_s: float,
                         max_output_tokens: int) -> AsyncIterator[Event]:
    """Yield Event("meta", ...), Event("token", ...)* and a final Event("done" | "error", ...).

    Rules, in order (see README.md for the full statement):
    - Try providers in the given order. Before each, reserve its estimate; skip an
      unaffordable provider without calling it.
    - Wait at most ttft_timeout_s (never past the overall deadline) for the first token.
      On timeout, ProviderError, or an empty stream before the first token: settle the
      reservation at 0 and try the next provider.
    - On the first token, emit "meta" {"provider": name}, then "token" {"text": ...} per token.
    - After the first token, never switch provider: a ProviderError now is a fatal
      "error" {"code": "upstream_interrupted", "partial": True}.
    - The whole call never runs past total_timeout_s from the start; past the deadline, emit
      "error" {"code": "deadline_exceeded", "partial": <any token sent yet>}.
    - On success, settle at the actual cost and emit
      "done" {"provider", "output_tokens", "cost_usd"}.
    - Every affordable provider failed before its first token: "error" {"code": "all_providers_failed"}.
      No provider was affordable: "error" {"code": "budget_exhausted"}.
    - Whatever happens, including the caller closing this generator early, close the
      provider's stream (aclose()) and settle its reservation at the actual cost of the
      tokens it produced.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + total_timeout_s
    attempted = False

    for provider in providers:
        left = deadline - loop.time()
        if left <= 0:
            yield Event("error", {"code": "deadline_exceeded", "partial": False})
            return

        reserved = estimate_cost_usd(provider, prompt, max_output_tokens)
        if not budget.try_reserve(tenant, reserved):
            continue                    # cannot afford it: skip without calling it
        attempted = True

        stream = provider.stream(prompt, max_output_tokens)
        produced = 0
        finished = False                # closed and settled already?

        async def finish(cost: float) -> None:
            nonlocal finished
            if not finished:
                finished = True
                await _close(stream)
                budget.settle(tenant, reserved, cost)

        try:
            # 1. the first token: failures here are safe to retry on the next provider
            first_wait = min(ttft_timeout_s, left)
            try:
                first = await _next_token(stream, first_wait)
            except (TimeoutError, ProviderError, StopAsyncIteration) as exc:
                await finish(0.0)
                if isinstance(exc, TimeoutError) and first_wait >= left:
                    yield Event("error", {"code": "deadline_exceeded", "partial": False})
                    return
                continue

            produced = 1
            yield Event("meta", {"provider": provider.name})
            yield Event("token", {"text": first})

            # 2. the rest: from here on we never switch provider
            while True:
                try:
                    tok = await _next_token(stream, deadline - loop.time())
                except StopAsyncIteration:
                    break
                except TimeoutError:
                    await finish(actual_cost_usd(provider, prompt, produced))
                    yield Event("error", {"code": "deadline_exceeded", "partial": True})
                    return
                except ProviderError:
                    await finish(actual_cost_usd(provider, prompt, produced))
                    yield Event("error", {"code": "upstream_interrupted", "partial": True})
                    return
                produced += 1
                yield Event("token", {"text": tok})

            cost = actual_cost_usd(provider, prompt, produced)
            await finish(cost)
            yield Event("done", {"provider": provider.name, "output_tokens": produced,
                                 "cost_usd": cost})
            return
        finally:
            # also runs when the caller aclose()s us or abandons us mid-stream
            if not finished:
                await finish(actual_cost_usd(provider, prompt, produced) if produced else 0.0)

    if attempted:
        yield Event("error", {"code": "all_providers_failed"})
    else:
        yield Event("error", {"code": "budget_exhausted"})
