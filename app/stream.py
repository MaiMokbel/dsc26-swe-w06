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
    ### lazem n2fel el stream bta3 el provider, 3shan maykamelsh y3ml tokens w y7aseb 3aleha
    aclose = getattr(stream, "aclose", None)
    if aclose is not None:
        try:
            await aclose()
        except Exception:
            ### law el provider 3ml error w e7na bn2felo, msh hn3ml 7aga, el mohem el result bta3na
            pass


async def _next_token(stream, timeout: float):
    """Wait for the next token for at most `timeout` seconds.

    The timeout only wraps the await on the provider, never a `yield` of ours, so it can
    not fire while the consumer is running."""
    ### law el wa2t 5eles asln, timeout 3la tool
    if timeout <= 0:
        raise TimeoutError
    ### el timeout 3la el await bta3 el provider bas, msh 3la el yield bta3na
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
    ### el deadline el kebir: el call kolaha lazem t5ales ablo
    deadline = loop.time() + total_timeout_s
    ### hal garabna ay provider wala la? (3shan nfara2 ben budget_exhausted w all_providers_failed)
    attempted = False

    for provider in providers:
        ### fadel ad eh mn el wa2t?
        left = deadline - loop.time()
        if left <= 0:
            yield Event("error", {"code": "deadline_exceeded", "partial": False})
            return

        reserved = estimate_cost_usd(provider, prompt, max_output_tokens)
        ### n7goz el worst case cost el awel, law el budget msh kfaya n3ady 3al provider elly ba3do
        if not budget.try_reserve(tenant, reserved):
            continue
        attempted = True

        stream = provider.stream(prompt, max_output_tokens)
        ### kam token el provider tala3
        produced = 0
        ### 2afalna el stream w 3amalna settle? (3shan n3mlha mara wa7da bas)
        finished = False

        ### n2fel el stream w n7aseb el tamn el 7a2i2i, mara wa7da bas
        async def finish(cost: float) -> None:
            nonlocal finished
            if not finished:
                finished = True
                await _close(stream)
                budget.settle(tenant, reserved, cost)

        try:
            ### 1. awel token: law 7asal moshkla hena, 3ady negarab el provider elly ba3do
            ### 3shan lesa ma ba3atnash ay 7aga lel user
            first_wait = min(ttft_timeout_s, left)
            try:
                first = await _next_token(stream, first_wait)
            except (TimeoutError, ProviderError, StopAsyncIteration) as exc:
                ### mafeesh tokens tel3et, yb2a el tamn 0
                await finish(0.0)
                ### law el timeout da kan el deadline el kebir, n2af 5ales msh negarab tany
                if isinstance(exc, TimeoutError) and first_wait >= left:
                    yield Event("error", {"code": "deadline_exceeded", "partial": False})
                    return
                continue

            produced = 1
            ### awel token wesel: neb3at meta (mein el provider) w ba3daha el token
            yield Event("meta", {"provider": provider.name})
            yield Event("token", {"text": first})

            ### 2. ba2y el tokens: mn hena msh hn8ayar el provider abadan
            ### 3shan el user 5alas shaf goz2 mn el egaba
            while True:
                try:
                    tok = await _next_token(stream, deadline - loop.time())
                except StopAsyncIteration:
                    ### el provider 5alas, kolo tamam
                    break
                except TimeoutError:
                    ### el deadline el kebir 3ada w e7na fe nos el egaba
                    await finish(actual_cost_usd(provider, prompt, produced))
                    yield Event("error", {"code": "deadline_exceeded", "partial": True})
                    return
                except ProviderError:
                    ### el provider wa2a3 ba3d ma ba3at tokens, msh hn8ayar, error w ne2af
                    await finish(actual_cost_usd(provider, prompt, produced))
                    yield Event("error", {"code": "upstream_interrupted", "partial": True})
                    return
                produced += 1
                yield Event("token", {"text": tok})

            ### success: n7aseb 3la el tokens elly tel3et bas, msh el worst case
            cost = actual_cost_usd(provider, prompt, produced)
            await finish(cost)
            yield Event("done", {"provider": provider.name, "output_tokens": produced,
                                 "cost_usd": cost})
            return
        finally:
            ### de btesht8al kaman law el user 2afal el stream badry aw sabo
            ### fa lazem n2fel el provider w n3ml settle hena
            if not finished:
                await finish(actual_cost_usd(provider, prompt, produced) if produced else 0.0)

    ### wesselna hena yb2a mafeesh provider nefe3
    if attempted:
        ### garabna w kolohom wo2o3o
        yield Event("error", {"code": "all_providers_failed"})
    else:
        ### mafeesh wala provider el budget yekfeeh
        yield Event("error", {"code": "budget_exhausted"})
