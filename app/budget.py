"""Per-request cost estimate and a per-tenant daily spend cap for AI calls.

Keep these names exactly (the public and hidden tests import them)."""
from collections.abc import Callable
from datetime import UTC, date, datetime

from app.providers import LLMProvider


def estimate_cost_usd(provider: LLMProvider, prompt: str, max_output_tokens: int) -> float:
    """Worst case: every allowed output token is produced.

    (count_tokens(prompt) * price_in_per_mtok + max_output_tokens * price_out_per_mtok) / 1e6
    """
    ### el worst case: n3tebar en el model hayektb kol el max_output_tokens
    return (provider.count_tokens(prompt) * provider.price_in_per_mtok
            + max_output_tokens * provider.price_out_per_mtok) / 1_000_000


def actual_cost_usd(provider: LLMProvider, prompt: str, output_tokens: int) -> float:
    """The same formula with the tokens actually produced."""
    ### nafs el formula bas b3adad el tokens elly tel3et fe3lan
    return estimate_cost_usd(provider, prompt, output_tokens)


class DailyBudget:
    """Per-tenant spend limit in USD. Budgets are per (tenant, today()) date; a new date
    starts from zero."""

    def __init__(self, limit_usd: float, today: Callable[[], date] = lambda: datetime.now(UTC).date()):
        self.limit_usd = limit_usd
        self.today = today
        ### kol (tenant, date) leh el folos elly etsarafet fe3lan, w el 7agz elly lesa maftou7
        self._spent: dict[tuple[str, date], float] = {}
        self._open: dict[tuple[str, date], list[float]] = {}

    def _key(self, tenant: str) -> tuple[str, date]:
        ### el date gowa el key, fa youm gdeed = budget gdeed mn el awel
        return (tenant, self.today())

    def remaining(self, tenant: str) -> float:
        """How much `tenant` can still spend today."""
        key = self._key(tenant)
        ### el fadel = el limit - elly etsaraf - elly ma7gooz
        return self.limit_usd - self._spent.get(key, 0.0) - sum(self._open.get(key, []))

    def try_reserve(self, tenant: str, amount: float) -> bool:
        """Reserve `amount` for `tenant` and return True, only if amount <= remaining(tenant)."""
        ### law msh kfaya, mansh7gezsh 7aga 5ales
        if amount > self.remaining(tenant):
            return False
        self._open.setdefault(self._key(tenant), []).append(amount)
        return True

    def settle(self, tenant: str, reserved: float, actual: float) -> None:
        """Replace a reservation with the actual cost (actual may be 0, or more or less than
        reserved)."""
        key = self._key(tenant)
        ### nesheel el 7agz (momken yekon et7agaz embare7 abl nos el leil)
        for k in [key] + [k for k in self._open if k[0] == tenant and k != key]:
            if reserved in self._open.get(k, []):
                self._open[k].remove(reserved)
                break
        ### w n7ot makano el tamn el 7a2i2i
        self._spent[key] = self._spent.get(key, 0.0) + actual
