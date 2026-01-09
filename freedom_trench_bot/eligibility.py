from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class EligibilityState:
    last_eligible: Optional[bool]
    last_alerted_at: Optional[int]
    last_ineligible_at: Optional[int]


@dataclass
class EligibilityDecision:
    eligible: bool
    should_alert: bool
    reason: str
    last_eligible: bool
    last_ineligible_at: Optional[int]


def evaluate_transition(
    now: int,
    eligible: bool,
    state: EligibilityState,
    dedupe_window_sec: int,
    min_ineligible_sec: int,
) -> EligibilityDecision:
    last_eligible = bool(state.last_eligible) if state.last_eligible is not None else False
    last_ineligible_at = state.last_ineligible_at

    if eligible:
        if last_eligible:
            return EligibilityDecision(
                eligible=True,
                should_alert=False,
                reason="still_eligible",
                last_eligible=True,
                last_ineligible_at=last_ineligible_at,
            )
        if (
            state.last_alerted_at is not None
            and state.last_alerted_at > 0
            and now - state.last_alerted_at < dedupe_window_sec
        ):
            return EligibilityDecision(
                eligible=True,
                should_alert=False,
                reason="dedupe_window",
                last_eligible=True,
                last_ineligible_at=last_ineligible_at,
            )
        if (
            last_ineligible_at is not None
            and last_ineligible_at > 0
            and now - last_ineligible_at < min_ineligible_sec
        ):
            return EligibilityDecision(
                eligible=True,
                should_alert=False,
                reason="rearm_wait",
                last_eligible=True,
                last_ineligible_at=last_ineligible_at,
            )
        return EligibilityDecision(
            eligible=True,
            should_alert=True,
            reason="became_eligible",
            last_eligible=True,
            last_ineligible_at=last_ineligible_at,
        )

    if last_eligible or last_ineligible_at is None:
        last_ineligible_at = now
    return EligibilityDecision(
        eligible=False,
        should_alert=False,
        reason="ineligible",
        last_eligible=False,
        last_ineligible_at=last_ineligible_at,
    )
