from freedom_trench_bot.eligibility import EligibilityState, evaluate_transition


def test_transition_to_eligible_alerts():
    state = EligibilityState(last_eligible=False, last_alerted_at=None, last_ineligible_at=0)
    decision = evaluate_transition(
        now=1000,
        eligible=True,
        state=state,
        dedupe_window_sec=3600,
        min_ineligible_sec=1800,
    )
    assert decision.should_alert is True
    assert decision.last_eligible is True


def test_dedupe_window_blocks_alert():
    state = EligibilityState(last_eligible=False, last_alerted_at=950, last_ineligible_at=900)
    decision = evaluate_transition(
        now=1000,
        eligible=True,
        state=state,
        dedupe_window_sec=3600,
        min_ineligible_sec=1800,
    )
    assert decision.should_alert is False
    assert decision.reason == "dedupe_window"


def test_rearm_wait_blocks_alert():
    state = EligibilityState(last_eligible=False, last_alerted_at=None, last_ineligible_at=900)
    decision = evaluate_transition(
        now=1000,
        eligible=True,
        state=state,
        dedupe_window_sec=3600,
        min_ineligible_sec=200,
    )
    assert decision.should_alert is False
    assert decision.reason == "rearm_wait"


def test_ineligible_sets_timestamp():
    state = EligibilityState(last_eligible=True, last_alerted_at=None, last_ineligible_at=None)
    decision = evaluate_transition(
        now=1000,
        eligible=False,
        state=state,
        dedupe_window_sec=3600,
        min_ineligible_sec=1800,
    )
    assert decision.should_alert is False
    assert decision.last_ineligible_at == 1000
