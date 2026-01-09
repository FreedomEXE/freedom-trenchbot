from freedom_trench_bot.config import FilterConfig
from freedom_trench_bot.filters import evaluate_pair


def test_filters_pass():
    filters = FilterConfig(
        max_market_cap=100000,
        min_change_24h=1,
        min_change_6h=1,
        min_change_1h=1,
        min_volume_1h=10000,
        require_profile=False,
    )
    pair = {
        "marketCap": 90000,
        "priceChange": {"h1": 1.2, "h6": 1.5, "h24": 2.0},
        "volume": {"h1": 12000},
    }
    result = evaluate_pair(pair, filters, use_fdv_proxy=True)
    assert result.passed is True


def test_filters_fdv_proxy_enabled():
    filters = FilterConfig(
        max_market_cap=100000,
        min_change_24h=1,
        min_change_6h=1,
        min_change_1h=1,
        min_volume_1h=10000,
        require_profile=False,
    )
    pair = {
        "fdv": 90000,
        "priceChange": {"h1": 1.2, "h6": 1.5, "h24": 2.0},
        "volume": {"h1": 12000},
    }
    result = evaluate_pair(pair, filters, use_fdv_proxy=True)
    assert result.metrics.market_cap_label == "FDV (proxy)"
    assert result.passed is True


def test_filters_fdv_proxy_disabled():
    filters = FilterConfig(
        max_market_cap=100000,
        min_change_24h=1,
        min_change_6h=1,
        min_change_1h=1,
        min_volume_1h=10000,
        require_profile=False,
    )
    pair = {
        "fdv": 90000,
        "priceChange": {"h1": 1.2, "h6": 1.5},
        "volume": {"h1": 12000},
    }
    result = evaluate_pair(pair, filters, use_fdv_proxy=False)
    assert result.passed is False
    assert "fdv proxy disabled" in result.reasons


def test_missing_change_24h_fails():
    filters = FilterConfig(
        max_market_cap=100000,
        min_change_24h=1,
        min_change_6h=1,
        min_change_1h=1,
        min_volume_1h=10000,
        require_profile=False,
    )
    pair = {
        "marketCap": 90000,
        "priceChange": {"h1": 1.2, "h6": 1.5},
        "volume": {"h1": 12000},
    }
    result = evaluate_pair(pair, filters, use_fdv_proxy=True)
    assert result.passed is False


def test_missing_change_6h_fails():
    filters = FilterConfig(
        max_market_cap=100000,
        min_change_24h=1,
        min_change_6h=1,
        min_change_1h=1,
        min_volume_1h=10000,
        require_profile=False,
    )
    pair = {
        "marketCap": 90000,
        "priceChange": {"h1": 1.2, "h24": 2.0},
        "volume": {"h1": 12000},
    }
    result = evaluate_pair(pair, filters, use_fdv_proxy=True)
    assert result.passed is False


def test_missing_change_1h_fails():
    filters = FilterConfig(
        max_market_cap=100000,
        min_change_24h=1,
        min_change_6h=1,
        min_change_1h=1,
        min_volume_1h=10000,
        require_profile=False,
    )
    pair = {
        "marketCap": 90000,
        "priceChange": {"h6": 1.5, "h24": 2.0},
        "volume": {"h1": 12000},
    }
    result = evaluate_pair(pair, filters, use_fdv_proxy=True)
    assert result.passed is False


def test_missing_volume_fails():
    filters = FilterConfig(
        max_market_cap=100000,
        min_change_24h=1,
        min_change_6h=1,
        min_change_1h=1,
        min_volume_1h=10000,
        require_profile=False,
    )
    pair = {
        "marketCap": 90000,
        "priceChange": {"h1": 1.2, "h6": 1.5, "h24": 2.0},
    }
    result = evaluate_pair(pair, filters, use_fdv_proxy=True)
    assert result.passed is False


def test_negative_changes_fail():
    filters = FilterConfig(
        max_market_cap=100000,
        min_change_24h=1,
        min_change_6h=1,
        min_change_1h=1,
        min_volume_1h=10000,
        require_profile=False,
    )
    pair = {
        "marketCap": 90000,
        "priceChange": {"h1": -2.0, "h6": 1.5, "h24": 2.0},
        "volume": {"h1": 12000},
    }
    result = evaluate_pair(pair, filters, use_fdv_proxy=True)
    assert result.passed is False


def test_missing_marketcap_and_fdv_fails():
    filters = FilterConfig(
        max_market_cap=100000,
        min_change_24h=1,
        min_change_6h=1,
        min_change_1h=1,
        min_volume_1h=10000,
        require_profile=False,
    )
    pair = {
        "priceChange": {"h1": 1.2, "h6": 1.5, "h24": 2.0},
        "volume": {"h1": 12000},
    }
    result = evaluate_pair(pair, filters, use_fdv_proxy=True)
    assert result.passed is False
    assert "market cap missing" in result.reasons


def test_profile_required_fails_without_info():
    filters = FilterConfig(
        max_market_cap=100000,
        min_change_24h=1,
        min_change_6h=1,
        min_change_1h=1,
        min_volume_1h=10000,
        require_profile=True,
    )
    pair = {
        "marketCap": 90000,
        "priceChange": {"h1": 1.2, "h6": 1.5, "h24": 2.0},
        "volume": {"h1": 12000},
    }
    result = evaluate_pair(pair, filters, use_fdv_proxy=True)
    assert result.passed is False
    assert "profile missing" in result.reasons
