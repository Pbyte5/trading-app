"""Step 2 Verification Test: Vectorization of quant_engine and Mathematical Parity."""

import time
import math
import numpy as np
from src.quant_engine.microstructure import (
    compute_vwap_and_bands,
    compute_volume_profile,
    find_liquidity_pools
)
from src.quant_engine.regime_detector import compute_atr


def _reference_compute_atr(candles, period=14):
    """Old sequential loop implementation for reference comparison."""
    if len(candles) < 2:
        return 0.0
    true_ranges = []
    for i in range(1, len(candles)):
        h = candles[i]["HIGH"]
        l = candles[i]["LOW"]
        pc = candles[i - 1]["CLOSE"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        true_ranges.append(tr)
    if not true_ranges:
        return 0.0
    return float(np.mean(true_ranges[-period:]))


def _reference_find_liquidity_pools(candles, atr):
    """Old O(N^2) loop implementation for reference comparison."""
    if len(candles) < 5 or atr <= 0:
        return {"equal_highs": [], "equal_lows": []}
    tolerance = 0.10 * atr
    highs = [float(c["HIGH"]) for c in candles]
    lows = [float(c["LOW"]) for c in candles]
    equal_highs = []
    equal_lows = []
    for i in range(len(candles) - 1):
        for j in range(i + 2, len(candles)):
            if abs(highs[i] - highs[j]) <= tolerance:
                max_inter = max(highs[i:j])
                if max_inter <= max(highs[i], highs[j]) + tolerance:
                    equal_highs.append(round((highs[i] + highs[j]) / 2.0, 4))
            if abs(lows[i] - lows[j]) <= tolerance:
                min_inter = min(lows[i:j])
                if min_inter >= min(lows[i], lows[j]) - tolerance:
                    equal_lows.append(round((lows[i] + lows[j]) / 2.0, 4))
    return {
        "equal_highs": list(set(equal_highs))[-3:],
        "equal_lows": list(set(equal_lows))[-3:]
    }


def generate_synthetic_candles(n=300, seed=42):
    np.random.seed(seed)
    candles = []
    price = 60000.0
    for _ in range(n):
        delta = np.random.normal(0, 50)
        price += delta
        h = price + abs(np.random.normal(10, 15))
        l = price - abs(np.random.normal(10, 15))
        v = abs(np.random.normal(25, 10)) + 0.1
        candles.append({"OPEN": price, "HIGH": h, "LOW": l, "CLOSE": price, "VOLUME": v})
    return candles


def test_atr_mathematical_parity():
    candles = generate_synthetic_candles(100)
    expected = _reference_compute_atr(candles, period=14)
    actual = compute_atr(candles, period=14)
    assert np.isclose(expected, actual, atol=1e-6), (
        f"ATR mismatch: expected {expected}, got {actual}"
    )


def test_liquidity_pools_mathematical_parity():
    candles = generate_synthetic_candles(150)
    atr = compute_atr(candles)
    expected = _reference_find_liquidity_pools(candles, atr)
    actual = find_liquidity_pools(candles, atr)
    assert set(expected["equal_highs"]) == set(actual["equal_highs"]), (
        f"Equal highs mismatch: {expected['equal_highs']} vs {actual['equal_highs']}"
    )
    assert set(expected["equal_lows"]) == set(actual["equal_lows"]), (
        f"Equal lows mismatch: {expected['equal_lows']} vs {actual['equal_lows']}"
    )


def test_vwap_and_volume_profile_validity():
    candles = generate_synthetic_candles(200)
    vwap_res = compute_vwap_and_bands(candles)
    assert "vwap" in vwap_res
    assert vwap_res["vwap_upper_1"] > vwap_res["vwap"]
    assert vwap_res["vwap_lower_1"] < vwap_res["vwap"]

    vp_res = compute_volume_profile(candles, num_bins=50)
    assert "poc" in vp_res
    assert vp_res["vah"] >= vp_res["poc"]
    assert vp_res["val"] <= vp_res["poc"]
    assert 0.65 <= vp_res["value_area_volume_ratio"] <= 0.85


def test_vectorization_performance():
    candles = generate_synthetic_candles(300)
    atr = compute_atr(candles)

    t0 = time.perf_counter()
    vwap_res = compute_vwap_and_bands(candles)
    vp_res = compute_volume_profile(candles)
    pools = find_liquidity_pools(candles, atr)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    print(f"Elapsed time for 300 candles (VWAP + VP + Pools): {elapsed_ms:.2f} ms")
    assert elapsed_ms < 50.0, f"Performance check failed: took {elapsed_ms:.2f} ms (>50ms)"


if __name__ == "__main__":
    print("Running test_atr_mathematical_parity...")
    test_atr_mathematical_parity()
    print("  [PASS]")

    print("Running test_liquidity_pools_mathematical_parity...")
    test_liquidity_pools_mathematical_parity()
    print("  [PASS]")

    print("Running test_vwap_and_volume_profile_validity...")
    test_vwap_and_volume_profile_validity()
    print("  [PASS]")

    print("Running test_vectorization_performance...")
    test_vectorization_performance()
    print("  [PASS]")

    print("\nALL STEP 2 TESTS PASSED SUCCESSFULLY!")
