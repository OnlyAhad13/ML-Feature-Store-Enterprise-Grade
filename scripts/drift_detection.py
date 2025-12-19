"""
Feature Drift Detection using Kolmogorov-Smirnov Test
Compares feature distributions between offline (training) and online (inference) data.
"""

import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats


def detect_drift_ks(
    reference_data: np.ndarray,
    current_data: np.ndarray,
    feature_name: str = "feature",
    p_threshold: float = 0.05,
) -> Tuple[bool, float, float]:
    """
    Detect distribution drift using the Kolmogorov-Smirnov (KS) test.
    
    The KS test compares two samples to determine if they come from the same
    distribution. A low p-value indicates significant drift.
    
    Args:
        reference_data: Historical/training data (1D array)
        current_data: Current/inference data (1D array)
        feature_name: Name of the feature being tested
        p_threshold: P-value threshold for drift detection (default: 0.05)
    
    Returns:
        Tuple of (is_drift_detected, ks_statistic, p_value)
    """
    # Remove NaN values
    reference_clean = reference_data[~np.isnan(reference_data)]
    current_clean = current_data[~np.isnan(current_data)]
    
    if len(reference_clean) < 10 or len(current_clean) < 10:
        warnings.warn(
            f"⚠️  Insufficient data for KS test on '{feature_name}'. "
            f"Reference: {len(reference_clean)}, Current: {len(current_clean)}. "
            "Minimum 10 samples required."
        )
        return False, 0.0, 1.0
    
    # Perform two-sample KS test
    ks_statistic, p_value = stats.ks_2samp(reference_clean, current_clean)
    
    # Drift is detected if p-value is below threshold
    is_drift = p_value < p_threshold
    
    return is_drift, ks_statistic, p_value


def analyze_click_count_drift(
    yesterday_data: pd.DataFrame,
    today_data: pd.DataFrame,
    feature_column: str = "click_count_1h",
    p_threshold: float = 0.05,
) -> Dict:
    """
    Analyze drift in click_count feature between yesterday's training data
    and today's inference data.
    
    Args:
        yesterday_data: DataFrame with yesterday's data (Offline Store)
        today_data: DataFrame with today's data (Online Store samples)
        feature_column: Name of the feature column to analyze
        p_threshold: P-value threshold for drift detection
    
    Returns:
        Dictionary with drift analysis results
    """
    print("=" * 60)
    print(f"📊 Drift Detection: {feature_column}")
    print("=" * 60)
    
    # Extract feature values
    if feature_column not in yesterday_data.columns:
        raise ValueError(f"Column '{feature_column}' not found in yesterday's data")
    if feature_column not in today_data.columns:
        raise ValueError(f"Column '{feature_column}' not found in today's data")
    
    reference = yesterday_data[feature_column].values
    current = today_data[feature_column].values
    
    print(f"\n📈 Data Summary:")
    print(f"   Reference (yesterday): {len(reference):,} samples")
    print(f"   Current (today):       {len(current):,} samples")
    
    # Run KS test
    is_drift, ks_stat, p_value = detect_drift_ks(
        reference, current, feature_column, p_threshold
    )
    
    # Calculate summary statistics
    ref_stats = {
        "mean": np.nanmean(reference),
        "std": np.nanstd(reference),
        "median": np.nanmedian(reference),
        "min": np.nanmin(reference),
        "max": np.nanmax(reference),
    }
    
    curr_stats = {
        "mean": np.nanmean(current),
        "std": np.nanstd(current),
        "median": np.nanmedian(current),
        "min": np.nanmin(current),
        "max": np.nanmax(current),
    }
    
    print(f"\n📉 Distribution Statistics:")
    print(f"   {'Metric':<12} {'Reference':<15} {'Current':<15} {'Change':<10}")
    print(f"   {'-'*52}")
    
    for metric in ["mean", "std", "median", "min", "max"]:
        ref_val = ref_stats[metric]
        curr_val = curr_stats[metric]
        change = ((curr_val - ref_val) / ref_val * 100) if ref_val != 0 else 0
        print(f"   {metric:<12} {ref_val:<15.2f} {curr_val:<15.2f} {change:+.1f}%")
    
    print(f"\n🔬 Kolmogorov-Smirnov Test Results:")
    print(f"   KS Statistic: {ks_stat:.4f}")
    print(f"   P-Value:      {p_value:.6f}")
    print(f"   Threshold:    {p_threshold}")
    
    # Alert on drift
    if is_drift:
        print(f"\n🚨 DRIFT ALERT: Significant distribution drift detected!")
        print(f"   P-value ({p_value:.6f}) < threshold ({p_threshold})")
        print(f"   Action: Review feature pipeline and consider model retraining.")
        
        # Additional context
        mean_shift = abs(curr_stats["mean"] - ref_stats["mean"])
        print(f"\n   Context:")
        print(f"   - Mean shift: {mean_shift:.2f} ({(mean_shift/ref_stats['mean']*100) if ref_stats['mean'] != 0 else 0:.1f}%)")
        print(f"   - KS statistic of {ks_stat:.4f} indicates {_interpret_ks_statistic(ks_stat)}")
    else:
        print(f"\n✅ No significant drift detected (p-value: {p_value:.4f})")
    
    return {
        "feature": feature_column,
        "is_drift_detected": is_drift,
        "ks_statistic": ks_stat,
        "p_value": p_value,
        "threshold": p_threshold,
        "reference_stats": ref_stats,
        "current_stats": curr_stats,
        "reference_samples": len(reference),
        "current_samples": len(current),
    }


def _interpret_ks_statistic(ks_stat: float) -> str:
    """Interpret the KS statistic magnitude."""
    if ks_stat < 0.1:
        return "negligible difference"
    elif ks_stat < 0.2:
        return "small difference"
    elif ks_stat < 0.3:
        return "moderate difference"
    elif ks_stat < 0.5:
        return "large difference"
    else:
        return "very large difference"


def detect_multi_feature_drift(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    features: List[str],
    p_threshold: float = 0.05,
) -> Dict[str, Dict]:
    """
    Detect drift across multiple features.
    
    Args:
        reference_df: Reference DataFrame (training data)
        current_df: Current DataFrame (inference data)
        features: List of feature names to analyze
        p_threshold: P-value threshold for drift detection
    
    Returns:
        Dictionary mapping feature names to their drift results
    """
    print("=" * 70)
    print("📊 Multi-Feature Drift Detection Report")
    print("=" * 70)
    
    results = {}
    drift_count = 0
    
    for feature in features:
        if feature not in reference_df.columns or feature not in current_df.columns:
            print(f"⚠️  Skipping '{feature}' - not found in both datasets")
            continue
        
        is_drift, ks_stat, p_value = detect_drift_ks(
            reference_df[feature].values,
            current_df[feature].values,
            feature,
            p_threshold,
        )
        
        results[feature] = {
            "is_drift": is_drift,
            "ks_statistic": ks_stat,
            "p_value": p_value,
        }
        
        status = "🚨 DRIFT" if is_drift else "✅ OK"
        print(f"   {feature:<30} KS={ks_stat:.4f}  p={p_value:.4f}  {status}")
        
        if is_drift:
            drift_count += 1
    
    print(f"\n📈 Summary: {drift_count}/{len(results)} features with detected drift")
    
    if drift_count > 0:
        print("🚨 ALERT: Distribution drift detected in one or more features!")
    
    return results


# ============================================================
# Example: Load from Feast (Offline vs Online Store)
# ============================================================
def compare_feast_stores(
    feast_repo_path: str,
    feature_view: str = "user_click_features",
    feature_name: str = "click_count",
    sample_size: int = 1000,
) -> Optional[Dict]:
    """
    Compare feature distributions between Feast's Offline and Online stores.
    
    Args:
        feast_repo_path: Path to Feast repository
        feature_view: Name of the feature view
        feature_name: Name of the feature to compare
        sample_size: Number of samples to compare
    
    Returns:
        Drift analysis results or None if comparison not possible
    """
    try:
        from feast import FeatureStore
    except ImportError:
        print("❌ Feast not installed. Run: pip install feast")
        return None
    
    print(f"\n🔄 Loading data from Feast stores...")
    
    fs = FeatureStore(repo_path=feast_repo_path)
    
    # Get entity IDs (would come from your entity source)
    entity_df = pd.DataFrame({
        "user_id": [f"user_{i:06d}" for i in range(sample_size)],
    })
    
    # Fetch from offline store (historical)
    yesterday = datetime.now() - timedelta(days=1)
    offline_features = fs.get_historical_features(
        entity_df=entity_df.assign(event_timestamp=yesterday),
        features=[f"{feature_view}:{feature_name}"],
    ).to_df()
    
    # Fetch from online store (current)
    online_features = fs.get_online_features(
        entity_rows=entity_df.to_dict("records"),
        features=[f"{feature_view}:{feature_name}"],
    ).to_df()
    
    if offline_features.empty or online_features.empty:
        print("⚠️  Insufficient data for comparison")
        return None
    
    return analyze_click_count_drift(
        offline_features, online_features, feature_name
    )


# ============================================================
# Demo with synthetic data
# ============================================================
def demo_drift_detection():
    """Demonstrate drift detection with synthetic data."""
    np.random.seed(42)
    
    print("\n" + "=" * 70)
    print("🧪 DEMO: Drift Detection with Synthetic Data")
    print("=" * 70)
    
    # Scenario 1: No drift (similar distributions)
    print("\n📌 Scenario 1: No Drift")
    print("-" * 40)
    yesterday_normal = pd.DataFrame({
        "click_count_1h": np.random.poisson(lam=15, size=1000)
    })
    today_normal = pd.DataFrame({
        "click_count_1h": np.random.poisson(lam=15, size=500)
    })
    analyze_click_count_drift(yesterday_normal, today_normal)
    
    # Scenario 2: Drift detected (different distributions)
    print("\n📌 Scenario 2: Drift Detected (Mean Shift)")
    print("-" * 40)
    yesterday_baseline = pd.DataFrame({
        "click_count_1h": np.random.poisson(lam=15, size=1000)
    })
    today_shifted = pd.DataFrame({
        "click_count_1h": np.random.poisson(lam=25, size=500)  # Higher mean
    })
    analyze_click_count_drift(yesterday_baseline, today_shifted)
    
    # Scenario 3: Variance drift
    print("\n📌 Scenario 3: Drift Detected (Variance Change)")
    print("-" * 40)
    yesterday_low_var = pd.DataFrame({
        "click_count_1h": np.random.normal(loc=15, scale=2, size=1000)
    })
    today_high_var = pd.DataFrame({
        "click_count_1h": np.random.normal(loc=15, scale=8, size=500)  # Same mean, higher variance
    })
    analyze_click_count_drift(yesterday_low_var, today_high_var)


if __name__ == "__main__":
    demo_drift_detection()
