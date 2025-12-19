"""
Real-Time Feature Inference Service
Fetches user features from Feast Online Store for purchase prediction.
Target latency: < 20ms for real-time inference.
"""

import argparse
import json
import time
from typing import Dict, List, Any, Optional

from feast import FeatureStore


# Configuration
FEAST_REPO_PATH = "feast_repo"
REALTIME_THRESHOLD_MS = 20.0

# Feature references (feature_view:feature_name)
FEATURES_TO_FETCH = [
    # User profile features (from user_profile_features view)
    "user_profile_features:age",
    "user_profile_features:country",
    "user_profile_features:subscription_tier",
    "user_profile_features:activity_weight",
    
    # Note: user_click_features will be available once Spark streaming populates data
    # "user_click_features:click_count",
    # "user_click_features:total_events",
    # "user_click_features:cart_count",
]


class FeatureService:
    """Real-time feature serving for ML inference."""
    
    def __init__(self, repo_path: str = FEAST_REPO_PATH):
        """
        Initialize the Feature Service.
        
        Args:
            repo_path: Path to the Feast repository
        """
        print(f"🔄 Initializing Feast FeatureStore from: {repo_path}")
        start = time.perf_counter()
        
        self.store = FeatureStore(repo_path=repo_path)
        
        init_time = (time.perf_counter() - start) * 1000
        print(f"✅ FeatureStore initialized in {init_time:.2f}ms")
        
        # List available feature views
        feature_views = self.store.list_feature_views()
        print(f"📋 Available feature views: {[fv.name for fv in feature_views]}")
    
    def get_features_for_users(
        self,
        user_ids: List[str],
        features: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Fetch features from the Online Store for given users.
        
        Args:
            user_ids: List of user IDs to fetch features for
            features: List of feature references (feature_view:feature_name)
                     If None, uses default FEATURES_TO_FETCH
        
        Returns:
            Dictionary with feature values and latency metrics
        """
        if features is None:
            features = FEATURES_TO_FETCH
        
        # Build entity rows
        entity_rows = [{"user_id": uid} for uid in user_ids]
        
        # Measure latency
        start = time.perf_counter()
        
        # Fetch from online store
        online_response = self.store.get_online_features(
            features=features,
            entity_rows=entity_rows,
        )
        
        # Calculate latency
        latency_ms = (time.perf_counter() - start) * 1000
        
        # Convert to dictionary
        feature_dict = online_response.to_dict()
        
        # Add metadata
        result = {
            "features": feature_dict,
            "metadata": {
                "num_users": len(user_ids),
                "num_features": len(features),
                "latency_ms": round(latency_ms, 3),
                "is_realtime": latency_ms < REALTIME_THRESHOLD_MS,
                "threshold_ms": REALTIME_THRESHOLD_MS,
            }
        }
        
        return result
    
    def get_features_for_prediction(
        self,
        user_id: str,
        features: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Fetch features for a single user (typical inference scenario).
        
        Args:
            user_id: User ID to fetch features for
            features: List of feature references
        
        Returns:
            Dictionary with user's features ready for model inference
        """
        result = self.get_features_for_users([user_id], features)
        
        # Flatten for single user
        feature_values = {}
        for key, values in result["features"].items():
            if key != "user_id" and values:
                feature_values[key] = values[0]
        
        return {
            "user_id": user_id,
            "features": feature_values,
            "latency_ms": result["metadata"]["latency_ms"],
            "is_realtime": result["metadata"]["is_realtime"],
        }


def run_inference_demo(
    user_ids: List[str],
    repo_path: str = FEAST_REPO_PATH,
    num_iterations: int = 5,
) -> None:
    """
    Run demo of real-time feature fetching with latency benchmarks.
    
    Args:
        user_ids: List of user IDs to test
        repo_path: Path to Feast repository
        num_iterations: Number of benchmark iterations
    """
    print("=" * 70)
    print("🚀 Real-Time Feature Inference Service")
    print("=" * 70)
    
    # Initialize service
    service = FeatureService(repo_path=repo_path)
    
    print(f"\n📊 Fetching features for users: {user_ids}")
    print("-" * 70)
    
    # Fetch features
    result = service.get_features_for_users(user_ids)
    
    # Print results
    print(f"\n📦 Feature Response (JSON):")
    print(json.dumps(result, indent=2, default=str))
    
    # Latency analysis
    latency = result["metadata"]["latency_ms"]
    is_rt = result["metadata"]["is_realtime"]
    
    print(f"\n⏱️  Latency Analysis:")
    print(f"   Measured latency: {latency:.3f}ms")
    print(f"   Threshold:        {REALTIME_THRESHOLD_MS}ms")
    
    if is_rt:
        print(f"   Status:           ✅ REAL-TIME (< {REALTIME_THRESHOLD_MS}ms)")
    else:
        print(f"   Status:           ⚠️  ABOVE THRESHOLD")
        print(f"   Consider: Redis connection pooling, feature caching")
    
    # Benchmark multiple iterations
    if num_iterations > 1:
        print(f"\n📈 Benchmark ({num_iterations} iterations):")
        latencies = []
        
        for i in range(num_iterations):
            result = service.get_features_for_users(user_ids)
            latencies.append(result["metadata"]["latency_ms"])
        
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
        
        print(f"   Average: {avg_latency:.3f}ms")
        print(f"   Min:     {min_latency:.3f}ms")
        print(f"   Max:     {max_latency:.3f}ms")
        print(f"   P95:     {p95_latency:.3f}ms")
        
        realtime_count = sum(1 for l in latencies if l < REALTIME_THRESHOLD_MS)
        print(f"   Real-time rate: {realtime_count}/{num_iterations} ({realtime_count/num_iterations*100:.1f}%)")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Real-time feature inference service"
    )
    parser.add_argument(
        "--users",
        type=str,
        nargs="+",
        default=["user_000001", "user_000002", "user_000010"],
        help="User IDs to fetch features for",
    )
    parser.add_argument(
        "--repo",
        type=str,
        default=FEAST_REPO_PATH,
        help="Path to Feast repository",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of benchmark iterations",
    )
    parser.add_argument(
        "--json-only",
        action="store_true",
        help="Output only JSON (for API integration)",
    )
    
    args = parser.parse_args()
    
    if args.json_only:
        # Minimal output for API integration
        service = FeatureService(args.repo)
        result = service.get_features_for_users(args.users)
        print(json.dumps(result, indent=2, default=str))
    else:
        run_inference_demo(
            user_ids=args.users,
            repo_path=args.repo,
            num_iterations=args.iterations,
        )


if __name__ == "__main__":
    main()
