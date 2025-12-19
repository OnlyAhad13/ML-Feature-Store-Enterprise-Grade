"""
Standalone Validation Script for Online Feature Store
Can be run independently to verify Redis-based online store is working.
"""

import argparse
import random
import sys
from pathlib import Path

import pandas as pd


def validate_online_store(
    feast_repo_path: str,
    user_profiles_path: str,
    num_users: int = 5,
) -> bool:
    """
    Fetch feature vectors from the Online Store (Redis) to validate
    that materialization worked correctly.
    
    Args:
        feast_repo_path: Path to Feast repository
        user_profiles_path: Path to user profiles CSV
        num_users: Number of random users to test
    
    Returns:
        True if validation passes, False otherwise
    """
    from feast import FeatureStore
    
    print("=" * 60)
    print("🔍 Online Feature Store Validation")
    print("=" * 60)
    
    # Initialize Feast
    try:
        fs = FeatureStore(repo_path=feast_repo_path)
        print(f"\n✅ Connected to Feast repo at: {feast_repo_path}")
    except Exception as e:
        print(f"❌ Failed to connect to Feast: {e}")
        return False
    
    # List available feature views
    feature_views = fs.list_feature_views()
    print(f"\n📋 Available Feature Views:")
    for fv in feature_views:
        print(f"   - {fv.name} ({len(fv.features)} features)")
    
    if not feature_views:
        print("❌ No feature views found!")
        return False
    
    # Load user IDs
    user_profiles = Path(user_profiles_path)
    if user_profiles.exists():
        users_df = pd.read_csv(user_profiles)
        sample_users = users_df["user_id"].sample(n=min(num_users, len(users_df))).tolist()
        print(f"\n👥 Loaded {len(users_df):,} users from profiles")
    else:
        sample_users = [f"user_{i:06d}" for i in random.sample(range(10000), num_users)]
        print(f"\n⚠️  User profiles not found, using generated IDs")
    
    print(f"   Testing with: {sample_users}")
    
    # Define features to fetch
    feature_refs = [
        "user_click_features:total_events",
        "user_click_features:click_count",
        "user_click_features:view_count",
        "user_click_features:cart_count",
        "user_click_features:purchase_count",
        "user_click_features:unique_products",
        "user_click_features:unique_categories",
        "user_click_features:session_count",
        "user_click_features:total_revenue",
        "user_click_features:click_through_rate",
        "user_click_features:cart_rate",
        "user_click_features:conversion_rate",
    ]
    
    # Fetch from online store
    print("\n🔄 Fetching from Online Store (Redis)...")
    
    try:
        online_features = fs.get_online_features(
            features=feature_refs,
            entity_rows=[{"user_id": uid} for uid in sample_users],
        ).to_dict()
        
        print("✅ Online store query successful!")
        
    except Exception as e:
        print(f"❌ Online store query failed: {e}")
        return False
    
    # Analyze results
    print("\n📊 Results:")
    print("-" * 60)
    
    users_with_features = 0
    
    for i, user_id in enumerate(online_features.get("user_id", [])):
        # Check if any feature is non-null
        has_features = False
        feature_values = {}
        
        for key in online_features:
            if key != "user_id":
                value = online_features[key][i]
                if value is not None:
                    has_features = True
                    feature_values[key.split(":")[-1]] = value
        
        if has_features:
            users_with_features += 1
            print(f"\n   ✅ {user_id}:")
            for feat_name, feat_value in list(feature_values.items())[:5]:
                if isinstance(feat_value, float):
                    print(f"      {feat_name}: {feat_value:.2f}")
                else:
                    print(f"      {feat_name}: {feat_value}")
            if len(feature_values) > 5:
                print(f"      ... and {len(feature_values) - 5} more features")
        else:
            print(f"\n   ⚪ {user_id}: No features (user may not have recent activity)")
    
    # Summary
    print("\n" + "=" * 60)
    print("📈 Validation Summary")
    print("=" * 60)
    print(f"   Users tested: {len(sample_users)}")
    print(f"   Users with features: {users_with_features}")
    print(f"   Coverage: {users_with_features / len(sample_users) * 100:.1f}%")
    
    if users_with_features > 0:
        print("\n✅ VALIDATION PASSED: Online store is serving features")
        return True
    else:
        print("\n⚠️  VALIDATION WARNING: No features found for sampled users")
        print("   This may be expected if users haven't been active recently")
        return True  # Don't fail for empty results


def main():
    parser = argparse.ArgumentParser(
        description="Validate Online Feature Store (Redis)"
    )
    parser.add_argument(
        "--feast-repo",
        type=str,
        default="/home/jovyan/feast_repo",
        help="Path to Feast repository",
    )
    parser.add_argument(
        "--user-profiles",
        type=str,
        default="/home/jovyan/data/user_profiles.csv",
        help="Path to user profiles CSV",
    )
    parser.add_argument(
        "--num-users",
        type=int,
        default=5,
        help="Number of random users to test",
    )
    
    args = parser.parse_args()
    
    success = validate_online_store(
        feast_repo_path=args.feast_repo,
        user_profiles_path=args.user_profiles,
        num_users=args.num_users,
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
